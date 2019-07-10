##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""The core of RelStorage, a ZODB storage for relational databases.

Stores pickles in the database.
"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import sys
import tempfile
import threading
import time
import weakref

import ZODB.interfaces
from perfmetrics import Metric
from perfmetrics import metricmethod
from persistent.timestamp import TimeStamp
from ZODB import ConflictResolution
from ZODB.blob import is_blob_record
from ZODB.mvccadapter import HistoricalStorageAdapter

from ZODB.POSException import POSKeyError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import ReadOnlyError

from ZODB.UndoLogCompatible import UndoLogCompatible
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64
from zope import interface
from zope.interface import implementer


from relstorage._compat import base64_encodebytes
from relstorage._compat import loads
from relstorage._compat import OID_SET_TYPE
from relstorage._compat import clear_frames


from relstorage.blobhelper import BlobHelper
from relstorage.blobhelper import NoBlobHelper
from relstorage.cache import StorageCache
from relstorage.cache.interfaces import CacheConsistencyError
from relstorage.options import Options

from .transaction_iterator import TransactionIterator
from .tpc import ABORT_EARLY
from .tpc import NotInTransaction
from .tpc.begin import HistoryFree
from .tpc.begin import HistoryPreserving
from .tpc.restore import Restore

__all__ = [
    'RelStorage',
]

log = logging.getLogger("relstorage")


z64 = b'\0' * 8

# pylint:disable=too-many-lines

class _DummyLock(object):
    # Enough like a lock for our purposes so we can switch
    # it in and out for tests.
    def __enter__(self):
        return

    def __exit__(self, t, v, tb):
        return

    def acquire(self):
        return

    def release(self):
        return


@implementer(ZODB.interfaces.IStorage,
             ZODB.interfaces.IMVCCStorage,
             ZODB.interfaces.IMultiCommitStorage,
             ZODB.interfaces.IStorageRestoreable,
             ZODB.interfaces.IStorageIteration,
             ZODB.interfaces.IStorageUndoable,
             ZODB.interfaces.IBlobStorage,
             ZODB.interfaces.IBlobStorageRestoreable,
             ZODB.interfaces.IMVCCAfterCompletionStorage,
             ZODB.interfaces.ReadVerifyingStorage,)
class RelStorage(UndoLogCompatible,
                 ConflictResolution.ConflictResolvingStorage):
    """Storage to a relational database, based on invalidation polling"""

    # pylint:disable=too-many-public-methods,too-many-instance-attributes
    _is_read_only = False

    # load_conn and load_cursor are open most of the time.
    _load_conn = None
    _load_cursor = None

    # _load_transaction_open is:
    # - an empty string when the load transaction is not open.
    # - 'active' when the load connection has begun a read-only transaction.
    _load_transaction_open = ''

    # store_conn and store_cursor are open during commit,
    # but not necessarily open at other times.
    _store_conn = None
    _store_cursor = None

    # _ltid is the ID of the last transaction committed by this instance.
    _ltid = z64

    # _closed is True after self.close() is called.  Since close()
    # can be called from another thread, access to self._closed should
    # be inside a _lock_acquire()/_lock_release() block.
    _closed = False

    # _max_new_oid is the highest OID provided by new_oid()
    _max_new_oid = 0

    # _cache, if set, is a StorageCache object.
    _cache = None

    # _prev_polled_tid contains the tid at the previous poll
    _prev_polled_tid = None

    # If the blob directory is set, blobhelper is a BlobHelper.
    blobhelper = NoBlobHelper()

    # _stale_error is None most of the time.  It's a ReadConflictError
    # when the database connection is stale (due to async replication).
    # (pylint likes to complain about raising None even if we have a 'not None'
    # check).
    #
    # TODO: Move the loading methods to state objects too, like the TPC methods;
    # that way we can have a Stale state for them.
    # pylint:disable=raising-bad-type
    _stale_error = None

    # The state of committing that we're in. Certain operations are
    # only available in certain states; certain information is only needed
    # in certain states.
    _tpc_phase = None

    def __init__(self, adapter, name=None, create=None,
                 options=None, cache=None, blobhelper=None,
                 # The top-level storage should use locks because
                 # new_oid is (potentially) shared among all connections. But the new_instance
                 # objects don't need to.
                 _use_locks=True,
                 **kwoptions):
        # pylint:disable=too-many-branches
        self._adapter = adapter

        if options is None:
            options = Options(**kwoptions)
        elif kwoptions:
            raise TypeError("The RelStorage constructor accepts either "
                            "an options parameter or keyword arguments, not both")
        self._options = options

        if not name:
            name = options.name
            if not name:
                name = 'RelStorage: %s' % adapter
        self.__name__ = name

        self._is_read_only = options.read_only

        need_check_compat = create is None
        if create is None:
            create = options.create_schema

        if create:
            self._adapter.schema.prepare()
        elif need_check_compat:
            # At the top level, not new_instance(), and not asked to create.
            self._adapter.schema.verify()


        # A ZODB Connection is documented as not being thread-safe and
        # must be used only by a single thread at a time. In IMVCC,
        # each Connection has a unique storage object. So the storage
        # object is used only by a single thread. So there's really
        # not much point in RelStorage keeping thread locks around its
        # methods.
        # There are a small handful of ZODB tests that assume the storage is thread
        # safe (but they know nothing about IMVCC) so we allow our tests to force us to
        # use locks.
        self._lock = _DummyLock() if not _use_locks else threading.RLock()
        self._commit_lock = _DummyLock() if not _use_locks else threading.Lock()
        # XXX: We don't use these attributes but some existing wrappers
        # might rely on them? They used to be a documented part of the FileStorage
        # interface prior to ZODB5. In ZODB5, _lock and _commit_lock are documented
        # attributes. (We used to have them as __lock, etc, so risk of breakage
        # in a rename is small.)
        self._lock_acquire = self._lock.acquire
        self._lock_release = self._lock.release
        self._commit_lock_acquire = self._commit_lock.acquire
        self._commit_lock_release = self._commit_lock.release

        # _instances is a list of weak references to storage instances bound
        # to the same database.
        self._instances = []

        # _preallocated_oids contains OIDs provided by the database
        # but not yet used.
        self._preallocated_oids = []

        if cache is not None:
            self._cache = cache
        else:
            prefix = options.cache_prefix
            if not prefix:
                # Use the database name as the cache prefix.
                self._open_load_connection()
                prefix = adapter.schema.get_database_name(self._load_cursor)
                self._drop_load_connection()
                prefix = prefix.replace(' ', '_')
            self._cache = StorageCache(adapter, options, prefix)

        # Creating the storage cache may have loaded cache files, and if so,
        # we have a previous tid state.
        if self._cache.current_tid is not None:
            self._prev_polled_tid = self._cache.current_tid


        if blobhelper is not None:
            self.blobhelper = blobhelper
        else:
            self.blobhelper = BlobHelper(options=options, adapter=adapter)

        self._tpc_begin_factory = HistoryPreserving if self._options.keep_history else HistoryFree

        if hasattr(self._adapter.packundo, 'deleteObject'):
            interface.alsoProvides(self, ZODB.interfaces.IExternalGC)

        self._tpc_phase = NotInTransaction(self)

    def __repr__(self):
        return "<%s at %x keep_history=%s phase=%r cache=%r>" % (
            self.__class__.__name__,
            id(self),
            self._options.keep_history,
            self._tpc_phase,
            self._cache
        )

    def new_instance(self):
        """Creates and returns another storage instance.

        See ZODB.interfaces.IMVCCStorage.
        """
        adapter = self._adapter.new_instance()
        cache = self._cache.new_instance()
        blobhelper = self.blobhelper.new_instance(adapter=adapter)
        other = type(self)(adapter=adapter, name=self.__name__,
                           create=False, options=self._options, cache=cache,
                           _use_locks=False,
                           blobhelper=blobhelper)
        self._instances.append(weakref.ref(other, self._instances.remove))

        if '_crs_transform_record_data' in self.__dict__:
            # registerDB has been called on us but isn't called on
            # our children. Make sure any wrapper that needs to transform
            # records can do so.
            # See https://github.com/zodb/relstorage/issues/71
            other._crs_transform_record_data = self._crs_transform_record_data
            other._crs_untransform_record_data = self._crs_untransform_record_data
        return other

    def before_instance(self, before):
        # Implement this method of MVCCAdapterInstance
        # (possibly destined for IMVCCStorage) as a small optimization
        # in ZODB5 that can eventually simplify ZODB.Connection.Connection
        # XXX: 5.0a2 doesn't forward the release method, so we leak
        # open connections.
        i = self.new_instance()
        x = HistoricalStorageAdapter(i, before)
        x.release = i.release
        return x

    @property
    def fshelper(self):
        """Used in tests"""
        return self.blobhelper.fshelper

    def _open_load_connection(self):
        """Open the load connection to the database.  Return nothing."""
        conn, cursor = self._adapter.connmanager.open_for_load()
        self._drop_load_connection()
        self._load_conn, self._load_cursor = conn, cursor
        self._load_transaction_open = 'active'

    def _drop_load_connection(self):
        """Unconditionally drop the load connection"""
        conn, cursor = self._load_conn, self._load_cursor
        self._load_conn, self._load_cursor = None, None
        self._adapter.connmanager.rollback_and_close(conn, cursor)
        self._load_transaction_open = ''

    def _rollback_load_connection(self):
        if self._load_conn is not None:
            try:
                self._adapter.connmanager.rollback(self._load_conn, self._load_cursor)
            except:
                self._drop_load_connection()
                raise
            finally:
                self._tpc_phase = self._tpc_phase.no_longer_stale()
            self._load_transaction_open = ''

    def _restart_load_and_call(self, f, *args, **kw):
        """Restart the load connection and call a function.

        The first two function parameters are the load connection and cursor.
        """
        if self._load_cursor is None:
            assert self._load_conn is None
            need_restart = False
            self._open_load_connection()
        else:
            need_restart = True
        try:
            if need_restart:
                self._adapter.connmanager.restart_load(
                    self._load_conn, self._load_cursor)
                self._load_transaction_open = 'active'
            return f(self._load_conn, self._load_cursor, *args, **kw)
        except self._adapter.connmanager.disconnected_exceptions as e:
            log.warning("Reconnecting load_conn: %s", e)
            self._drop_load_connection()
            try:
                self._open_load_connection()
            except:
                log.exception("Reconnect failed.")
                raise
            log.info("Reconnected.")
            return f(self._load_conn, self._load_cursor, *args, **kw)

    def _open_store_connection(self):
        """Open the store connection to the database.  Return nothing."""
        assert self._store_conn is None
        conn, cursor = self._adapter.connmanager.open_for_store()
        self._drop_store_connection()
        self._store_conn, self._store_cursor = conn, cursor

    def _drop_store_connection(self):
        """Unconditionally drop the store connection"""
        conn, cursor = self._store_conn, self._store_cursor
        self._store_conn, self._store_cursor = None, None
        self._adapter.connmanager.rollback_and_close(conn, cursor)

    def _restart_store(self):
        """Restart the store connection, creating a new connection if needed"""
        if self._store_cursor is None:
            assert self._store_conn is None
            self._open_store_connection()
            return
        try:
            self._adapter.connmanager.restart_store(
                self._store_conn, self._store_cursor)
        except self._adapter.connmanager.disconnected_exceptions as e:
            log.warning("Reconnecting store_conn: %s", e)
            self._drop_store_connection()
            try:
                self._open_store_connection()
            except:
                log.exception("Reconnect failed.")
                raise
            else:
                log.info("Reconnected.")

    def __with_store(self, f):
        """Call a function with the store cursor."""
        if self._store_cursor is None:
            self._open_store_connection()
        try:
            return f(self._store_cursor)
        except self._adapter.connmanager.disconnected_exceptions as e:
            if self._tpc_phase:
                # If transaction commit is in progress, it's too late
                # to reconnect.
                raise
            log.warning("Reconnecting store_conn: %s", e)
            self._drop_store_connection()
            try:
                self._open_store_connection()
            except:
                log.exception("Reconnect failed.")
                raise
            log.info("Reconnected.")
            return f(self._store_cursor)

    def zap_all(self, **kwargs):
        """Clear all objects and transactions out of the database.

        Used by the test suite and the ZODBConvert script.
        """
        self._adapter.schema.zap_all(**kwargs)
        self._drop_load_connection()
        self._drop_store_connection()
        self._cache.zap_all()

    def release(self):
        """
        Release external resources used by this storage instance.

        This includes the database sessions (connections) and any memcache
        connections.

        Does *not* affect any other instances created by this instance. This object
        should still be :meth:`close` (but note that might have global affects
        on other instances of the same base object).
        """
        with self._lock:
            self._drop_load_connection()
            self._drop_store_connection()
        self._cache.release()
        self._tpc_phase = None

    def close(self):
        """Close the storage and all instances."""

        with self._lock:
            if self._closed:
                return

            self._closed = True
            self._drop_load_connection()
            self._drop_store_connection()
            self.blobhelper.close()
            for wref in self._instances:
                instance = wref()
                if instance is not None:
                    instance.close()
            self._instances = []
            self._cache.close()
            self._tpc_phase = None

    def __len__(self):
        return self._adapter.stats.get_object_count()

    def sortKey(self):
        """Return a string that can be used to sort storage instances.

        The key must uniquely identify a storage and must be the same
        across multiple instantiations of the same storage.
        """
        return self.__name__

    def getName(self):
        return self.__name__

    def getSize(self):
        """Return database size in bytes"""
        return self._adapter.stats.get_db_size()

    def registerDB(self, wrapper):
        if (ZODB.interfaces.IStorageWrapper.providedBy(wrapper)
                and not ZODB.interfaces.IDatabase.providedBy(wrapper)
                and not hasattr(type(wrapper), 'new_instance')):
            # Fixes for https://github.com/zopefoundation/zc.zlibstorage/issues/2
            # We special-case zlibstorage for speed
            if hasattr(wrapper, 'base') and hasattr(wrapper, 'copied_methods'):
                type(wrapper).new_instance = _zlibstorage_new_instance
                # NOTE that zlibstorage has a custom copyTransactionsFrom that overrides
                # our own implementation.
            else:
                wrapper.new_instance = lambda s: type(wrapper)(self.new_instance())

        # Prior to ZODB 4.3.1, ConflictResolvingStorage would raise an AttributeError
        super(RelStorage, self).registerDB(wrapper)

    def isReadOnly(self):
        return self._is_read_only

    def _log_keyerror(self, oid_int, reason):
        """Log just before raising POSKeyError in load().

        KeyErrors in load() are generally not supposed to happen,
        so this is a good place to gather information.
        """
        cursor = self._load_cursor
        adapter = self._adapter
        logfunc = log.warning
        msg = ["POSKeyError on oid %d: %s" % (oid_int, reason)]

        if adapter.keep_history:
            tid = adapter.txncontrol.get_tid(cursor)
            if not tid:
                # This happens when initializing a new database or
                # after packing, so it's not a warning.
                logfunc = log.debug
                msg.append("No previous transactions exist")
            else:
                msg.append("Current transaction is %d" % tid)

            tids = []
            try:
                rows = adapter.dbiter.iter_object_history(cursor, oid_int)
            except KeyError:
                # The object has no history, at least from the point of view
                # of the current database load connection.
                pass
            else:
                for row in rows:
                    tids.append(row[0])
                    if len(tids) >= 10:
                        break
            msg.append("Recent object tids: %s" % repr(tids))

        else:
            if oid_int == 0:
                # This happens when initializing a new database or
                # after packing, so it's usually not a warning.
                logfunc = log.debug
            msg.append("history-free adapter")

        logfunc('; '.join(msg))

    def _before_load(self):
        if not self._load_transaction_open:
            self._restart_load_and_poll()
        assert self._load_transaction_open == 'active'

    def __load_using_method(self, meth, argument):
        if self._stale_error is not None:
            raise self._stale_error

        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            try:
                return meth(cursor, argument)
            except CacheConsistencyError:
                log.exception("Cache consistency error; restarting load")
                self._drop_load_connection()
                raise

    @Metric(method=True, rate=0.1)
    def load(self, oid, version=''):
        # pylint:disable=unused-argument

        oid_int = bytes8_to_int64(oid)

        state, tid_int = self.__load_using_method(self._cache.load, oid_int)

        if tid_int is None:
            self._log_keyerror(oid_int, "no tid found")
            raise POSKeyError(oid)

        if not state:
            # This can happen if something attempts to load
            # an object whose creation has been undone.
            self._log_keyerror(oid_int, "creation has been undone")
            raise POSKeyError(oid)
        return state, int64_to_8bytes(tid_int)

    def prefetch(self, oids):
        prefetch = self._cache.prefetch
        oid_ints = [bytes8_to_int64(oid) for oid in oids]
        try:
            self.__load_using_method(prefetch, oid_ints)
        except Exception: # pylint:disable=broad-except
            # This could raise self._stale_error, or
            # CacheConsistencyError. Both of those mean that regular loads
            # may fail too, but we don't know what our transaction state is
            # at this time, so we don't want to raise it to the caller.
            log.exception("Failed to prefetch")

    def getTid(self, oid):
        _state, serial = self.load(oid)
        return serial

    def loadEx(self, oid, version=''):
        # Since we don't support versions, just tack the empty version
        # string onto load's result.
        return self.load(oid, version) + ("",)

    @Metric(method=True, rate=0.1)
    def loadSerial(self, oid, serial):
        """Load a specific revision of an object"""
        oid_int = bytes8_to_int64(oid)
        tid_int = bytes8_to_int64(serial)

        # If we've got this state cached exactly,
        # use it. No need to poll or anything like that first;
        # polling is unlikely to get us the state we want.
        # If the data happens to have been removed from the database,
        # due to a pack, this won't detect it if it was already cached
        # and the pack happened somewhere else. This method is
        # only used for conflict resolution, though, and we
        # shouldn't be able to get to that point if the root revision
        # went missing, right? Packing periodically takes the same locks we
        # want to take for committing.
        state = self._cache.loadSerial(oid_int, tid_int)
        if state:
            return state

        with self._lock:
            self._before_load()
            state = self._adapter.mover.load_revision(
                self._load_cursor, oid_int, tid_int)
            if state is None and self._store_cursor is not None:
                # Allow loading data from later transactions
                # for conflict resolution.
                state = self._adapter.mover.load_revision(
                    self._store_cursor, oid_int, tid_int)

        if state is None or not state:
            raise POSKeyError(oid)
        return state

    @Metric(method=True, rate=0.1)
    def loadBefore(self, oid, tid):
        """Return the most recent revision of oid before tid committed."""
        if self._stale_error is not None:
            raise self._stale_error

        oid_int = bytes8_to_int64(oid)

        with self._lock:
            if self._store_cursor is not None:
                # Allow loading data from later transactions
                # for conflict resolution.
                cursor = self._store_cursor
            else:
                self._before_load()
                cursor = self._load_cursor
            if not self._adapter.mover.exists(cursor, oid_int):
                raise POSKeyError(oid)

            state, start_tid = self._adapter.mover.load_before(
                cursor, oid_int, bytes8_to_int64(tid))

            if start_tid is None:
                return None

            if state is None:
                # This can happen if something attempts to load
                # an object whose creation has been undone, see load()
                # This change fixes the test in
                # TransactionalUndoStorage.checkUndoCreationBranch1
                # self._log_keyerror doesn't work here, only in certain states.
                raise POSKeyError(oid)
            end_int = self._adapter.mover.get_object_tid_after(
                cursor, oid_int, start_tid)
            if end_int is not None:
                end = int64_to_8bytes(end_int)
            else:
                end = None

            return state, int64_to_8bytes(start_tid), end

    @Metric(method=True, rate=0.1)
    def store(self, oid, previous_tid, data, version, transaction):
        # Called by Connection.commit(), after tpc_begin has been called.
        assert not version, "Versions aren't supported"

        # If we get here and we're read-only, our phase will report that.
        self._tpc_phase.store(oid, previous_tid, data, transaction)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # Like store(), but used for importing transactions.  See the
        # comments in FileStorage.restore().  The prev_txn optimization
        # is not used.
        # pylint:disable=unused-argument
        assert not version, "Versions aren't supported"
        # If we get here and we're read-only, our phase will report that.

        self._tpc_phase.restore(oid, serial, data, prev_txn, transaction)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self._tpc_phase.checkCurrentSerialInTransaction(oid, serial, transaction)

    def deleteObject(self, oid, oldserial, transaction):
        # This method is only expected to be called from zc.zodbdgc
        # currently.
        return self._tpc_phase.deleteObject(oid, oldserial, transaction)

    @metricmethod
    def tpc_begin(self, transaction, tid=None, status=' '):
        try:
            self._tpc_phase = self._tpc_phase.tpc_begin(transaction)
        except:
            # Could be a database (connection) error, could be a programming
            # bug. Either way, we're fine to roll everything back and hope
            # for the best on a retry.
            self._drop_load_connection()
            self._drop_store_connection()
            raise

        if tid is not None:
            # tid is a committed transaction we will restore.
            # The allowed actions are carefully prescribed.
            # This argument is specified by IStorageRestoreable
            self._tpc_phase = Restore(self._tpc_phase, tid, status)

    def tpc_transaction(self):
        return self._tpc_phase.transaction

    # For tests
    _transaction = property(tpc_transaction)

    @metricmethod
    def tpc_vote(self, transaction):
        # Returns an iterable of OIDs; the storage will ghost all
        # cached objects in that list. This is invalidation because
        # the object has changed during the commit process, due to
        # conflict resolution or undo.
        try:
            next_phase = self._tpc_phase.tpc_vote(transaction)
        except:
            if ABORT_EARLY:
                self._tpc_phase = self.tpc_abort(transaction)
            raise
        else:
            self._tpc_phase = next_phase
            return self._tpc_phase.invalidated_oids

    @metricmethod
    def tpc_finish(self, transaction, f=None):
        try:
            next_phase, committed_tid = self._tpc_phase.tpc_finish(transaction, f)
        except:
            # OH NO! This isn't supposed to happen!
            # It's unlikely tpc_abort will get called...
            self.tpc_abort(transaction)
            raise
        self._tpc_phase = next_phase
        self._ltid = committed_tid
        return committed_tid

    @metricmethod
    def tpc_abort(self, transaction):
        with self._lock:
            self._tpc_phase = self._tpc_phase.tpc_abort(transaction)

    def afterCompletion(self):
        # Note that this method exists mainly to deal with read-only
        # transactions that don't go through 2-phase commit (although
        # it's called for all transactions). For this reason, we only
        # have to roll back the load connection. The store connection
        # is completed during normal write-transaction commit or
        # abort.
        self._rollback_load_connection()

    def lastTransaction(self):
        with self._lock:
            if self._ltid == z64 and self._prev_polled_tid is None:
                # We haven't committed *or* polled for transactions,
                # so our MVCC state is "floating".
                # Read directly from the database to get the latest value,
                self._before_load() # connect if needed
                return int64_to_8bytes(self._adapter.txncontrol.get_tid(self._load_cursor))

            return max(self._ltid, int64_to_8bytes(self._prev_polled_tid or 0))

    def new_oid(self):
        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()
        with self._lock:
            # See comments in __init__.py. Prior to that patch and
            # ZODB 5.1.2, this method was actually called on the
            # storage object of the DB, not the storage object of a
            # connection for some reason. This meant that this method
            # (and the oid cache) was shared among all connections
            # using a database and was called outside of a transaction
            # (starting its own long-running transaction). The
            # DB.new_oid() method still exists, so we still need to
            # support that usage, hence `with_store`.
            #
            # Also, there are tests (ZODB.tests.MTStorage) that directly call
            # this *outside of any transaction*. That doesn't make any sense, but
            # that's what they do.
            if not self._preallocated_oids:
                preallocated = self.__with_store(self._adapter.oidallocator.new_oids)
                self._preallocated_oids = preallocated

            oid_int = self._preallocated_oids.pop()
            self._max_new_oid = max(self._max_new_oid, oid_int)
            return int64_to_8bytes(oid_int)

    def cleanup(self):
        pass

    def supportsVersions(self):
        return False

    def modifiedInVersion(self, oid):
        # pylint:disable=unused-argument
        return ''

    def supportsUndo(self):
        return self._adapter.keep_history

    def supportsTransactionalUndo(self):
        return self._adapter.keep_history

    @metricmethod
    def undoLog(self, first=0, last=-20, filter=None):
        # pylint:disable=too-many-locals
        if self._stale_error is not None:
            raise self._stale_error
        if last < 0:
            last = first - last

        # use a private connection to ensure the most current results
        adapter = self._adapter
        conn, cursor = adapter.connmanager.open()
        try:
            rows = adapter.dbiter.iter_transactions(cursor)
            i = 0
            res = []
            for tid_int, user, desc, ext in rows:
                tid = int64_to_8bytes(tid_int)
                # Note that user and desc are schizophrenic. The transaction
                # interface specifies that they are a Python str, *probably*
                # meaning bytes. But code in the wild and the ZODB test suite
                # sets them as native strings, meaning unicode on Py3. OTOH, the
                # test suite checks that this method *returns* them as bytes!
                # This is largely cleaned up with transaction 2.0/ZODB 5, where the storage
                # interface is defined in terms of bytes only.
                d = {
                    'id': base64_encodebytes(tid)[:-1],  # pylint:disable=deprecated-method
                    'time': TimeStamp(tid).timeTime(),
                    'user_name':  user or b'',
                    'description': desc or b'',
                }
                if ext:
                    d.update(loads(ext))

                if filter is None or filter(d):
                    if i >= first:
                        res.append(d)
                    i += 1
                    if i >= last:
                        break
            return res

        finally:
            adapter.connmanager.close(conn, cursor)

    @metricmethod
    def history(self, oid, version=None, size=1, filter=None):
        # pylint:disable=unused-argument,too-many-locals
        if self._stale_error is not None:
            raise self._stale_error
        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            oid_int = bytes8_to_int64(oid)
            try:
                rows = self._adapter.dbiter.iter_object_history(
                    cursor, oid_int)
            except KeyError:
                raise POSKeyError(oid)

            res = []
            for tid_int, username, description, extension, length in rows:
                tid = int64_to_8bytes(tid_int)
                if extension:
                    d = loads(extension)
                else:
                    d = {}
                d.update({
                    "time": TimeStamp(tid).timeTime(),
                    "user_name": username or b'',
                    "description": description or b'',
                    "tid": tid,
                    "version": '',
                    "size": length,
                })
                if filter is None or filter(d):
                    res.append(d)
                    if size is not None and len(res) >= size:
                        break
            return res

    @metricmethod
    def undo(self, transaction_id, transaction):
        """
        Undo a transaction identified by transaction_id.

        transaction_id is the base 64 encoding of an 8 byte tid. Undo
        by writing new data that reverses the action taken by the
        transaction.
        """
        # This is called directly from code in DB.py on a new instance
        # (created either by new_instance() or a special
        # undo_instance()). That new instance is never asked to load
        # anything, or poll invalidations, so our storage cache is ineffective
        # (unless we had loaded persistent state files)
        #
        # TODO: Implement 'undo_instance' to make this clear.
        #
        # A regular Connection going through two-phase commit will
        # call tpc_begin(), do a bunch of store() from its commit(),
        # then tpc_vote(), tpc_finish().
        #
        # During undo, we get a tpc_begin(), then a bunch of undo() from
        # ZODB.DB.TransactionalUndo.commit(), then tpc_vote() and tpc_finish().
        self._tpc_phase.undo(transaction_id, transaction)

    @metricmethod
    def pack(self, t, referencesf, prepack_only=False, skip_prepack=False,
             sleep=None):
        """Pack the storage. Holds the pack lock for the duration."""
        # pylint:disable=too-many-branches,unused-argument
        # 'sleep' is a legacy argument, no longer used.
        if self._is_read_only:
            raise ReadOnlyError()

        prepack_only = prepack_only or self._options.pack_prepack_only
        skip_prepack = skip_prepack or self._options.pack_skip_prepack

        if prepack_only and skip_prepack:
            raise ValueError('Pick either prepack_only or skip_prepack.')

        def get_references(state):
            """Return an iterable of the set of OIDs the given state refers to."""
            if not state:
                return ()

            return {bytes8_to_int64(oid) for oid in referencesf(state)}

        # Use a private connection (lock_conn and lock_cursor) to
        # hold the pack lock.  Have the adapter open temporary
        # connections to do the actual work, allowing the adapter
        # to use special transaction modes for packing.
        adapter = self._adapter
        lock_conn, lock_cursor = adapter.connmanager.open()
        try:
            adapter.locker.hold_pack_lock(lock_cursor)
            try:
                if not skip_prepack:
                    # Find the latest commit before or at the pack time.
                    pack_point = TimeStamp(*time.gmtime(t)[:5] + (t % 60,)).raw()
                    tid_int = adapter.packundo.choose_pack_transaction(
                        bytes8_to_int64(pack_point))
                    if tid_int is None:
                        log.debug("all transactions before %s have already "
                                  "been packed", time.ctime(t))
                        return

                    if prepack_only:
                        log.info("pack: beginning pre-pack")

                    s = time.ctime(TimeStamp(int64_to_8bytes(tid_int)).timeTime())
                    log.info("pack: analyzing transactions committed "
                             "%s or before", s)

                    # In pre_pack, the adapter fills tables with
                    # information about what to pack.  The adapter
                    # must not actually pack anything yet.
                    adapter.packundo.pre_pack(tid_int, get_references)
                else:
                    # Need to determine the tid_int from the pack_object table
                    tid_int = adapter.packundo._find_pack_tid()

                if prepack_only:
                    log.info("pack: pre-pack complete")
                else:
                    # Now pack. We'll get a callback for every oid/tid removed,
                    # and we'll use that to keep caches consistent.
                    # In the common case of using zodbpack, this will rewrite the
                    # persistent cache on the machine running zodbpack.
                    oids_removed = OID_SET_TYPE()
                    def invalidate_cached_data(
                            oid_int, tid_int,
                            cache=self._cache,
                            blob_invalidate=self.blobhelper.after_pack,
                            keep_history=self._options.keep_history,
                            oids=oids_removed
                    ):
                        # pylint:disable=dangerous-default-value
                        # Flush the data from the local/global cache. It's quite likely that
                        # a fair amount if it is now useless. We almost certainly want to
                        # establish new checkpoints. The alternative is to clear out
                        # data that we know we removed in the pack; we *do* keep track
                        # of that, it's what's passed to the `packed_func`, so we could probably
                        # piggyback on that in some fashion.
                        #
                        # Having consistent cache data became especially important
                        # when loadSerial() began using the cache: Since that function
                        # is allowed to return non-transactional data, it wouldn't be
                        # great for it to return data that is no longer in the DB,
                        # only the cache. I *think* this is only an issue in the tests,
                        # that use the storage in a non-conventional way after packing,
                        # by directly verifying that loadSerial() doesn't return data,
                        # when in a real use we could only get there through detecting a conflict
                        # in the database at commit time, with locks involved.
                        cache.invalidate(oid_int, tid_int)
                        # Clean up blob files. This currently does nothing
                        # if we're a blob cache, but it could.
                        blob_invalidate(oid_int, tid_int)
                        # If we're not keeping history, we need to remove all the cached
                        # data for a particular OID, no matter what key it was under:
                        # there was only one way to access it.
                        if not keep_history:
                            oids.add(oid_int)

                    adapter.packundo.pack(tid_int,
                                          packed_func=invalidate_cached_data)
                    self._cache.invalidate_all(oids_removed)
            finally:
                adapter.locker.release_pack_lock(lock_cursor)
        finally:
            adapter.connmanager.rollback_and_close(lock_conn, lock_cursor)
        self.sync()

        self._pack_finished()

    def _pack_finished(self):
        "Hook for testing."

    def iterator(self, start=None, stop=None):
        # XXX: This is broken for purposes of copyTransactionsFrom() because
        # it can only be iterated over once. zodbconvert works around this.
        return TransactionIterator(self._adapter, start, stop)

    def sync(self, force=True): # pylint:disable=unused-argument
        """Updates to a current view of the database.

        This is implemented by rolling back the relational database
        transaction.
        """
        with self._lock:
            if not self._load_transaction_open:
                return

            try:
                self._rollback_load_connection()
            except self._adapter.connmanager.disconnected_exceptions:
                # disconnected. Well, the rollback happens automatically in that case.
                self._drop_load_connection()
                if self._tpc_phase:
                    raise

    def _restart_load_and_poll(self):
        """Call _restart_load, poll for changes, and update self._cache.
        """
        # Ignore changes made by the last transaction committed
        # by this connection.
        if self._ltid is not None:
            ignore_tid = bytes8_to_int64(self._ltid)
        else:
            ignore_tid = None
        prev = self._prev_polled_tid

        # get a list of changed OIDs and the most recent tid
        try:
            changes, new_polled_tid = self._restart_load_and_call(
                self._adapter.poller.poll_invalidations, prev, ignore_tid)
        except ReadConflictError as e:
            # The database connection is stale, but postpone this
            # error until the application tries to read or write something.
            # XXX: We probably need to drop our pickle cache? At least the local
            # delta_after* maps/current_tid/checkpoints?
            log.error("ReadConflictError from polling invalidations; %s", e)
            # Allow GC to do its thing with the locals
            clear_frames(sys.exc_info()[2])
            self._stale_error = e
            self._tpc_phase = self._tpc_phase.stale(e)
            return (), prev

        self._stale_error = None
        self._tpc_phase = self._tpc_phase.no_longer_stale()

        # Inform the cache of the changes.
        # It's not good if this raises a CacheConsistencyError, that should
        # be a fresh connection. It's not clear that we can take any steps to
        # recover that the cache hasn't already taken.
        self._cache.after_poll(
            self._load_cursor, prev, new_polled_tid, changes)

        return changes, new_polled_tid

    def poll_invalidations(self):
        """Look for OIDs of objects that changed since _prev_polled_tid.

        Returns {oid: 1}, or None if all objects need to be invalidated
        because prev_polled_tid is not in the database (presumably it
        has been packed).
        """
        with self._lock:
            if self._closed:
                return {}

            changes, new_polled_tid = self._restart_load_and_poll()

            self._prev_polled_tid = new_polled_tid

            if changes is None:
                oids = None
            else:
                # The value is ignored, only key matters
                oids = {int64_to_8bytes(oid_int): 1 for oid_int, _tid_int in changes}
            return oids

    @metricmethod
    def loadBlob(self, oid, serial):
        """Return the filename of the Blob data for this OID and serial.

        Returns a filename.

        Raises POSKeyError if the blobfile cannot be found.
        """
        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            return self.blobhelper.loadBlob(cursor, oid, serial)

    @metricmethod
    def openCommittedBlobFile(self, oid, serial, blob=None):
        """
        Return a file for committed data for the given object id and serial

        If a blob is provided, then a BlobFile object is returned,
        otherwise, an ordinary file is returned. In either case, the
        file is opened for binary reading.

        This method is used to allow storages that cache blob data to
        make sure that data are available at least long enough for the
        file to be opened.
        """
        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            return self.blobhelper.openCommittedBlobFile(
                cursor, oid, serial, blob=blob)

    def temporaryDirectory(self):
        """Return a directory that should be used for uncommitted blob data.

        If Blobs use this, then commits can be performed with a simple rename.
        """
        return self.blobhelper.temporaryDirectory()

    @metricmethod
    def storeBlob(self, oid, serial, data, blobfilename, version, txn):
        """Stores data that has a BLOB attached.

        The blobfilename argument names a file containing blob data.
        The storage will take ownership of the file and will rename it
        (or copy and remove it) immediately, or at transaction-commit
        time.  The file must not be open.

        Returns nothing.
        """
        assert not version
        with self._lock:
            # We used to flush the batcher here, for some reason.
            cursor = self._store_cursor
            self.blobhelper.storeBlob(cursor, self.store,
                                      oid, serial, data, blobfilename, version, txn)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, txn):
        """
        Write blob data already committed in a separate database

        See the restore and storeBlob methods.
        """
        self._tpc_phase.restoreBlob(oid, serial, data, blobfilename, prev_txn, txn)

    def copyTransactionsFrom(self, other):
        # pylint:disable=too-many-locals
        # adapted from ZODB.blob.BlobStorageMixin
        begin_time = time.time()
        txnum = 0
        total_size = 0
        log.info("Counting the transactions to copy.")
        num_txns = 0
        for _ in other.iterator():
            num_txns += 1
        log.info("Copying %d transactions", num_txns)

        for trans in other.iterator():
            txnum += 1
            num_txn_records = 0

            self.tpc_begin(trans, trans.tid, trans.status)
            for record in trans:
                blobfile = None
                if is_blob_record(record.data):
                    try:
                        blobfile = other.openCommittedBlobFile(
                            record.oid, record.tid)
                    except POSKeyError:
                        pass
                if blobfile is not None:
                    fd, name = tempfile.mkstemp(
                        suffix='.tmp',
                        dir=self.blobhelper.temporaryDirectory())
                    os.close(fd)
                    with open(name, 'wb') as target:
                        ZODB.utils.cp(blobfile, target)
                    blobfile.close()
                    self.restoreBlob(record.oid, record.tid, record.data,
                                     name, record.data_txn, trans)
                else:
                    self.restore(record.oid, record.tid, record.data,
                                 '', record.data_txn, trans)
                num_txn_records += 1
                if record.data:
                    total_size += len(record.data)
            self.tpc_vote(trans)
            self.tpc_finish(trans)

            pct_complete = '%1.2f%%' % (txnum * 100.0 / num_txns)
            elapsed = time.time() - begin_time
            if elapsed:
                rate = total_size / 1e6 / elapsed
            else:
                rate = 0.0
            rate_str = '%1.3f' % rate
            log.info("Copied tid %d,%5d records | %6s MB/s (%6d/%6d,%7s)",
                     bytes8_to_int64(trans.tid), num_txn_records, rate_str,
                     txnum, num_txns, pct_complete)

        elapsed = time.time() - begin_time
        log.info(
            "All %d transactions copied successfully in %4.1f minutes.",
            txnum, elapsed / 60.0)

def _zlibstorage_new_instance(self):
    new_self = type(self).__new__(type(self))
    # Preserve _transform, etc
    new_self.__dict__ = self.__dict__.copy()
    new_self.base = self.base.new_instance()
    # Because these are bound methods, we must re-copy
    # them or ivars might be wrong, like _transaction
    for name in self.copied_methods:
        v = getattr(new_self.base, name, None)
        if v is not None:
            setattr(new_self, name, v)
    return new_self
