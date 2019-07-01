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
from ZODB.POSException import ConflictError
from ZODB.POSException import POSKeyError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import Unsupported
from ZODB.UndoLogCompatible import UndoLogCompatible
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64
from zope import interface
from zope.interface import implementer

from relstorage._compat import base64_decodebytes
from relstorage._compat import base64_encodebytes
from relstorage._compat import dumps
from relstorage._compat import loads
from relstorage._compat import OID_TID_MAP_TYPE
from relstorage._compat import OID_SET_TYPE
from relstorage._transaction_iterator import TransactionIterator
from relstorage.blobhelper import BlobHelper
from relstorage.cache import StorageCache
from relstorage.cache.interfaces import CacheConsistencyError
from relstorage.options import Options

# pylint:disable=too-many-lines

log = logging.getLogger("relstorage")

# Set the RELSTORAGE_ABORT_EARLY environment variable when debugging
# a failure revealed by the ZODB test suite.  The test suite often fails
# to call tpc_abort in the event of an error, leading to deadlocks.
# This variable causes RelStorage to abort failed transactions
# early rather than wait for an explicit abort.
abort_early = os.environ.get('RELSTORAGE_ABORT_EARLY')

z64 = b'\0' * 8

def _to_utf8(data):
    if data is None:
        return data
    if isinstance(data, bytes):
        return data
    return data.encode("utf-8")

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
             ZODB.interfaces.IMVCCAfterCompletionStorage)
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
    # Otherwise, blobhelper is None.
    blobhelper = None

    # _stale_error is None most of the time.  It's a ReadConflictError
    # when the database connection is stale (due to async replication).
    # (pylint likes to complain about raising None even if we have a 'not None'
    # check).
    #
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

        if create is None:
            create = options.create_schema
        if create:
            self._adapter.schema.prepare()

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
        elif options.blob_dir:
            self.blobhelper = BlobHelper(options=options, adapter=adapter)

        if self._options.keep_history:
            self._tpc_begin_factory = _HistoryPreservingBeginTPCState
        else:
            self._tpc_begin_factory = _HistoryFreeBeginTPCState

        if hasattr(self._adapter.packundo, 'deleteObject'):
            interface.alsoProvides(self, ZODB.interfaces.IExternalGC)

        self._tpc_phase = _NotInTPCState(self)

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
        if self.blobhelper is not None:
            blobhelper = self.blobhelper.new_instance(adapter=adapter)
        else:
            blobhelper = None
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
        self._adapter.connmanager.close(conn, cursor)
        self._load_transaction_open = ''

    def _rollback_load_connection(self):
        if self._load_conn is not None:
            try:
                self._load_conn.rollback()
            except:
                self._drop_load_connection()
                raise
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
        self._adapter.connmanager.close(conn, cursor)

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
        """
        with self._lock:
            self._drop_load_connection()
            self._drop_store_connection()
        self._cache.release()

    def close(self):
        """Close the storage and all instances."""

        with self._lock:
            if self._closed:
                return

            self._closed = True
            self._drop_load_connection()
            self._drop_store_connection()
            if self.blobhelper is not None:
                self.blobhelper.close()
            for wref in self._instances:
                instance = wref()
                if instance is not None:
                    instance.close()
            self._instances = []
            self._cache.close()

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
        if self._stale_error is not None:
            raise self._stale_error
        assert not version, "Versions aren't supported"

        # If we get here and we're read-only, our phase will report that.
        self._tpc_phase.store(oid, previous_tid, data, transaction)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # Like store(), but used for importing transactions.  See the
        # comments in FileStorage.restore().  The prev_txn optimization
        # is not used.
        # pylint:disable=unused-argument
        if self._stale_error is not None:
            raise self._stale_error
        assert not version, "Versions aren't supported"
        # If we get here and we're read-only, our phase will report that.

        self._tpc_phase.restore(oid, serial, data, prev_txn, transaction)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        if self._stale_error is not None:
            raise self._stale_error
        self._tpc_phase.checkCurrentSerialInTransaction(oid, serial, transaction)

    def deleteObject(self, oid, oldserial, transaction):
        # This method is only expected to be called from zc.zodbdgc
        # currently.
        return self._tpc_phase.deleteObject(oid, oldserial, transaction)

    @metricmethod
    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()

        if self._tpc_phase and self._tpc_phase.transaction is transaction:
            raise StorageTransactionError(
                "Duplicate tpc_begin calls for same transaction")

        self._tpc_phase = self._tpc_begin_factory(self, transaction)
        if tid is not None:
            # tid is a committed transaction we will restore.
            # The allowed actions are carefully prescribed.
            # This argument is specified by IStorageRestoreable
            self._tpc_phase = _RestoreBeginTPCState(self._tpc_phase, tid, status)

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
            if abort_early:
                self._tpc_phase = self.tpc_abort(transaction)
            raise
        else:
            self._tpc_phase = next_phase
            return self._tpc_phase.resolved_oids

    @metricmethod
    def tpc_finish(self, transaction, f=None):
        next_phase, committed_tid = self._tpc_phase.tpc_finish(transaction, f)
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
        if self._stale_error is not None:
            raise self._stale_error
        self._tpc_phase.undo(transaction_id, transaction)

    @metricmethod
    def pack(self, t, referencesf, prepack_only=False, skip_prepack=False,
             sleep=None):
        """Pack the storage. Holds the pack lock for the duration."""
        # pylint:disable=too-many-branches
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
                            blob_invalidate=getattr(self.blobhelper, 'after_pack', None),
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
                        if blob_invalidate:
                            # Clean up blob files. This currently does nothing
                            # if we're a blob cache, but it could.
                            blob_invalidate(oid_int, tid_int)
                        # If we're not keeping history, we need to remove all the cached
                        # data for a particular OID, no matter what key it was under:
                        # there was only one way to access it.
                        if not keep_history:
                            oids.add(oid_int)

                    adapter.packundo.pack(tid_int, sleep=sleep,
                                          packed_func=invalidate_cached_data)
                    self._cache.invalidate_all(oids_removed)
            finally:
                adapter.locker.release_pack_lock(lock_cursor)
        finally:
            lock_conn.rollback()
            adapter.connmanager.close(lock_conn, lock_cursor)
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
            self._stale_error = e
            return (), prev

        self._stale_error = None

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
        if self.blobhelper is None:
            raise Unsupported("No blob directory is configured.")

        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            return self.blobhelper.loadBlob(cursor, oid, serial)

    @metricmethod
    def openCommittedBlobFile(self, oid, serial, blob=None):
        """Return a file for committed data for the given object id and serial

        If a blob is provided, then a BlobFile object is returned,
        otherwise, an ordinary file is returned.  In either case, the
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
                if self.blobhelper is not None:
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


class _AbstractTPCState(object):
    """
    ABC for things a phase of TPC should be able to do.
    """

    # The transaction
    transaction = None
    prepared_txn = None

    aborted = False
    storage = None

    # - store
    # - restore/restoreBlob
    # - deleteObject
    # - undo

    # should raise ReadOnlyError if the storage is read only.

    # - tpc_vote should raise StorageTransactionError

    # Because entering tpc_begin wasn't allowed if the storage was
    # read only, this needs to happen in the "not in transaction"
    # state.

    def tpc_finish(self, transaction, f=None):
        # For the sake of some ZODB tests, we need to implement this everywhere,
        # even if it's not actually usable, and the first thing it needs to
        # do is check the transaction.
        if transaction is not self.transaction:
            raise StorageTransactionError('tpc_finish called with wrong transaction')
        raise NotImplementedError("tpc_finish not allowed in this state.")

    def tpc_abort(self, transaction):
        if transaction is not self.transaction:
            return self

        # the lock is held here
        try:
            try:
                self.storage._rollback_load_connection()
                if self.storage._store_cursor is not None:
                    self.storage._adapter.txncontrol.abort(
                        self.storage._store_conn,
                        self.storage._store_cursor,
                        self.prepared_txn)
                    self.storage._adapter.locker.release_commit_lock(self.storage._store_cursor)
                if self.storage.blobhelper is not None:
                    self.storage.blobhelper.abort()
            finally:
                self._clear_temp()
        finally:
            self.aborted = True
            self.storage._commit_lock.release()
        return _NotInTPCState(self.storage)

    def _clear_temp(self):
        # Clear all attributes used for transaction commit.
        # It is assumed that self._lock.acquire was called before this
        # method was called.
        self.storage._cache.clear_temp()
        blobhelper = self.storage.blobhelper
        if blobhelper is not None:
            blobhelper.clear_temp()

class _NotInTPCState(_AbstractTPCState):

    __slots__ = ('is_read_only',)

    def __init__(self, storage):
        self.is_read_only = storage._is_read_only

    def tpc_abort(self, *args, **kwargs): # pylint:disable=arguments-differ,unused-argument
        # Nothing to do
        return self

    def _no_transaction(self, *args, **kwargs):
        raise StorageTransactionError("No transaction in progress")

    tpc_finish = tpc_vote = _no_transaction

    def store(self, *_args, **_kwargs):
        if self.is_read_only:
            raise ReadOnlyError()
        self._no_transaction()

    restore = deleteObject = undo = restoreBlob = store

    # This object appears to be false.
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class _AbstractBeginTPCState(_AbstractTPCState):
    """
    The phase we enter after ``tpc_begin`` has been called.
    """

    # (user, description, extension) from the transaction.
    ude = None

    # max_stored_oid is the highest OID stored by the current
    # transaction
    max_stored_oid = 0

    # required_tids: {oid_int: tid_int}; confirms that certain objects
    # have not changed at commit. May be a BTree
    required_tids = ()

    def __init__(self, storage, transaction):
        self.storage = storage
        self.transaction = transaction
        storage._lock.acquire()
        try:
            storage._lock.release()
            storage._commit_lock.acquire()
            storage._lock.acquire()
            storage.transaction = transaction

            user = _to_utf8(transaction.user)
            desc = _to_utf8(transaction.description)
            ext = transaction.extension

            if ext:
                ext = dumps(ext, 1)
            else:
                ext = b""
            self.ude = user, desc, ext

            storage._restart_store()
            storage._cache.tpc_begin()
        finally:
            storage._lock.release()

    def _tpc_vote_factory(self, state):
        "Return the object with the enter() method."
        raise NotImplementedError

    def tpc_vote(self, transaction):
        with self.storage._lock:
            if transaction is not self.transaction:
                raise StorageTransactionError(
                    "tpc_vote called with wrong transaction")

            next_phase = self._tpc_vote_factory(self)
            next_phase.enter()
            return next_phase

class _RestoreBeginTPCState(object):
    # Wraps another begin state and adds the methods needed to
    # restore or commit to a particular tid.

    # batcher: An object that accumulates store operations
    # so they can be executed in batch (to minimize latency).
    batcher = None

    # _batcher_row_limit: The number of rows to queue before
    # calling the database.
    batcher_row_limit = 100

    # Methods from the underlying begin implementation we need to
    # expose.
    _COPY_ATTRS = (
        'store',
        'checkCurrentSerialInTransaction',
        'deletObject',
        'undo',
        'tpc_vote',
    )

    def __init__(self, begin_state, committing_tid, status):
        # This is an extension we use for copyTransactionsFrom;
        # it is not part of the IStorage API.
        assert committing_tid is not None
        self.wrapping = begin_state

        # hold the commit lock and add the transaction now
        storage = begin_state.storage
        adapter = storage._adapter
        cursor = storage._store_cursor
        packed = (status == 'p')
        adapter.locker.hold_commit_lock(cursor, ensure_current=True)
        tid_int = bytes8_to_int64(committing_tid)
        user, desc, ext = begin_state.ude
        try:
            adapter.txncontrol.add_transaction(
                cursor, tid_int, user, desc, ext, packed)
        except:
            storage._drop_store_connection()
            raise
        committing_tid_lock = _DatabaseLockedForTid(committing_tid, tid_int, adapter)
        # This is now only used for restore()
        self.batcher = adapter.mover.make_batcher(
            storage._store_cursor,
            self.batcher_row_limit)

        for name in self._COPY_ATTRS:
            try:
                meth = getattr(begin_state, name)
            except AttributeError:
                continue
            else:
                setattr(self, name, meth)

        orig_factory = begin_state._tpc_vote_factory
        def tpc_vote_factory(state):
            vote_state = orig_factory(state)
            vote_state.committing_tid_lock = committing_tid_lock

            orig_flush = vote_state._flush_temps_to_db
            def flush(cursor):
                orig_flush(cursor)
                self.batcher.flush()
            vote_state._flush_temps_to_db = flush
            return vote_state

        begin_state._tpc_vote_factory = tpc_vote_factory

    def restore(self, oid, this_tid, data, prev_txn, transaction):
        # Similar to store() (see comments in FileStorage.restore for
        # some differences), but used for importing transactions.
        # Note that *data* can be None.
        # The *prev_txn* "backpointer" optimization/hint is ignored.
        #
        # pylint:disable=unused-argument
        state = self.wrapping
        if transaction is not state.transaction:
            raise StorageTransactionError(self, transaction)

        adapter = state.storage._adapter
        cursor = state.storage._store_cursor
        assert cursor is not None
        oid_int = bytes8_to_int64(oid)
        tid_int = bytes8_to_int64(this_tid)

        with state.storage._lock:
            state.max_stored_oid = max(state.max_stored_oid, oid_int)
            # Save the `data`.  Note that `data` can be None.
            # Note also that this doesn't go through the cache.

            # TODO: Make it go through the cache, or at least the same
            # sort of queing thing, so that we can do a bulk COPY?
            # This complicates restoreBlob().
            adapter.mover.restore(
                cursor, self.batcher, oid_int, tid_int, data)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, txn):
        self.restore(oid, serial, data, prev_txn, txn)
        state = self.wrapping
        with state.storage._lock:
            # Restoring the entry for the blob MAY have used the batcher, and
            # we're going to want to foreign-key off of that data when
            # we add blob chunks (since we skip the temp tables).
            # Ideally, we wouldn't need to flush the batcher here
            # (we'd prefer having DEFERRABLE INITIALLY DEFERRED FK
            # constraints, but as-of 8.0 MySQL doesn't support that.)
            self.batcher.flush()
            cursor = state.storage._store_cursor
            state.storage.blobhelper.restoreBlob(cursor, oid, serial, blobfilename)


class _AbstractStoreBeginTPCState(_AbstractBeginTPCState):
    # pylint:disable=abstract-method

    def store(self, oid, previous_tid, data, transaction):
        # Called by Connection.commit(), after tpc_begin has been called.
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        #adapter = self._adapter
        cache = self.storage._cache
        cursor = self.storage._store_cursor
        assert cursor is not None
        oid_int = bytes8_to_int64(oid)
        if previous_tid:
            # previous_tid is the tid of the state that the
            # object was loaded from.

            # XXX PY3: ZODB.tests.IteratorStorage passes a str (non-bytes) value for oid
            prev_tid_int = bytes8_to_int64(
                previous_tid
                if isinstance(previous_tid, bytes)
                else previous_tid.encode('ascii')
            )
        else:
            prev_tid_int = 0

        with self.storage._lock:
            self.max_stored_oid = max(self.max_stored_oid, oid_int)
            # Save the data locally in a temporary place. Later, closer to commit time,
            # we'll send it all over at once. This lets us do things like use
            # COPY in postgres.
            cache.store_temp(oid_int, data, prev_tid_int)
            return None

    def checkCurrentSerialInTransaction(self, oid, required_tid, transaction):
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        _, committed_tid = self.storage.load(oid)
        if committed_tid != required_tid:
            raise ReadConflictError(
                oid=oid, serials=(committed_tid, required_tid))

        required_tid_int = bytes8_to_int64(required_tid)
        oid_int = bytes8_to_int64(oid)

        # If this transaction already specified a different serial for
        # this oid, the transaction conflicts with itself.
        required_tids = self.required_tids
        if not required_tids:
            required_tids = self.required_tids = OID_TID_MAP_TYPE()

        previous_serial_int = required_tids.get(oid_int, required_tid_int)
        if previous_serial_int != required_tid_int:
            raise ReadConflictError(
                oid=oid,
                serials=(int64_to_8bytes(previous_serial_int),
                         required_tid))
        required_tids[oid_int] = required_tid_int

class _HistoryFreeBeginTPCState(_AbstractStoreBeginTPCState):

    def deleteObject(self, oid, oldserial, transaction):
        # This method is only expected to be called from zc.zodbdgc
        # currently, or from ZODB/tests/IExternalGC.test

        # This is called in a phase of two-phase-commit (tpc).
        # This means we have a transaction, and that we are holding
        # the commit lock as well as the regular lock.
        # RelStorage native pack uses a separate pack lock, but
        # unfortunately there's no way to not hold the commit lock;
        # however, the transactions are very short.
        if transaction is not self.transaction: # pragma: no cover
            raise StorageTransactionError(self, transaction)

        # We don't worry about anything in self._cache because
        # by definition we are deleting objects that were
        # not reachable and so shouldn't be in the cache (or if they
        # were, we'll never ask for them anyway)

        # We delegate the actual operation to the adapter's packundo,
        # just like native pack
        cursor = self.storage._store_cursor
        assert cursor is not None
        # When this is done, we get a tpc_vote,
        # and a tpc_finish.
        # The interface doesn't specify a return value, so for testing
        # we return the count of rows deleted (should be 1 if successful)
        return self.storage._adapter.packundo.deleteObject(cursor, oid, oldserial)

class _DatabaseLockedForTid(object):

    def __init__(self, tid, tid_int, adapter):
        self.tid = tid
        self.tid_int = tid_int
        self._adapter = adapter

    def release_commit_lock(self, cursor):
        self._adapter.locker.release_commit_lock(cursor)

def _prepare_tid_and_lock_database(adapter, cursor, ude):
    """
    Returns an _DatabaseLockedForTid.
    """
    # TODO: Stop doing this here; go to row-level locking.
    adapter.locker.hold_commit_lock(cursor, ensure_current=True)
    user, desc, ext = ude

    # Choose a transaction ID.
    # Base the transaction ID on the current time,
    # but ensure that the tid of this transaction
    # is greater than any existing tid.
    # TODO: Stop allocating this here. Defer until tpc_finish.
    last_tid = adapter.txncontrol.get_tid(cursor)
    now = time.time()
    stamp = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
    stamp = stamp.laterThan(TimeStamp(int64_to_8bytes(last_tid)))
    tid = stamp.raw()

    tid_int = bytes8_to_int64(tid)
    adapter.txncontrol.add_transaction(cursor, tid_int, user, desc, ext)
    return _DatabaseLockedForTid(tid, tid_int, adapter)

class _HistoryPreservingBeginTPCState(_AbstractStoreBeginTPCState):

    # Stored in their 8 byte form
    undone_oids = ()

    # If we use undo(), we have to allocate a TID, which means
    # we have to lock the database. Not cool.
    committing_tid_lock = None

    def undo(self, transaction_id, transaction):
        # Typically if this is called, the store/restore methods will *not* be
        # called, but there's not a strict guarantee about that.
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        # Unlike most places, transaction_id is the base 64 encoding
        # of an 8 byte tid

        undo_tid = base64_decodebytes(transaction_id + b'\n') # pylint:disable=deprecated-method
        assert len(undo_tid) == 8
        undo_tid_int = bytes8_to_int64(undo_tid)

        with self.storage._lock:
            adapter = self.storage._adapter
            cursor = self.storage._store_cursor
            assert cursor is not None

            adapter.locker.hold_pack_lock(cursor)
            try:
                adapter.packundo.verify_undoable(cursor, undo_tid_int)
                if self.committing_tid_lock is None:
                    # Note that _prepare_tid acquires the commit lock.
                    # The commit lock must be acquired after the pack lock
                    # because the database adapters also acquire in that
                    # order during packing.
                    adapter = self.storage._adapter
                    cursor = self.storage._store_cursor
                    tid_lock = _prepare_tid_and_lock_database(adapter, cursor, self.ude)
                    self.committing_tid_lock = tid_lock

                self_tid_int = self.committing_tid_lock.tid_int
                copied = adapter.packundo.undo(
                    cursor, undo_tid_int, self_tid_int)
                oids = [int64_to_8bytes(oid_int) for oid_int, _ in copied]

                # Update the current object pointers immediately, so that
                # subsequent undo operations within this transaction will see
                # the new current objects.
                adapter.mover.update_current(cursor, self_tid_int)

                if self.storage.blobhelper is not None:
                    self.storage.blobhelper.copy_undone(copied,
                                                        self.committing_tid_lock.tid)

                if not self.undone_oids:
                    self.undone_oids = set()
                self.undone_oids.update(oids)
            finally:
                adapter.locker.release_pack_lock(cursor)

class _AbstractVoteTPCState(_AbstractTPCState):
    """
    The state we're in following ``tpc_vote``.

    Unlike the begin states, you *must* explicitly call :meth:`enter`
    on this object after it is constructed.

    """

    # The byte representation of the TID we've picked to commit.
    committing_tid_lock = None

    prepared_txn = None

    def __init__(self, begin_state, committing_tid_lock=None):
        # If committing_tid is passed to this method, it means
        # the database has already been locked and the TID is
        # locked in.
        self.storage = begin_state.storage
        self.transaction = begin_state.transaction
        self.required_tids = begin_state.required_tids
        self.max_stored_oid = begin_state.max_stored_oid
        self.ude = begin_state.ude
        self.committing_tid_lock = committing_tid_lock
        self.resolved_oids = set()

    def enter(self):
        resolved_in_vote = self.__vote()
        self.resolved_oids.update(resolved_in_vote)

    def _flush_temps_to_db(self, cursor):
        mover = self.storage._adapter.mover
        mover.store_temps(cursor, self.storage._cache.temp_objects)

    def __vote(self):
        """
        Prepare the transaction for final commit.

        Takes the exclusive database commit lock.
        """
        # This method initiates a two-phase commit process,
        # saving the name of the prepared transaction in self._prepared_txn.

        # It is assumed that self._lock.acquire was called before this
        # method was called.
        cursor = self.storage._store_cursor
        assert cursor is not None
        conn = self.storage._store_conn
        adapter = self.storage._adapter
        mover = adapter.mover

        # execute all remaining batch store operations
        self._flush_temps_to_db(cursor)

        # Reserve all OIDs used by this transaction
        if self.max_stored_oid > self.storage._max_new_oid:
            adapter.oidallocator.set_min_oid(
                cursor, self.max_stored_oid + 1)

        # Here's where we take the global commit lock, and
        # allocate the next available transaction id, storing it
        # into history-preserving DBs. But if someone passed us
        # one, then it must already be in the DB, and the lock must
        # already be held.
        if not self.committing_tid_lock:
            self.__prepare_tid()

        committing_tid_int = self.committing_tid_lock.tid_int

        # XXX: When we stop allocating the TID early, we need to move
        # 'move_from_temp' (hf and hp) and 'update_current' (hp only)
        # later, down to tpc_finish. With everything safely locked,
        # that shouldn't fail. But we still need to check serials and
        # resolve conflicts here. When we do that, we need to lock all
        # the rows involved for update, in order (to avoid deadlocks),
        # including the prev_tid rows (hp only) and current committed
        # rows (both) for everything that conflicts.

        # Check the things registered by Connection.readCurrent()
        if self.required_tids:
            oid_ints = self.required_tids.keys()
            current = mover.current_object_tids(cursor, oid_ints)
            for oid_int, expect_tid_int in self.required_tids.items():
                actual_tid_int = current.get(oid_int, 0)
                if actual_tid_int != expect_tid_int:
                    raise ReadConflictError(
                        oid=int64_to_8bytes(oid_int),
                        serials=(int64_to_8bytes(actual_tid_int),
                                 int64_to_8bytes(expect_tid_int)))

        resolved_serials = self.__finish_store(committing_tid_int)
        self.storage._adapter.mover.update_current(cursor, committing_tid_int)
        self.prepared_txn = self.storage._adapter.txncontrol.commit_phase1(
            conn, cursor, committing_tid_int)

        if self.storage.blobhelper is not None:
            self.storage.blobhelper.vote(self.committing_tid_lock.tid)

        # New storage protocol
        return resolved_serials

    def __prepare_tid(self):
        """
        Choose a tid for the current transaction, and exclusively lock
        the database commit lock.

        This should be done as late in the commit as possible, since
        it must hold an exclusive commit lock.
        """
        adapter = self.storage._adapter
        cursor = self.storage._store_cursor
        # TODO: Stop doing this here; go to row-level locking.
        lock = _prepare_tid_and_lock_database(adapter, cursor, self.ude)
        self.committing_tid_lock = lock
        return lock

    def __finish_store(self, committing_tid_int):
        """
        Move stored objects from the temporary table to final storage.

        Returns a sequence of OIDs that were resolved to be received
        by Connection._handle_serial().
        """
        # pylint:disable=too-many-locals
        cursor = self.storage._store_cursor
        adapter = self.storage._adapter
        cache = self.storage._cache
        tryToResolveConflict = self.storage.tryToResolveConflict

        # Detect conflicting changes.
        # Try to resolve the conflicts.
        resolved = set()  # a set of OIDs
        # In the past, we didn't load all conflicts from the DB at once,
        # just one at a time. This was because we also fetched the state data
        # from the DB, and it could be large. But now we use the state we have in
        # our local temp cache, so memory concerns are gone.
        conflicts = adapter.mover.detect_conflict(cursor)
        if conflicts:
            log.debug("Attempting to resolve %d conflicts", len(conflicts))

        for conflict in conflicts:
            oid_int, committed_tid_int, tid_this_txn_saw_int = conflict
            state_from_this_txn = cache.read_temp(oid_int)
            oid = int64_to_8bytes(oid_int)
            prev_tid = int64_to_8bytes(committed_tid_int)
            serial = int64_to_8bytes(tid_this_txn_saw_int)

            resolved_state = tryToResolveConflict(oid, prev_tid, serial, state_from_this_txn)
            if resolved_state is None:
                # unresolvable; kill the whole transaction
                raise ConflictError(
                    oid=oid, serials=(prev_tid, serial), data=state_from_this_txn)

            # resolved
            state_from_this_txn = resolved_state
            # TODO: Make this use the bulk methods so we can use COPY.
            adapter.mover.replace_temp(
                cursor, oid_int, committed_tid_int, state_from_this_txn)
            resolved.add(oid)
            cache.store_temp(oid_int, state_from_this_txn)

        # Move the new states into the permanent table
        if self.storage.blobhelper is not None:
            txn_has_blobs = self.storage.blobhelper.txn_has_blobs
        else:
            txn_has_blobs = False

        # This returns the OID ints stored, but we don't use them here
        adapter.mover.move_from_temp(cursor, committing_tid_int, txn_has_blobs)

        return resolved

    def tpc_finish(self, transaction, f=None):
        with self.storage._lock:
            if transaction is not self.transaction:
                raise StorageTransactionError(
                    "tpc_finish called with wrong transaction")
            assert self.committing_tid_lock is not None, self
            try:
                try:
                    if f is not None:
                        f(self.committing_tid_lock.tid)
                    next_phase = _tpc_finish_factory(self)
                    return next_phase, self.committing_tid_lock.tid
                finally:
                    self._clear_temp()
            finally:
                self.storage._commit_lock.release()


class _HistoryFreeVoteTPCState(_AbstractVoteTPCState):
    pass

_HistoryFreeBeginTPCState._tpc_vote_factory = _HistoryFreeVoteTPCState

class _HistoryPreservingVoteTPCState(_AbstractVoteTPCState):

    def __init__(self, begin_state):
        assert isinstance(begin_state, _HistoryPreservingBeginTPCState)
         # Using undo() requires a new TID, so if we had already begun
        # a transaction by locking the database and allocating a TID,
        # we must preserve that.
        super(_HistoryPreservingVoteTPCState, self).__init__(begin_state,
                                                             begin_state.committing_tid_lock)
        self.resolved_oids.update(begin_state.undone_oids)

_HistoryPreservingBeginTPCState._tpc_vote_factory = _HistoryPreservingVoteTPCState

def _tpc_finish_factory(vote_state):
    """
    The state we enter with tpc_finish.

    This is transient; once we successfully enter this state, we immediately return
    to the not-in-transaction state.
    """
    # It is assumed that self._lock.acquire was called before this
    # method was called.
    vote_state.storage._rollback_load_connection()
    txn = vote_state.prepared_txn
    assert txn is not None
    vote_state.storage._adapter.txncontrol.commit_phase2(
        vote_state.storage._store_conn,
        vote_state.storage._store_cursor,
        txn)
    vote_state.committing_tid_lock.release_commit_lock(vote_state.storage._store_cursor)
    vote_state.storage._cache.after_tpc_finish(vote_state.committing_tid_lock.tid)

    return _NotInTPCState(vote_state.storage)
