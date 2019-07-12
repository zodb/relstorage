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
import sys
import weakref

import ZODB.interfaces

from perfmetrics import metricmethod

from ZODB import ConflictResolution

from ZODB.mvccadapter import HistoricalStorageAdapter

from ZODB.POSException import ReadConflictError

from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64
from ZODB.utils import z64
from zope import interface
from zope.interface import implementer

from relstorage._compat import clear_frames


from relstorage.blobhelper import BlobHelper
from relstorage.blobhelper import NoBlobHelper
from relstorage.cache import StorageCache

from relstorage.options import Options

from .transaction_iterator import TransactionIterator

from .copy import CopyMethods
from .history import HistoryMethodsMixin
from .legacy import LegacyMethodsMixin
from .load import BlobLoadMethodsMixin
from .oid import OIDs
from .oid import ReadOnlyOIDs
from .pack import PackMethodsMixin
from .store import BlobStoreMethodsMixin

from .tpc import ABORT_EARLY
from .tpc import NotInTransaction
from .tpc.begin import HistoryFree
from .tpc.begin import HistoryPreserving
from .tpc.restore import Restore

__all__ = [
    'RelStorage',
]

log = logging.getLogger("relstorage")

# pylint:disable=too-many-lines

@implementer(ZODB.interfaces.IStorage,
             ZODB.interfaces.IMVCCStorage,
             ZODB.interfaces.IMultiCommitStorage,
             ZODB.interfaces.IStorageRestoreable,
             ZODB.interfaces.IStorageIteration,
             # Perhaps this should be conditional on whether
             # supportsUndo actually returns True, as documented
             # in this interface.
             ZODB.interfaces.IStorageUndoable,
             # XXX: BlobStorage is conditional, should be dynamic
             ZODB.interfaces.IBlobStorage,
             ZODB.interfaces.IBlobStorageRestoreable,
             ZODB.interfaces.IMVCCAfterCompletionStorage,
             ZODB.interfaces.ReadVerifyingStorage,)
class RelStorage(LegacyMethodsMixin,
                 HistoryMethodsMixin,
                 PackMethodsMixin,
                 BlobLoadMethodsMixin,
                 BlobStoreMethodsMixin,
                 ConflictResolution.ConflictResolvingStorage):
    """Storage to a relational database, based on invalidation polling"""

    # pylint:disable=too-many-public-methods,too-many-instance-attributes,too-many-ancestors
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
    _cache = None # type: StorageCache

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

        # There are a small handful of ZODB tests that assume the
        # storage is thread safe (but they know nothing about IMVCC);
        # for those tests we use a custom wrapper storage that makes
        # a single instance appear thread safe.

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
        if self._is_read_only:
            self._oids = ReadOnlyOIDs()
        else:
            self._oids = OIDs(self._adapter.oidallocator, self.__with_store)

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
                           blobhelper=blobhelper)
        # NOTE: We're depending on the GIL (or list implementation)
        # for thread safety here.
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
                self._oids = self._oids.no_longer_stale()
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

    def __restart_store_callback(self, store_conn, store_cursor, fresh_connection):
        if fresh_connection:
            return
        self._adapter.connmanager.restart_store(store_conn, store_cursor)

    def _restart_store(self):
        """
        Restart the store connection, creating a new connection if
        needed.
        """
        self.__with_store(self.__restart_store_callback)

    def __with_store(self, f):
        """
        Call a function with the store cursor.

        :param callable f: Function to call ``f(store_conn, store_cursor, fresh_connection)``.
            The function may be called up to twice, if the *fresh_connection* is false
            on the first call and a disconnected exception is raised.
        """
        # If transaction commit is in progress, it's too late
        # to reconnect.
        can_reconnect = not self._tpc_phase
        fresh_connection = False
        if self._store_cursor is None:
            assert self._store_conn is None
            self._open_store_connection()
            fresh_connection = True
            # If we just connected no point in trying again.
            can_reconnect = False

        try:
            return f(self._store_conn, self._store_cursor, fresh_connection)
        except self._adapter.connmanager.disconnected_exceptions as e:
            if not can_reconnect:
                raise
            log.warning("Reconnecting store_conn: %s", e)
            self._drop_store_connection()
            try:
                self._open_store_connection()
            except:
                log.exception("Reconnect failed.")
                raise
            log.info("Reconnected.")
            return f(self._store_conn, self._store_cursor, True)

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
        self._drop_load_connection()
        self._drop_store_connection()
        self._cache.release()
        self._tpc_phase = None
        self._oids = None

    def close(self):
        """Close the storage and all instances."""
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
        self._oids = None

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

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self._tpc_phase.checkCurrentSerialInTransaction(oid, serial, transaction)

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
        if self._ltid == z64 and self._prev_polled_tid is None:
            # We haven't committed *or* polled for transactions,
            # so our MVCC state is "floating".
            # Read directly from the database to get the latest value,
            self._before_load() # connect if needed
            return int64_to_8bytes(self._adapter.txncontrol.get_tid(self._load_cursor))

        return max(self._ltid, int64_to_8bytes(self._prev_polled_tid or 0))

    def new_oid(self):
        return self._oids.new_oid()

    def iterator(self, start=None, stop=None):
        # XXX: This is broken for purposes of copyTransactionsFrom() because
        # it can only be iterated over once. zodbconvert works around this.
        return TransactionIterator(self._adapter, start, stop)

    def sync(self, force=True): # pylint:disable=unused-argument
        """Updates to a current view of the database.

        This is implemented by rolling back the relational database
        transaction.
        """
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
            self._oids = self._oids.stale(e)
            return (), prev

        self._stale_error = None
        self._tpc_phase = self._tpc_phase.no_longer_stale()
        self._oids = self._oids.no_longer_stale()

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


    def temporaryDirectory(self):
        """Return a directory that should be used for uncommitted blob data.

        If Blobs use this, then commits can be performed with a simple rename.
        """
        return self.blobhelper.temporaryDirectory()

    def copyTransactionsFrom(self, other):
        CopyMethods(self.blobhelper, self, self).copyTransactionsFrom(other)

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
