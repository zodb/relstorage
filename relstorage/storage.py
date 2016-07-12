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
from __future__ import absolute_import, print_function

from ZODB import ConflictResolution

from ZODB.BaseStorage import DataRecord
from ZODB.BaseStorage import TransactionRecord

from ZODB.POSException import ConflictError
from ZODB.POSException import POSKeyError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import Unsupported

from ZODB.UndoLogCompatible import UndoLogCompatible
from ZODB.blob import is_blob_record
from ZODB.interfaces import StorageStopIteration
from ZODB.utils import p64
from ZODB.utils import u64

from perfmetrics import Metric
from perfmetrics import metricmethod

from persistent.TimeStamp import TimeStamp
from relstorage.blobhelper import BlobHelper

from relstorage.cache import StorageCache
from relstorage.options import Options

from zope.interface import implementer
from zope import interface

import ZODB.interfaces
import logging
import os
import tempfile
import threading
import time
import weakref

from relstorage._compat import iterkeys, iteritems
from relstorage._compat import dumps, loads
from relstorage._compat import base64_encodebytes
from relstorage._compat import base64_decodebytes

log = logging.getLogger("relstorage")

# Set the RELSTORAGE_ABORT_EARLY environment variable when debugging
# a failure revealed by the ZODB test suite.  The test suite often fails
# to call tpc_abort in the event of an error, leading to deadlocks.
# This variable causes RelStorage to abort failed transactions
# early rather than wait for an explicit abort.
abort_early = os.environ.get('RELSTORAGE_ABORT_EARLY')

z64 = b'\0' * 8

def _to_latin1(data):
    if data is None:
        return data
    if isinstance(data, bytes):
        return data
    return data.encode("latin-1")

def _from_latin1(data):
    if isinstance(data, str) or data is None:
        return data
    return data.decode('latin-1')

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
             ZODB.interfaces.IBlobStorageRestoreable)
class RelStorage(UndoLogCompatible,
                 ConflictResolution.ConflictResolvingStorage):
    """Storage to a relational database, based on invalidation polling"""

    _transaction = None  # Transaction that is being committed
    _tstatus = ' '  # Transaction status, used for copying data
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

    # _tid is the current transaction ID being committed; generally
    # only set after tpc_vote().
    _tid = None

    # _ltid is the ID of the last transaction committed by this instance.
    _ltid = z64

    # _prepared_txn is the name of the transaction to commit in the
    # second phase.
    _prepared_txn = None

    # _closed is True after self.close() is called.  Since close()
    # can be called from another thread, access to self._closed should
    # be inside a _lock_acquire()/_lock_release() block.
    _closed = False

    # _max_stored_oid is the highest OID stored by the current
    # transaction
    _max_stored_oid = 0

    # _max_new_oid is the highest OID provided by new_oid()
    _max_new_oid = 0

    # _cache, if set, is a StorageCache object.
    _cache = None

    # _prev_polled_tid contains the tid at the previous poll
    _prev_polled_tid = None

    # If the blob directory is set, blobhelper is a BlobHelper.
    # Otherwise, blobhelper is None.
    blobhelper = None

    # _txn_check_serials: {oid, serial}; confirms that certain objects
    # have not changed at commit.
    _txn_check_serials = None

    # _batcher: An object that accumulates store operations
    # so they can be executed in batch (to minimize latency).
    _batcher = None

    # _batcher_row_limit: The number of rows to queue before
    # calling the database.
    _batcher_row_limit = 100

    # _stale_error is None most of the time.  It's a ReadConflictError
    # when the database connection is stale (due to async replication).
    _stale_error = None

    # OIDs resolved by undo()
    _resolved = ()

    # user, description, extension from transaction metadata.
    _ude = None

    def __init__(self, adapter, name=None, create=None,
                 options=None, cache=None, blobhelper=None,
                 # The top-level storage should use locks because
                 # new_oid is shared among all connections. But the new_instance
                 # objects don't need to.
                 _use_locks=True,
                 **kwoptions):
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

        if blobhelper is not None:
            self.blobhelper = blobhelper
        elif options.blob_dir:
            self.blobhelper = BlobHelper(options=options, adapter=adapter)

        if hasattr(self._adapter.packundo, 'deleteObject'):
            interface.alsoProvides(self, ZODB.interfaces.IExternalGC)
        else:
            def deleteObject(*_args):
                raise AttributeError("deleteObject")
            self.deleteObject = deleteObject

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

    try:
        from ZODB.mvccadapter import HistoricalStorageAdapter
    except ImportError:
        pass # ZODB4
    else:
        def before_instance(self, before):
            # Implement this method of MVCCAdapterInstance
            # (possibly destined for IMVCCStorage) as a small optimization
            # in ZODB5 that can eventually simplify ZODB.Connection.Connection
            # XXX: 5.0a2 doesn't forward the release method, so we leak
            # open connections.
            i = self.new_instance()
            x = self.HistoricalStorageAdapter(i, before)
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

    def _with_store(self, f, *args, **kw):
        """Call a function with the store connection and cursor."""
        if self._store_cursor is None:
            self._open_store_connection()
        try:
            return f(self._store_conn, self._store_cursor, *args, **kw)
        except self._adapter.connmanager.disconnected_exceptions as e:
            if self._transaction is not None:
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
            return f(self._store_conn, self._store_cursor, *args, **kw)

    def zap_all(self, **kwargs):
        """Clear all objects and transactions out of the database.

        Used by the test suite and the ZODBConvert script.
        """
        self._adapter.schema.zap_all(**kwargs)
        self._drop_load_connection()
        self._drop_store_connection()
        self._cache.clear()

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

    def getExtensionMethods(self):
        # this method is only here for b/w compat with ZODB 3.7.
        # It still exists in FileStorage in ZODB 4.0 "for testing a ZEO extension
        # mechanism"
        return {}

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

    @Metric(method=True, rate=0.1)
    def load(self, oid, version=''):
        if self._stale_error is not None:
            raise self._stale_error

        oid_int = u64(oid)
        cache = self._cache

        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            state, tid_int = cache.load(cursor, oid_int)

        if tid_int is not None:
            if not state:
                # This can happen if something attempts to load
                # an object whose creation has been undone.
                self._log_keyerror(oid_int, "creation has been undone")
                raise POSKeyError(oid)
            return state, p64(tid_int)
        else:
            self._log_keyerror(oid_int, "no tid found")
            raise POSKeyError(oid)

    def getTid(self, oid):
        if self._stale_error is not None:
            raise self._stale_error

        _state, serial = self.load(oid, '')
        return serial

    def loadEx(self, oid, version=''):
        # Since we don't support versions, just tack the empty version
        # string onto load's result.
        return self.load(oid, version) + ("",)

    @Metric(method=True, rate=0.1)
    def loadSerial(self, oid, serial):
        """Load a specific revision of an object"""
        oid_int = u64(oid)
        tid_int = u64(serial)

        with self._lock:
            self._before_load()
            state = self._adapter.mover.load_revision(
                self._load_cursor, oid_int, tid_int)
            if state is None and self._store_cursor is not None:
                # Allow loading data from later transactions
                # for conflict resolution.
                state = self._adapter.mover.load_revision(
                    self._store_cursor, oid_int, tid_int)

        if state is not None:
            assert isinstance(state, bytes) # XXX PY3 used to do str(state)
            if not state:
                raise POSKeyError(oid)
            return state
        else:
            raise POSKeyError(oid)

    @Metric(method=True, rate=0.1)
    def loadBefore(self, oid, tid):
        """Return the most recent revision of oid before tid committed."""
        if self._stale_error is not None:
            raise self._stale_error

        oid_int = u64(oid)

        with self._lock:
            if self._store_cursor is not None:
                # Allow loading data from later transactions
                # for conflict resolution.
                cursor = self._store_cursor
            else:
                self._before_load()
                cursor = self._load_cursor
            if not self._adapter.mover.exists(cursor, u64(oid)):
                raise POSKeyError(oid)

            state, start_tid = self._adapter.mover.load_before(
                cursor, oid_int, u64(tid))

            if start_tid is not None:
                if state is None:
                    # This can happen if something attempts to load
                    # an object whose creation has been undone, see load()
                    # This change fixes the test in TransactionalUndoStorage.checkUndoCreationBranch1
                    # self._log_keyerror doesn't work here, only in certain states.
                    raise POSKeyError(oid)
                end_int = self._adapter.mover.get_object_tid_after(
                    cursor, oid_int, start_tid)
                if end_int is not None:
                    end = p64(end_int)
                else:
                    end = None
                assert isinstance(state, bytes), type(state) # XXX Py3 port: state = str(state)
                return state, p64(start_tid), end
            else:
                return None

    @Metric(method=True, rate=0.1)
    def store(self, oid, serial, data, version, transaction):
        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if version:
            raise Unsupported("Versions aren't supported")

        # If self._prepared_txn is not None, that means something is
        # attempting to store objects after the vote phase has finished.
        # That should not happen, should it?
        assert self._prepared_txn is None

        adapter = self._adapter
        cache = self._cache
        cursor = self._store_cursor
        assert cursor is not None
        oid_int = u64(oid)
        if serial:
            # XXX PY3: ZODB.tests.IteratorStorage passes a str (non-bytes) value for oid
            prev_tid_int = u64(serial if isinstance(serial, bytes) else serial.encode('ascii'))
        else:
            prev_tid_int = 0

        with self._lock:
            self._max_stored_oid = max(self._max_stored_oid, oid_int)
            # save the data in a temporary table
            adapter.mover.store_temp(
                cursor, self._batcher, oid_int, prev_tid_int, data)
            cache.store_temp(oid_int, data)
            return None

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # Like store(), but used for importing transactions.  See the
        # comments in FileStorage.restore().  The prev_txn optimization
        # is not used.

        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if version:
            raise Unsupported("Versions aren't supported")

        assert self._tid is not None
        assert self._prepared_txn is None

        adapter = self._adapter
        cursor = self._store_cursor
        assert cursor is not None
        oid_int = u64(oid)
        tid_int = u64(serial)

        with self._lock:
            self._max_stored_oid = max(self._max_stored_oid, oid_int)
            # save the data.  Note that data can be None.
            adapter.mover.restore(
                cursor, self._batcher, oid_int, tid_int, data)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        if self._stale_error is not None:
            raise self._stale_error
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)

        _, committed_tid = self.load(oid, '')
        if committed_tid != serial:
            raise ReadConflictError(
                oid=oid, serials=(committed_tid, serial))

        if self._txn_check_serials is None:
            self._txn_check_serials = {}
        else:
            # If this transaction already specified a different serial for
            # this oid, the transaction conflicts with itself.
            previous_serial = self._txn_check_serials.get(oid, serial)
            if previous_serial != serial:
                raise ReadConflictError(
                    oid=oid, serials=(previous_serial, serial))
        self._txn_check_serials[oid] = serial

    def deleteObject(self, oid, oldserial, transaction): # pylint:disable=method-hidden
        # NOTE: packundo.deleteObject is only defined for
        # history-free schemas. For other schemas, the __init__ function
        # overrides this method.
        # This method is only expected to be called from zc.zodbdgc
        # currently.
        if self._is_read_only: # pragma: no cover
            raise ReadOnlyError()
        # This is called in a phase of two-phase-commit (tpc).
        # This means we have a transaction, and that we are holding
        # the commit lock as well as the regular lock.
        # RelStorage native pack uses a separate pack lock, but
        # unfortunately there's no way to not hold the commit lock;
        # however, the transactions are very short.
        if transaction is not self._transaction: # pragma: no cover
            raise StorageTransactionError(self, transaction)

        # We don't worry about anything in self._cache because
        # by definition we are deleting objects that were
        # not reachable and so shouldn't be in the cache (or if they
        # were, we'll never ask for them anyway)

        # We delegate the actual operation to the adapter's packundo,
        # just like native pack
        cursor = self._store_cursor
        assert cursor is not None
        # When this is done, we get a tpc_vote,
        # and a tpc_finish.
        # The interface doesn't specify a return value, so for testing
        # we return the count of rows deleted (should be 1 if successful)
        return self._adapter.packundo.deleteObject(cursor, oid, oldserial)


    @metricmethod
    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()
        self._lock.acquire()
        try:
            if self._transaction is transaction:
                raise StorageTransactionError(
                    "Duplicate tpc_begin calls for same transaction")

            self._lock.release()
            self._commit_lock.acquire()
            self._lock.acquire()
            self._clear_temp()
            self._transaction = transaction
            self._resolved = set()

            user = _to_latin1(transaction.user)
            desc = _to_latin1(transaction.description)
            ext = transaction._extension
            if ext:
                ext = dumps(ext, 1)
            else:
                ext = b""
            self._ude = user, desc, ext
            self._tstatus = status

            self._restart_store()
            adapter = self._adapter
            self._cache.tpc_begin()
            self._batcher = self._adapter.mover.make_batcher(
                self._store_cursor, self._batcher_row_limit)

            if tid is not None:
                # hold the commit lock and add the transaction now
                cursor = self._store_cursor
                packed = (status == 'p')
                adapter.locker.hold_commit_lock(cursor, ensure_current=True)
                tid_int = u64(tid)
                try:
                    adapter.txncontrol.add_transaction(
                        cursor, tid_int, user, desc, ext, packed)
                except:
                    self._drop_store_connection()
                    raise
            # else choose the tid later
            self._tid = tid

        finally:
            self._lock.release()

    def tpc_transaction(self):
        return self._transaction

    def _prepare_tid(self):
        """Choose a tid for the current transaction.

        This should be done as late in the commit as possible, since
        it must hold an exclusive commit lock.
        """
        if self._tid is not None:
            return
        if self._transaction is None:
            raise StorageError("No transaction in progress")

        adapter = self._adapter
        cursor = self._store_cursor
        adapter.locker.hold_commit_lock(cursor, ensure_current=True)
        user, desc, ext = self._ude

        # Choose a transaction ID.
        # Base the transaction ID on the current time,
        # but ensure that the tid of this transaction
        # is greater than any existing tid.
        last_tid = adapter.txncontrol.get_tid(cursor)
        now = time.time()
        stamp = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
        stamp = stamp.laterThan(TimeStamp(p64(last_tid)))
        tid = stamp.raw()

        tid_int = u64(tid)
        adapter.txncontrol.add_transaction(cursor, tid_int, user, desc, ext)
        self._tid = tid

    def _clear_temp(self):
        # Clear all attributes used for transaction commit.
        # It is assumed that self._lock.acquire was called before this
        # method was called.
        self._transaction = None
        self._ude = None
        self._tid = None
        self._prepared_txn = None
        self._max_stored_oid = 0
        self._batcher = None
        self._txn_check_serials = None
        self._cache.clear_temp()
        blobhelper = self.blobhelper
        if blobhelper is not None:
            blobhelper.clear_temp()

    def _finish_store(self):
        """Move stored objects from the temporary table to final storage.

        Returns a sequence of OIDs that were resolved to be received by
        Connection._handle_serial().
        """
        assert self._tid is not None
        cursor = self._store_cursor
        adapter = self._adapter
        cache = self._cache

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
            oid_int, prev_tid_int, serial_int = conflict
            data = self._cache.read_temp(oid_int)
            oid = p64(oid_int)
            prev_tid = p64(prev_tid_int)
            serial = p64(serial_int)

            rdata = self.tryToResolveConflict(oid, prev_tid, serial, data)
            if rdata is None:
                # unresolvable; kill the whole transaction
                raise ConflictError(
                    oid=oid, serials=(prev_tid, serial), data=data)
            else:
                # resolved
                data = rdata
                self._adapter.mover.replace_temp(
                    cursor, oid_int, prev_tid_int, data)
                resolved.add(oid)
                cache.store_temp(oid_int, data)

        # Move the new states into the permanent table
        tid_int = u64(self._tid)

        if self.blobhelper is not None:
            txn_has_blobs = self.blobhelper.txn_has_blobs
        else:
            txn_has_blobs = False
        oid_ints = adapter.mover.move_from_temp(cursor, tid_int, txn_has_blobs)


        return resolved

    @metricmethod
    def tpc_vote(self, transaction):
        with self._lock:
            if transaction is not self._transaction:
                raise StorageTransactionError(
                    "tpc_vote called with wrong transaction")

            try:
                resolved_by_vote = self._vote()
                if self._resolved:
                    # self._resolved contains OIDs from undo()
                    self._resolved.update(resolved_by_vote)
                    return self._resolved
                return resolved_by_vote
            except:
                if abort_early:
                    # abort early to avoid lockups while running the
                    # somewhat brittle ZODB test suite
                    self.tpc_abort(transaction)
                raise

    def _vote(self):
        """Prepare the transaction for final commit."""
        # This method initiates a two-phase commit process,
        # saving the name of the prepared transaction in self._prepared_txn.

        # It is assumed that self._lock.acquire was called before this
        # method was called.

        if self._prepared_txn is not None:
            # the vote phase has already completed
            return

        cursor = self._store_cursor
        assert cursor is not None
        conn = self._store_conn

        # execute all remaining batch store operations
        self._batcher.flush()

        # Reserve all OIDs used by this transaction
        if self._max_stored_oid > self._max_new_oid:
            self._adapter.oidallocator.set_min_oid(
                cursor, self._max_stored_oid + 1)

        self._prepare_tid()
        tid_int = u64(self._tid)

        if self._txn_check_serials:
            oid_ints = [u64(oid) for oid in iterkeys(self._txn_check_serials)]
            current = self._adapter.mover.current_object_tids(cursor, oid_ints)
            for oid, expect in iteritems(self._txn_check_serials):
                oid_int = u64(oid)
                actual = p64(current.get(oid_int, 0))
                if actual != expect:
                    raise ReadConflictError(
                        oid=oid, serials=(actual, expect))

        resolved_serials = self._finish_store()
        self._adapter.mover.update_current(cursor, tid_int)
        self._prepared_txn = self._adapter.txncontrol.commit_phase1(
            conn, cursor, tid_int)

        if self.blobhelper is not None:
            self.blobhelper.vote(self._tid)

        # New storage protocol
        return resolved_serials

    @metricmethod
    def tpc_finish(self, transaction, f=None):
        with self._lock:
            if transaction is not self._transaction:
                raise StorageTransactionError(
                    "tpc_finish called with wrong transaction")
            try:
                try:
                    if f is not None:
                        f(self._tid)
                    u, d, e = self._ude
                    self._finish(self._tid, u, d, e)
                    return self._tid
                finally:
                    self._clear_temp()
            finally:
                self._commit_lock.release()

    def _finish(self, tid, user, desc, ext): # pylint:disable=unused-argument
        """Commit the transaction."""
        # We take the same params as BaseStorage._finish, even though
        # we don't inherit from it or use them...in case people might
        # be calling us directly?

        # It is assumed that self._lock.acquire was called before this
        # method was called.
        assert self._tid is not None
        self._rollback_load_connection()
        txn = self._prepared_txn
        assert txn is not None
        self._adapter.txncontrol.commit_phase2(
            self._store_conn, self._store_cursor, txn)
        self._adapter.locker.release_commit_lock(self._store_cursor)
        self._cache.after_tpc_finish(self._tid)

        # N.B. only set _ltid after the commit succeeds,
        # including cache updates.
        self._ltid = self._tid

    @metricmethod
    def tpc_abort(self, transaction):
        with self._lock:
            if transaction is not self._transaction:
                return
            try:
                try:
                    self._abort()
                finally:
                    self._clear_temp()
            finally:
                self._commit_lock.release()

    def _abort(self):
        # the lock is held here
        self._rollback_load_connection()
        if self._store_cursor is not None:
            self._adapter.txncontrol.abort(
                self._store_conn, self._store_cursor, self._prepared_txn)
            self._adapter.locker.release_commit_lock(self._store_cursor)
        if self.blobhelper is not None:
            self.blobhelper.abort()

    def lastTransaction(self):
        with self._lock:
            if self._ltid == z64 and self._prev_polled_tid is None:
                # We haven't committed *or* polled for transactions,
                # so our MVCC state is "floating".
                # Read directly from the database to get the latest value,
                self._before_load() # connect if needed
                return p64(self._adapter.txncontrol.get_tid(self._load_cursor))

            return max(self._ltid, p64(self._prev_polled_tid or 0))

    def new_oid(self):
        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()
        with self._lock:
            # This method is actually called on the storage object of
            # the DB, not the storage object of a connection for some
            # reason. This means this method is shared among all
            # connections using a database.
            if self._preallocated_oids:
                oid_int = self._preallocated_oids.pop()
            else:
                def f(conn, cursor):
                    return list(self._adapter.oidallocator.new_oids(cursor))
                preallocated = self._with_store(f)
                preallocated.sort(reverse=True)
                oid_int = preallocated.pop()
                self._preallocated_oids = preallocated
            self._max_new_oid = max(self._max_new_oid, oid_int)
            return p64(oid_int)

    def cleanup(self):
        pass

    def supportsVersions(self):
        return False

    def modifiedInVersion(self, oid):
        return ''

    def supportsUndo(self):
        return self._adapter.keep_history

    def supportsTransactionalUndo(self):
        return self._adapter.keep_history

    @metricmethod
    def undoLog(self, first=0, last=-20, filter=None):
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
                tid = p64(tid_int)
                # Note that user and desc are schizophrenic. The transaction
                # interface specifies that they are a Python str, *probably*
                # meaning bytes. But code in the wild and the ZODB test suite
                # sets them as native strings, meaning unicode on Py3. OTOH, the
                # test suite checks that this method *returns* them as bytes!
                d = {'id': base64_encodebytes(tid)[:-1],
                     'time': TimeStamp(tid).timeTime(),
                     'user_name':  user or b'',
                     'description': desc or b''}
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
        if self._stale_error is not None:
            raise self._stale_error
        with self._lock:
            self._before_load()
            cursor = self._load_cursor
            oid_int = u64(oid)
            try:
                rows = self._adapter.dbiter.iter_object_history(
                    cursor, oid_int)
            except KeyError:
                raise POSKeyError(oid)

            res = []
            for tid_int, username, description, extension, length in rows:
                tid = p64(tid_int)
                if extension:
                    d = loads(extension)
                else:
                    d = {}
                d.update({"time": TimeStamp(tid).timeTime(),
                          "user_name": username or '',
                          "description": description or '',
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
        """Undo a transaction identified by transaction_id.

        transaction_id is the base 64 encoding of an 8 byte tid.
        Undo by writing new data that reverses the action taken by
        the transaction.
        """

        if self._stale_error is not None:
            raise self._stale_error
        if self._is_read_only:
            raise ReadOnlyError()
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)

        undo_tid = base64_decodebytes(transaction_id + b'\n')
        assert len(undo_tid) == 8
        undo_tid_int = u64(undo_tid)

        with self._lock:
            adapter = self._adapter
            cursor = self._store_cursor
            assert cursor is not None

            adapter.locker.hold_pack_lock(cursor)
            try:
                # Note that _prepare_tid acquires the commit lock.
                # The commit lock must be acquired after the pack lock
                # because the database adapters also acquire in that
                # order during packing.
                self._prepare_tid()
                adapter.packundo.verify_undoable(cursor, undo_tid_int)

                self_tid_int = u64(self._tid)
                copied = adapter.packundo.undo(
                    cursor, undo_tid_int, self_tid_int)
                oids = [p64(oid_int) for oid_int, _ in copied]

                # Update the current object pointers immediately, so that
                # subsequent undo operations within this transaction will see
                # the new current objects.
                adapter.mover.update_current(cursor, self_tid_int)

                if self.blobhelper is not None:
                    self.blobhelper.copy_undone(copied, self._tid)

                #return self._tid, oids
                # new storage protocol
                self._resolved.update(oids)
                return None
            finally:
                adapter.locker.release_pack_lock(cursor)

    @metricmethod
    def pack(self, t, referencesf, prepack_only=False, skip_prepack=False,
             sleep=None):
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

            assert isinstance(state, bytes), type(state) # XXX PY3: str(state)
            return {u64(oid) for oid in referencesf(state)}

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
                        u64(pack_point))
                    if tid_int is None:
                        log.debug("all transactions before %s have already "
                                  "been packed", time.ctime(t))
                        return

                    if prepack_only:
                        log.info("pack: beginning pre-pack")

                    s = time.ctime(TimeStamp(p64(tid_int)).timeTime())
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
                    # Now pack.
                    if self.blobhelper is not None:
                        packed_func = self.blobhelper.after_pack
                    else:
                        packed_func = None
                    adapter.packundo.pack(tid_int, sleep=sleep,
                                          packed_func=packed_func)
            finally:
                adapter.locker.release_pack_lock(lock_cursor)
        finally:
            lock_conn.rollback()
            adapter.connmanager.close(lock_conn, lock_cursor)
        self.sync()

        self._pack_finished()

    def _pack_finished(self):
        if self.blobhelper is None or self._adapter.keep_history:
            return

        # TODO: Remove all old revisions of blobs in history-free mode.

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
                if self._transaction:
                    raise

    def _restart_load_and_poll(self):
        """Call _restart_load, poll for changes, and update self._cache.
        """
        # Ignore changes made by the last transaction committed
        # by this connection.
        if self._ltid is not None:
            ignore_tid = u64(self._ltid)
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
            self._stale_error = e
            return (), prev

        self._stale_error = None

        # Inform the cache of the changes.
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
                oids = {p64(oid_int): 1 for oid_int, _tid_int in changes}
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
            self._batcher.flush()
            cursor = self._store_cursor
            self.blobhelper.storeBlob(cursor, self.store,
                                      oid, serial, data, blobfilename, version, txn)
        return None

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, txn):
        """Write blob data already committed in a separate database

        See the restore and storeBlob methods.
        """
        self.restore(oid, serial, data, '', prev_txn, txn)
        with self._lock:
            self._batcher.flush()
            cursor = self._store_cursor
            self.blobhelper.restoreBlob(cursor, oid, serial, blobfilename)

    def copyTransactionsFrom(self, other):
        # adapted from ZODB.blob.BlobStorageMixin
        begin_time = time.time()
        txnum = 0
        total_size = 0
        log.info("Counting the transactions to copy.")
        num_txns = 0
        for _ in other.iterator():
            num_txns += 1
        log.info("Copying the transactions.")
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
                     u64(trans.tid), num_txn_records, rate_str,
                     txnum, num_txns, pct_complete)

        elapsed = time.time() - begin_time
        log.info(
            "All %d transactions copied successfully in %4.1f minutes.",
            txnum, elapsed / 60.0)


class TransactionIterator(object):
    """Iterate over the transactions in a RelStorage instance."""

    def __init__(self, adapter, start, stop):
        self._adapter = adapter
        self._conn, self._cursor = self._adapter.connmanager.open_for_load()
        self._closed = False

        if start is not None:
            start_int = u64(start)
        else:
            start_int = 1
        if stop is not None:
            stop_int = u64(stop)
        else:
            stop_int = None

        # _transactions: [(tid, username, description, extension, packed)]
        self._transactions = list(adapter.dbiter.iter_transactions_range(
            self._cursor, start_int, stop_int))
        self._index = 0

    def close(self):
        if self._closed:
            return
        self._adapter.connmanager.close(self._conn, self._cursor)
        self._closed = True
        self._conn = None
        self._cursor = None

    def __del__(self):
        # belt-and-suspenders, effective on CPython
        self.close()

    def iterator(self):
        return self

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._transactions)

    def __getitem__(self, n):
        self._index = n
        return next(self)

    def next(self):
        if self._index >= len(self._transactions):
            self.close() # Don't leak our connection
            raise StorageStopIteration()
        if self._closed:
            raise IOError("TransactionIterator already closed")
        params = self._transactions[self._index]
        res = RelStorageTransactionRecord(self, *params)
        self._index += 1
        return res

    __next__ = next

class RelStorageTransactionRecord(TransactionRecord):

    def __init__(self, trans_iter, tid_int, user, desc, ext, packed):
        self._trans_iter = trans_iter
        self._tid_int = tid_int
        tid = p64(tid_int)
        status = packed and 'p' or ' '
        user = user or b''
        description = desc or b''
        if ext:
            extension = loads(ext)
        else:
            extension = {}
        TransactionRecord.__init__(self, tid, status, user, description, extension)

    def __iter__(self):
        return RecordIterator(self)


class RecordIterator(object):
    """Iterate over the objects in a transaction."""
    def __init__(self, record):
        # record is a RelStorageTransactionRecord.
        cursor = record._trans_iter._cursor
        adapter = record._trans_iter._adapter
        tid_int = record._tid_int
        self.tid = record.tid
        self._records = list(adapter.dbiter.iter_objects(cursor, tid_int))
        self._index = 0

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._records)

    def __getitem__(self, n):
        self._index = n
        return next(self)

    def next(self):
        if self._index >= len(self._records):
            raise StorageStopIteration()
        params = self._records[self._index]
        res = Record(self.tid, *params)
        self._index += 1
        return res

    __next__ = next


class Record(DataRecord):
    """An object state in a transaction"""

    def __init__(self, tid, oid_int, data):
        # XXX PY3: Used to to str(data) on Py2
        if data is not None:
            assert isinstance(data, bytes)
        DataRecord.__init__(self, p64(oid_int), tid, data, None)

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
