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

import base64
import cPickle
import logging
import md5
import os
import time
import weakref
from ZODB.utils import p64, u64
from ZODB.BaseStorage import BaseStorage, TransactionRecord, DataRecord
from ZODB import ConflictResolution, POSException
from persistent.TimeStamp import TimeStamp

try:
    from ZODB.interfaces import StorageStopIteration
except ImportError:
    class StorageStopIteration(IndexError, StopIteration):
        """A combination of StopIteration and IndexError to provide a
        backwards-compatible exception.
        """


log = logging.getLogger("relstorage")

# Set the RELSTORAGE_ABORT_EARLY environment variable when debugging
# a failure revealed by the ZODB test suite.  The test suite often fails
# to call tpc_abort in the event of an error, leading to deadlocks.
# This variable causes RelStorage to abort failed transactions
# early rather than wait for an explicit abort.
abort_early = os.environ.get('RELSTORAGE_ABORT_EARLY')


class RelStorage(BaseStorage,
                ConflictResolution.ConflictResolvingStorage):
    """Storage to a relational database, based on invalidation polling"""

    def __init__(self, adapter, name=None, create=True,
            read_only=False, options=None):
        if name is None:
            name = 'RelStorage on %s' % adapter.__class__.__name__

        self._adapter = adapter
        self._name = name
        self._is_read_only = read_only
        if options is None:
            options = Options()
        self._options = options
        self._cache_client = None

        if create:
            self._adapter.prepare_schema()

        # load_conn and load_cursor are open most of the time.
        self._load_conn = None
        self._load_cursor = None
        self._load_transaction_open = False
        self._open_load_connection()
        # store_conn and store_cursor are open during commit,
        # but not necessarily open at other times.
        self._store_conn = None
        self._store_cursor = None

        BaseStorage.__init__(self, name)

        self._tid = None
        self._ltid = None

        # _prepared_txn is the name of the transaction to commit in the
        # second phase.
        self._prepared_txn = None

        # _instances is a list of weak references to storage instances bound
        # to the same database.
        self._instances = []

        # _closed is True after self.close() is called.  Since close()
        # can be called from another thread, access to self._closed should
        # be inside a _lock_acquire()/_lock_release() block.
        self._closed = False

        # _max_stored_oid is the highest OID stored by the current
        # transaction
        self._max_stored_oid = 0

        # _max_new_oid is the highest OID provided by new_oid()
        self._max_new_oid = 0

        # set _cache_client
        if options.cache_servers:
            module_name = options.cache_module_name
            module = __import__(module_name, {}, {}, ['Client'])
            servers = options.cache_servers
            if isinstance(servers, basestring):
                servers = servers.split()
            self._cache_client = module.Client(servers)
        else:
            self._cache_client = None


    def _open_load_connection(self):
        """Open the load connection to the database.  Return nothing."""
        conn, cursor = self._adapter.open_for_load()
        self._drop_load_connection()
        self._load_conn, self._load_cursor = conn, cursor
        self._load_transaction_open = True

    def _drop_load_connection(self):
        """Unconditionally drop the load connection"""
        conn, cursor = self._load_conn, self._load_cursor
        self._load_conn, self._load_cursor = None, None
        self._adapter.close(conn, cursor)
        self._load_transaction_open = False

    def _rollback_load_connection(self):
        if self._load_conn is not None:
            try:
                self._load_conn.rollback()
            except:
                self._drop_load_connection()
                raise
            self._load_transaction_open = False

    def _restart_load(self):
        """Restart the load connection, creating a new connection if needed"""
        if self._load_cursor is None:
            self._open_load_connection()
        else:
            try:
                self._adapter.restart_load(self._load_cursor)
            except POSException.StorageError, e:
                log.warning("Reconnecting load_conn: %s", e)
                self._drop_load_connection()
                try:
                    self._open_load_connection()
                except:
                    log.exception("Reconnect failed.")
                    raise
                else:
                    log.info("Reconnected.")
            self._load_transaction_open = True


    def _open_store_connection(self):
        """Open the store connection to the database.  Return nothing."""
        conn, cursor = self._adapter.open_for_store()
        self._drop_store_connection()
        self._store_conn, self._store_cursor = conn, cursor

    def _drop_store_connection(self):
        """Unconditionally drop the store connection"""
        conn, cursor = self._store_conn, self._store_cursor
        self._store_conn, self._store_cursor = None, None
        self._adapter.close(conn, cursor)

    def _restart_store(self):
        """Restart the store connection, creating a new connection if needed"""
        if self._store_cursor is None:
            self._open_store_connection()
        else:
            try:
                self._adapter.restart_store(self._store_cursor)
            except POSException.StorageError, e:
                log.warning("Reconnecting store_conn: %s", e)
                self._drop_store_connection()
                try:
                    self._open_store_connection()
                except:
                    log.exception("Reconnect failed.")
                    raise
                else:
                    log.info("Reconnected.")


    def zap_all(self):
        """Clear all objects and transactions out of the database.

        Used by the test suite and migration scripts.
        """
        self._adapter.zap_all()
        self._rollback_load_connection()
        cache = self._cache_client
        if cache is not None:
            cache.flush_all()

    def close(self):
        """Close the connections to the database."""
        self._lock_acquire()
        try:
            self._closed = True
            self._drop_load_connection()
            self._drop_store_connection()
            for wref in self._instances:
                instance = wref()
                if instance is not None:
                    instance.close()
        finally:
            self._lock_release()

    def bind_connection(self, zodb_conn):
        """Get a connection-bound storage instance.

        Connections have their own storage instances so that
        the database can provide the MVCC semantics rather than ZODB.
        """
        res = BoundRelStorage(self, zodb_conn)
        self._instances.append(weakref.ref(res))
        return res

    def connection_closing(self):
        """Release resources."""
        # Note that this is overridden in BoundRelStorage.
        self._rollback_load_connection()

    def __len__(self):
        return self._adapter.get_object_count()

    def getSize(self):
        """Return database size in bytes"""
        return self._adapter.get_db_size()

    def _get_oid_cache_key(self, oid_int):
        """Return the cache key for finding the current tid.

        This is overridden by BoundRelStorage.  This version always returns
        None because a non-bound storage does not have a prev_polled_tid,
        which is required for cache invalidation.
        """
        return None

    def _log_keyerror(self, oid_int, reason):
        """Log just before raising KeyError in load().

        KeyErrors in load() are generally not supposed to happen,
        so this is a good place to gather information.
        """
        cursor = self._load_cursor
        adapter = self._adapter
        msg = ["Storage KeyError on oid %d: %s" % (oid_int, reason)]
        rows = adapter.iter_transactions(cursor)
        row = None
        for row in rows:
            # just get the first row
            break
        if not row:
            msg.append("No transactions exist")
        else:
            msg.append("Current transaction is %d" % row[0])

        tids = []
        try:
            rows = adapter.iter_object_history(cursor, oid_int)
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
        log.warning('; '.join(msg))

    def load(self, oid, version):
        oid_int = u64(oid)
        cache = self._cache_client

        self._lock_acquire()
        try:
            if not self._load_transaction_open:
                self._restart_load()
            cursor = self._load_cursor
            if cache is None:
                state, tid_int = self._adapter.load_current(cursor, oid_int)
            else:
                # get tid_int from the cache or the database
                cachekey = self._get_oid_cache_key(oid_int)
                if cachekey:
                    tid_int = cache.get(cachekey)
                if not cachekey or not tid_int:
                    tid_int = self._adapter.get_current_tid(
                        cursor, oid_int)
                    if cachekey and tid_int is not None:
                        cache.set(cachekey, tid_int)
                if tid_int is None:
                    self._log_keyerror(oid_int, "no tid found(1)")
                    raise KeyError(oid)

                # get state from the cache or the database
                cachekey = 'state:%d:%d' % (oid_int, tid_int)
                state = cache.get(cachekey)
                if not state:
                    state = self._adapter.load_revision(
                        cursor, oid_int, tid_int)
                    if state:
                        state = str(state)
                        cache.set(cachekey, state)
        finally:
            self._lock_release()

        if tid_int is not None:
            if state:
                state = str(state)
            if not state:
                # This can happen if something attempts to load
                # an object whose creation has been undone.
                self._log_keyerror(oid_int, "creation has been undone")
                raise KeyError(oid)
            return state, p64(tid_int)
        else:
            self._log_keyerror(oid_int, "no tid found(2)")
            raise KeyError(oid)

    def loadEx(self, oid, version):
        # Since we don't support versions, just tack the empty version
        # string onto load's result.
        return self.load(oid, version) + ("",)

    def loadSerial(self, oid, serial):
        """Load a specific revision of an object"""
        oid_int = u64(oid)
        tid_int = u64(serial)
        cache = self._cache_client
        if cache is not None:
            cachekey = 'state:%d:%d' % (oid_int, tid_int)
            state = cache.get(cachekey)
            if state:
                return state

        self._lock_acquire()
        try:
            if self._store_cursor is not None:
                # Allow loading data from later transactions
                # for conflict resolution.
                cursor = self._store_cursor
            else:
                if not self._load_transaction_open:
                    self._restart_load()
                cursor = self._load_cursor
            state = self._adapter.load_revision(cursor, oid_int, tid_int)
        finally:
            self._lock_release()

        if state is not None:
            state = str(state)
            if not state:
                raise POSKeyError(oid)
            if cache is not None:
                cache.set(cachekey, state)
            return state
        else:
            raise KeyError(oid)

    def loadBefore(self, oid, tid):
        """Return the most recent revision of oid before tid committed."""
        oid_int = u64(oid)

        self._lock_acquire()
        try:
            if self._store_cursor is not None:
                # Allow loading data from later transactions
                # for conflict resolution.
                cursor = self._store_cursor
            else:
                if not self._load_transaction_open:
                    self._restart_load()
                cursor = self._load_cursor
            if not self._adapter.exists(cursor, u64(oid)):
                raise KeyError(oid)

            state, start_tid = self._adapter.load_before(
                cursor, oid_int, u64(tid))
            if start_tid is not None:
                end_int = self._adapter.get_object_tid_after(
                    cursor, oid_int, start_tid)
                if end_int is not None:
                    end = p64(end_int)
                else:
                    end = None
                if state is not None:
                    state = str(state)
                return state, p64(start_tid), end
            else:
                return None
        finally:
            self._lock_release()


    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            raise POSException.Unsupported("Versions aren't supported")

        # If self._prepared_txn is not None, that means something is
        # attempting to store objects after the vote phase has finished.
        # That should not happen, should it?
        assert self._prepared_txn is None
        md5sum = md5.new(data).hexdigest()

        adapter = self._adapter
        cursor = self._store_cursor
        assert cursor is not None
        oid_int = u64(oid)
        if serial:
            prev_tid_int = u64(serial)
        else:
            prev_tid_int = 0

        self._lock_acquire()
        try:
            self._max_stored_oid = max(self._max_stored_oid, oid_int)
            # save the data in a temporary table
            adapter.store_temp(cursor, oid_int, prev_tid_int, md5sum, data)
            return None
        finally:
            self._lock_release()


    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # Like store(), but used for importing transactions.  See the
        # comments in FileStorage.restore().  The prev_txn optimization
        # is not used.
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            raise POSException.Unsupported("Versions aren't supported")

        assert self._tid is not None
        assert self._prepared_txn is None
        if data is not None:
            md5sum = md5.new(data).hexdigest()
        else:
            # George Bailey object
            md5sum = None

        adapter = self._adapter
        cursor = self._store_cursor
        assert cursor is not None
        oid_int = u64(oid)
        tid_int = u64(serial)

        self._lock_acquire()
        try:
            self._max_stored_oid = max(self._max_stored_oid, oid_int)
            # save the data.  Note that md5sum and data can be None.
            adapter.restore(cursor, oid_int, tid_int, md5sum, data)
        finally:
            self._lock_release()


    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._lock_acquire()
        try:
            if self._transaction is transaction:
                return
            self._lock_release()
            self._commit_lock_acquire()
            self._lock_acquire()
            self._transaction = transaction
            self._clear_temp()

            user = str(transaction.user)
            desc = str(transaction.description)
            ext = transaction._extension
            if ext:
                ext = cPickle.dumps(ext, 1)
            else:
                ext = ""
            self._ude = user, desc, ext
            self._tstatus = status

            adapter = self._adapter
            self._restart_store()

            if tid is not None:
                # get the commit lock and add the transaction now
                cursor = self._store_cursor
                packed = (status == 'p')
                adapter.start_commit(cursor)
                tid_int = u64(tid)
                try:
                    adapter.add_transaction(
                        cursor, tid_int, user, desc, ext, packed)
                except:
                    self._drop_store_connection()
                    raise
            # else choose the tid later
            self._tid = tid

        finally:
            self._lock_release()

    def _prepare_tid(self):
        """Choose a tid for the current transaction.

        This should be done as late in the commit as possible, since
        it must hold an exclusive commit lock.
        """
        if self._tid is not None:
            return
        if self._transaction is None:
            raise POSException.StorageError("No transaction in progress")

        adapter = self._adapter
        cursor = self._store_cursor
        adapter.start_commit(cursor)
        user, desc, ext = self._ude

        # Choose a transaction ID.
        # Base the transaction ID on the database time,
        # while ensuring that the tid of this transaction
        # is greater than any existing tid.
        last_tid, now = adapter.get_tid_and_time(cursor)
        stamp = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
        stamp = stamp.laterThan(TimeStamp(p64(last_tid)))
        tid = repr(stamp)

        tid_int = u64(tid)
        adapter.add_transaction(cursor, tid_int, user, desc, ext)
        self._tid = tid


    def _clear_temp(self):
        # It is assumed that self._lock_acquire was called before this
        # method was called.
        self._prepared_txn = None
        self._max_stored_oid = 0


    def _finish_store(self):
        """Move stored objects from the temporary table to final storage.

        Returns a list of (oid, tid) to be received by
        Connection._handle_serial().
        """
        assert self._tid is not None
        cursor = self._store_cursor
        adapter = self._adapter

        # Detect conflicting changes.
        # Try to resolve the conflicts.
        resolved = set()  # a set of OIDs
        while True:
            conflict = adapter.detect_conflict(cursor)
            if conflict is None:
                break

            oid_int, prev_tid_int, serial_int, data = conflict
            oid = p64(oid_int)
            prev_tid = p64(prev_tid_int)
            serial = p64(serial_int)

            rdata = self.tryToResolveConflict(oid, prev_tid, serial, data)
            if rdata is None:
                # unresolvable; kill the whole transaction
                raise POSException.ConflictError(
                    oid=oid, serials=(prev_tid, serial), data=data)
            else:
                # resolved
                data = rdata
                md5sum = md5.new(data).hexdigest()
                self._adapter.replace_temp(
                    cursor, oid_int, prev_tid_int, md5sum, data)
                resolved.add(oid)

        # Move the new states into the permanent table
        tid_int = u64(self._tid)
        serials = []
        oid_ints = adapter.move_from_temp(cursor, tid_int)
        for oid_int in oid_ints:
            oid = p64(oid_int)
            if oid in resolved:
                serial = ConflictResolution.ResolvedSerial
            else:
                serial = self._tid
            serials.append((oid, serial))

        return serials


    def _vote(self):
        """Prepare the transaction for final commit."""
        # This method initiates a two-phase commit process,
        # saving the name of the prepared transaction in self._prepared_txn.

        # It is assumed that self._lock_acquire was called before this
        # method was called.

        if self._prepared_txn is not None:
            # the vote phase has already completed
            return

        cursor = self._store_cursor
        assert cursor is not None

        if self._max_stored_oid > self._max_new_oid:
            self._adapter.set_min_oid(cursor, self._max_stored_oid + 1)

        self._prepare_tid()
        tid_int = u64(self._tid)

        serials = self._finish_store()
        self._adapter.update_current(cursor, tid_int)
        self._prepared_txn = self._adapter.commit_phase1(cursor, tid_int)

        return serials


    def tpc_vote(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
            try:
                return self._vote()
            except:
                if abort_early:
                    # abort early to avoid lockups while running the
                    # somewhat brittle ZODB test suite
                    self.tpc_abort(transaction)
                raise
        finally:
            self._lock_release()


    def _finish(self, tid, user, desc, ext):
        """Commit the transaction."""
        # It is assumed that self._lock_acquire was called before this
        # method was called.
        assert self._tid is not None
        self._rollback_load_connection()
        txn = self._prepared_txn
        assert txn is not None
        self._adapter.commit_phase2(self._store_cursor, txn)
        self._prepared_txn = None
        self._ltid = self._tid
        self._tid = None

    def _abort(self):
        # the lock is held here
        self._rollback_load_connection()
        if self._store_cursor is not None:
            self._adapter.abort(self._store_cursor, self._prepared_txn)
        self._prepared_txn = None
        self._tid = None

    def lastTransaction(self):
        return self._ltid

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._lock_acquire()
        try:
            cursor = self._load_cursor
            if cursor is None:
                self._open_load_connection()
                cursor = self._load_cursor
            oid_int = self._adapter.new_oid(cursor)
            self._max_new_oid = max(self._max_new_oid, oid_int)
            return p64(oid_int)
        finally:
            self._lock_release()

    def cleanup(self):
        pass

    def supportsVersions(self):
        return False

    def modifiedInVersion(self, oid):
        return ''

    def supportsUndo(self):
        return True

    def supportsTransactionalUndo(self):
        return True

    def undoLog(self, first=0, last=-20, filter=None):
        if last < 0:
            last = first - last

        # use a private connection to ensure the most current results
        adapter = self._adapter
        conn, cursor = adapter.open()
        try:
            rows = adapter.iter_transactions(cursor)
            i = 0
            res = []
            for tid_int, user, desc, ext in rows:
                tid = p64(tid_int)
                d = {'id': base64.encodestring(tid)[:-1],
                     'time': TimeStamp(tid).timeTime(),
                     'user_name': user or '',
                     'description': desc or ''}
                if ext:
                    d.update(cPickle.loads(ext))
                if filter is None or filter(d):
                    if i >= first:
                        res.append(d)
                    i += 1
                    if i >= last:
                        break
            return res

        finally:
            adapter.close(conn, cursor)

    def history(self, oid, version=None, size=1, filter=None):
        self._lock_acquire()
        try:
            cursor = self._load_cursor
            oid_int = u64(oid)
            try:
                rows = self._adapter.iter_object_history(cursor, oid_int)
            except KeyError:
                raise KeyError(oid)

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
        finally:
            self._lock_release()


    def undo(self, transaction_id, transaction):
        """Undo a transaction identified by transaction_id.

        transaction_id is the base 64 encoding of an 8 byte tid.
        Undo by writing new data that reverses the action taken by
        the transaction.
        """

        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        undo_tid = base64.decodestring(transaction_id + '\n')
        assert len(undo_tid) == 8
        undo_tid_int = u64(undo_tid)

        self._lock_acquire()
        try:
            adapter = self._adapter
            cursor = self._store_cursor
            assert cursor is not None

            adapter.hold_pack_lock(cursor)
            try:
                # Note that _prepare_tid acquires the commit lock.
                # The commit lock must be acquired after the pack lock
                # because the database adapters also acquire in that
                # order during packing.
                self._prepare_tid()
                adapter.verify_undoable(cursor, undo_tid_int)

                self_tid_int = u64(self._tid)
                oid_ints = adapter.undo(cursor, undo_tid_int, self_tid_int)
                oids = [p64(oid_int) for oid_int in oid_ints]

                # Update the current object pointers immediately, so that
                # subsequent undo operations within this transaction will see
                # the new current objects.
                adapter.update_current(cursor, self_tid_int)

                return self._tid, oids
            finally:
                adapter.release_pack_lock(cursor)
        finally:
            self._lock_release()


    def pack(self, t, referencesf):
        if self._is_read_only:
            raise POSException.ReadOnlyError()

        pack_point = repr(TimeStamp(*time.gmtime(t)[:5]+(t%60,)))
        pack_point_int = u64(pack_point)

        def get_references(state):
            """Return the set of OIDs the given state refers to."""
            refs = set()
            if state:
                for oid in referencesf(str(state)):
                    refs.add(u64(oid))
            return refs

        # Use a private connection (lock_conn and lock_cursor) to
        # hold the pack lock.  Have the adapter open temporary
        # connections to do the actual work, allowing the adapter
        # to use special transaction modes for packing.
        adapter = self._adapter
        lock_conn, lock_cursor = adapter.open()
        try:
            adapter.hold_pack_lock(lock_cursor)
            try:
                # Find the latest commit before or at the pack time.
                tid_int = adapter.choose_pack_transaction(pack_point_int)
                if tid_int is None:
                    # Nothing needs to be packed.
                    return

                s = time.ctime(TimeStamp(p64(tid_int)).timeTime())
                log.info("packing transactions committed %s or before", s)

                # In pre_pack, the adapter fills tables with
                # information about what to pack.  The adapter
                # should not actually pack anything yet.
                adapter.pre_pack(tid_int, get_references, self._options)

                # Now pack.
                adapter.pack(tid_int, self._options)
                self._after_pack()
            finally:
                adapter.release_pack_lock(lock_cursor)
        finally:
            lock_conn.rollback()
            adapter.close(lock_conn, lock_cursor)


    def _after_pack(self):
        """Reset the transaction state after packing."""
        # The tests depend on this.
        self._rollback_load_connection()

    def iterator(self, start=None, stop=None):
        return TransactionIterator(self._adapter, start, stop)


class BoundRelStorage(RelStorage):
    """Storage to a database, bound to a particular ZODB.Connection."""

    # The propagate_invalidations flag, set to a false value, tells
    # the Connection not to propagate object invalidations across
    # connections, since that ZODB feature is detrimental when the
    # storage provides its own MVCC.
    propagate_invalidations = False

    def __init__(self, parent, zodb_conn):
        # self._zodb_conn = zodb_conn
        RelStorage.__init__(self, adapter=parent._adapter, name=parent._name,
            create=False, read_only=parent._is_read_only,
            options=parent._options)
        # _prev_polled_tid contains the tid at the previous poll
        self._prev_polled_tid = None
        self._poll_at = 0

    def _get_oid_cache_key(self, oid_int):
        my_tid = self._prev_polled_tid
        if my_tid is None:
            return None
        return 'tid:%d:%d' % (oid_int, my_tid)

    def connection_closing(self):
        """Release resources."""
        if not self._options.poll_interval:
            self._rollback_load_connection()
        # else keep the load transaction open so that it's possible
        # to ignore the next poll.

    def sync(self):
        """Process pending invalidations regardless of poll interval"""
        self._lock_acquire()
        try:
            if self._load_transaction_open:
                self._rollback_load_connection()
        finally:
            self._lock_release()

    def poll_invalidations(self):
        """Looks for OIDs of objects that changed since _prev_polled_tid

        Returns {oid: 1}, or None if all objects need to be invalidated
        because prev_polled_tid is not in the database (presumably it
        has been packed).
        """
        self._lock_acquire()
        try:
            if self._closed:
                return {}

            if self._options.poll_interval:
                now = time.time()
                if self._load_transaction_open and now < self._poll_at:
                    # It's not yet time to poll again.  The previous load
                    # transaction is still open, so it's safe to
                    # ignore this poll.
                    return {}
                # else poll now after resetting the timeout
                self._poll_at = now + self._options.poll_interval

            self._restart_load()
            conn = self._load_conn
            cursor = self._load_cursor

            # Ignore changes made by the last transaction committed
            # by this connection.
            if self._ltid is not None:
                ignore_tid = u64(self._ltid)
            else:
                ignore_tid = None

            # get a list of changed OIDs and the most recent tid
            oid_ints, new_polled_tid = self._adapter.poll_invalidations(
                conn, cursor, self._prev_polled_tid, ignore_tid)
            self._prev_polled_tid = new_polled_tid

            if oid_ints is None:
                oids = None
            else:
                oids = {}
                for oid_int in oid_ints:
                    oids[p64(oid_int)] = 1
            return oids
        finally:
            self._lock_release()

    def _after_pack(self):
        # Override transaction reset after packing.  If the connection
        # wants to see the new state, it should call sync().
        pass


class TransactionIterator(object):
    """Iterate over the transactions in a RelStorage instance."""

    def __init__(self, adapter, start, stop):
        self._adapter = adapter
        self._conn, self._cursor = self._adapter.open_for_load()
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
        self._transactions = list(adapter.iter_transactions_range(
            self._cursor, start_int, stop_int))
        self._index = 0

    def close(self):
        self._adapter.close(self._conn, self._cursor)
        self._closed = True

    def iterator(self):
        return self

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._transactions)

    def __getitem__(self, n):
        self._index = n
        return self.next()

    def next(self):
        if self._closed:
            raise IOError("TransactionIterator already closed")
        if self._index >= len(self._transactions):
            raise StorageStopIteration()
        params = self._transactions[self._index]
        res = RelStorageTransactionRecord(self, *params)
        self._index += 1
        return res


class RelStorageTransactionRecord(TransactionRecord):

    def __init__(self, trans_iter, tid_int, user, desc, ext, packed):
        self._trans_iter = trans_iter
        self._tid_int = tid_int
        self.tid = p64(tid_int)
        self.status = packed and 'p' or ' '
        self.user = user or ''
        self.description = desc or ''
        if ext:
            self.extension = cPickle.loads(ext)
        else:
            self.extension = {}

    # maintain compatibility with the old (ZODB 3.8 and below) name of
    # the extension attribute.
    def _ext_set(self, value):
        self.extension = value
    def _ext_get(self):
        return self.extension
    _extension = property(fset=_ext_set, fget=_ext_get)

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
        self._records = list(adapter.iter_objects(cursor, tid_int))
        self._index = 0

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._records)

    def __getitem__(self, n):
        self._index = n
        return self.next()

    def next(self):
        if self._index >= len(self._records):
            raise StorageStopIteration()
        params = self._records[self._index]
        res = Record(self.tid, *params)
        self._index += 1
        return res


class Record(DataRecord):
    """An object state in a transaction"""
    version = ''
    data_txn = None

    def __init__(self, tid, oid_int, data):
        self.tid = tid
        self.oid = p64(oid_int)
        if data is not None:
            self.data = str(data)
        else:
            self.data = None


class Options:
    """Options for tuning RelStorage."""
    def __init__(self):
        self.poll_interval = 0
        self.pack_gc = True
        self.pack_batch_timeout = 5.0
        self.pack_duty_cycle = 0.5
        self.pack_max_delay = 20.0
        self.cache_servers = ()  # ['127.0.0.1:11211']
        self.cache_module_name = 'memcache'
