##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import bz2
import functools
import sqlite3
import threading
import time
import zlib

from contextlib import closing

from zope import interface

from relstorage._compat import iteritems
from relstorage._compat import get_memory_usage
from relstorage.adapters.batch import RowBatcher
from relstorage.cache.persistence import sqlite_connect
from relstorage.cache.interfaces import IStateCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import OID_OBJECT_MAP_TYPE
from relstorage.cache.interfaces import OID_TID_MAP_TYPE
from relstorage.cache.interfaces import MAX_TID
from relstorage.cache.interfaces import CacheCorruptedError
from relstorage.cache.mapping import SizedLRUMapping as LocalClientBucket

logger = __import__('logging').getLogger(__name__)

class _timer(object):
    begin = None
    end = None
    duration = None

    try:
        from pyperf import perf_counter as counter
    except ImportError: # pragma: no cover
        counter = time.time

    def __enter__(self):
        self.begin = self.counter()
        return self

    def __exit__(self, t, v, tb):
        self.end = self.counter()
        self.duration = self.end - self.begin

def _log_timed(func):
    @functools.wraps(func)
    def timer(*args, **kwargs):
        t = _timer()
        with t:
            result = func(*args, **kwargs)
        logger.debug("Function %s took %s", func.__name__, t.duration)
        return result
    return timer


@interface.implementer(IStateCache,
                       IPersistentCache)
class LocalClient(object):

    # Use the same markers as zc.zlibstorage (well, one marker)
    # to automatically avoid double-compression
    _compression_markers = {
        'zlib': (b'.z', zlib.compress),
        'bz2': (b'.b', bz2.compress),
        'none': (None, None)
    }
    _decompression_functions = {
        b'.z': zlib.decompress,
        b'.b': bz2.decompress
    }

    _bucket_type = LocalClientBucket

    def __init__(self, options, prefix=None):
        self._lock = threading.Lock()
        self.options = options
        self.checkpoints = None
        self.prefix = prefix or ''
        # XXX: The calc for limit is substantially smaller
        # The real MB value is 1024 * 1024 = 1048576
        self.limit = int(1000000 * options.cache_local_mb)
        self._value_limit = options.cache_local_object_max
        self.__bucket = None

        # The {oid: tid} that we read from the cache.
        # These are entries that we know are there, and if we see them
        # change, we need to be sure to update that in the database,
        # *even if they are evicted* and we would otherwise lose
        # knowledge of them before we save. We do this by watching incoming
        # TIDs; only if they were already in here do we continue to keep track.
        # At write time, if we can't meet the requirement ourself, we at least
        # make sure there are no stale entries in the cache database.
        self.__min_allowed_writeback = OID_TID_MAP_TYPE()

        self.flush_all()

        compression_module = options.cache_local_compression
        try:
            compression_markers = self._compression_markers[compression_module]
        except KeyError:
            raise ValueError("Unknown compression module")
        else:
            self.__compression_marker = compression_markers[0]
            self.__compress = compression_markers[1]
            if self.__compress is None:
                self._compress = None

    @property
    def size(self):
        return self.__bucket.size

    def __len__(self):
        return len(self.__bucket)

    def __iter__(self):
        return iter(self.__bucket)

    def _decompress(self, data):
        pfx = data[:2]
        if pfx not in self._decompression_functions:
            return data
        return self._decompression_functions[pfx](data[2:])

    def _compress(self, data): # pylint:disable=method-hidden
        # We override this if we're disabling compression
        # altogether.
        # Use the same basic rule as zc.zlibstorage, but bump the object size up from 20;
        # many smaller object (under 100 bytes) like you get with small btrees,
        # tend not to compress well, so don't bother.
        if data and (len(data) > 100) and data[:2] not in self._decompression_functions:
            compressed = self.__compression_marker + self.__compress(data)
            if len(compressed) < len(data):
                return compressed
        return data

    @_log_timed
    def save(self, overwrite=False, close_async=True):
        options = self.options
        if options.cache_local_dir and self.__bucket.size:
            conn, pathname = sqlite_connect(options, self.prefix,
                                            overwrite=overwrite, close_async=close_async)
            with closing(conn):
                try:
                    self.write_to_sqlite(conn)
                except CacheCorruptedError:
                    # The cache_trace_analysis.rst test fills
                    # us with junk data and triggers this.
                    logger.exception("Failed to save cache")
            # Testing: Return a signal when we tried to write
            # something.
            return pathname

    def restore(self):
        """
        Return ``(delta_after0, delta_after1)``.

        If no data could be loaded, returns ``None``.
        """
        options = self.options
        if options.cache_local_dir:
            conn, fname = sqlite_connect(options, self.prefix, close_async=False)
            with closing(conn):
                return self.read_from_sqlite(conn, fname)

    @property
    def _bucket0(self):
        # For testing only.
        return self.__bucket

    @staticmethod
    def key_weight(_):
        # All keys are equally weighted: the size of two 64-bit
        # integers
        return 32

    @staticmethod
    def value_weight(value):
        # Values are the (state, actual_tid) pair, and their
        # weight is the size of the state plus one 64-bit integer
        return len(value[0] if value[0] else b'') + 16

    def flush_all(self):
        with self._lock:
            self.__bucket = self._bucket_type(
                self.limit,
                key_weight=self.key_weight,
                value_weight=self.value_weight,
            )
            self.checkpoints = None
            self.__min_allowed_writeback = OID_TID_MAP_TYPE()

    def reset_stats(self):
        self.__bucket.reset_stats()

    def stats(self):
        return self.__bucket.stats()

    def __getitem__(self, oid_tid):
        return self(*oid_tid)

    def __call__(self, oid, tid1, tid2=None, _keys=None):
        # _keys is a hook for testing.
        decompress = self._decompress
        get = self.__bucket.get_from_key_or_backup_key

        with self._lock:
            if _keys is not None:
                res = get(*_keys)
            else:
                key1 = (oid, tid1)
                backup_key = (oid, tid2) if tid2 is not None else None
                res = get(key1, backup_key)

        # Finally, while not holding the lock, decompress if needed
        if res is not None:
            state, tid = res
            return decompress(state), tid

    def __setitem__(self, oid_tid, state_bytes_tid):
        if not self.limit:
            # don't bother
            return

        # This used to allow non-byte values, but that's confusing
        # on Py3 and wasn't used outside of tests, so we enforce it.
        # A state of 'None' happens for undone transactions.
        state_bytes, tid = state_bytes_tid
        assert isinstance(state_bytes, bytes) or state_bytes is None, type(state_bytes)
        compress = self._compress
        cvalue = compress(state_bytes) if compress else state_bytes # pylint:disable=not-callable

        if cvalue and len(cvalue) >= self._value_limit:
            # This value is too big, so don't cache it.
            return

        with self._lock:
            self.__bucket[oid_tid] = (cvalue, tid) # possibly evicts
            if tid > self.__min_allowed_writeback.get(oid_tid[0], MAX_TID):
                self.__min_allowed_writeback[oid_tid[0]] = tid

    def set_multi(self, keys_and_values):
        for k, v in iteritems(keys_and_values):
            self[k] = v

    def store_checkpoints(self, cp0, cp1):
        # No lock, the assignment should be atomic
        self.checkpoints = cp0, cp1

    def get_checkpoints(self):
        return self.checkpoints

    def close(self):
        pass

    def items(self):
        return self.__bucket.items()

    def values(self):
        return self.__bucket.values()

    @_log_timed
    def read_from_sqlite(self, connection, storage=None):
        import gc
        gc.collect() # Free memory, we're about to make big allocations.
        mem_before = get_memory_usage()

        db = _DatabaseModel(connection)
        checkpoints = db.checkpoints
        if checkpoints:
            cp0, cp1 = checkpoints
            self.store_checkpoints(cp0, cp1)
        else:
            cp0, cp1 = (0, 0)

        delta_after0 = OID_TID_MAP_TYPE()
        delta_after1 = OID_TID_MAP_TYPE()
        #cur = _ExplainCursor(cur)

        self.__min_allowed_writeback = db.oid_to_tid

        # XXX: The cache_ring is going to consume all the items we
        # give it, even if it doesn't actually add them to the cache.
        # It also creates a very large preallocated array for all to
        # hold the `total_count` of items. We don't want to read more
        # rows than necessary, to keep the delta maps small and to
        # avoid the overhead; we could pass the COUNT(zoid) to
        # `bulk_update`, and have our row iterable be a generator that
        # reads a row and breaks when it reaches the limit, but then
        # we have that whole preallocated array hanging around, which
        # is probably much bigger than we need. So we need to give an
        # accurate count, and the easiest way to do that is to materialize
        # the list of rows.
        @_log_timed
        def data():
            # Do this here so that we don't have the result
            # in a local variable and it can be collected
            # before we measure the memory delta.
            #
            # In large benchmarks, this function accounts for 31%
            # of the time to load data; iterating the cursor takes 12%
            # and allocating the state bytes takes 11%.
            rows = []
            bytes_read = 0
            cur = db.fetch_rows_by_priority()

            for row in cur:
                oid = row[0]
                tid = row[1]
                state = row[2]

                if tid >= cp0:
                    key = (oid, tid)
                    delta_after0[oid] = tid
                elif tid >= cp1:
                    key = (oid, tid)
                    delta_after1[oid] = tid
                else:
                    # Old generation, no delta.
                    # Even though this is old, it could be good to have it,
                    # it might be something that doesn't change much.
                    key = (oid, cp0)

                rows.append((key, (state, tid)))
                bytes_read += len(state) + 32 + 16
                if bytes_read > self.limit:
                    break
            cur.close()
            # Look at them from most to least recent,
            # but insert them the other way
            rows.reverse()
            return rows

        self.__bucket.bulk_update(data(),
                                  source=storage or connection,
                                  log_count=len(self.__min_allowed_writeback),
                                  mem_usage_before=mem_before)

        return delta_after0, delta_after1

    @_log_timed
    def _items_to_write(self, stored_oid_tid):
        # pylint:disable=too-many-locals
        all_entries = self.__bucket.items_to_write(sort=False)
        all_entries_len = len(all_entries)

        # Only write the newest entry for each OID.
        # Track frequency information for the OID, not the (oid, tid)

        # Newly added items have a frequency of 1. They *may* be
        # useless stuff we picked up from an old cache file and
        # haven't touched again, or they may be something that
        # really is new and we have no track history for (that
        # would be in eden generation).
        # Ideally we want to do something here that's similar to what the
        # SegmentedLRU does: if we would reject an item from eden, then only
        # keep it if it's better than the least popular thing in probation.

        # But we have a problem: If there's an object in the existing
        # cache that we have updated but now exclude, we would fail to
        # write its new data. So we can't exclude anything that's
        # changed without also nuking it from the DB.

        # Also, if we had a value loaded from the DB, and then we
        # modified it, but later ejected all entries for that object
        # from the cache, we wouldn't see it here, and we could be
        # left with stale data in the database. We must track the
        # (oid, tid) we load from the database and record that we
        # should DELETE those rows if that happens.
        # This is done with the __min_allowed_writeback dictionary,
        # which we mutate here: When we find a row we can write that meets
        # the requirement, we take it out of the dictionary.
        # Later, if there's anything left in the dictionary, we *know* there
        # may be stale entries in the cache, and we remove them.

        # TODO: Finish tuning these.
        # These apply to the first place we saw the OID
        # {oid ->
        #   First, the components of the row so we can slice
        #   ['oid', 'tid', 'state', 'frequency',
        #    'generation', 'min_allowed_tid', 'allowed'])
        newest_entries = OID_OBJECT_MAP_TYPE()
        pop_min_required_tid = self.__min_allowed_writeback.pop
        get_min_required_tid = self.__min_allowed_writeback.get # pylint:disable=no-member
        get_current_oid_tid = stored_oid_tid.get
        thresholds = {'eden': 0, 'protected': 1, 'probation': 0}
        with _timer() as t:
            # This is very ugly, but it's been pretty optimized.
            # Creating the new list is 14% of the runtime of a large
            # storage benchmark. This whole function is 18%.
            while all_entries:
                entry = all_entries.pop() # let us reclaim memory as we go
                oid = entry.key[0]
                current = newest_entries.get(oid)
                if current is None:
                    # We must have something at least this fresh
                    # to consider writing it out
                    min_allowed = get_min_required_tid(oid, -1)
                    # If we have something >= min_allowed, but == this,
                    # it's not worth putting in the database. To accomodate both
                    # conditions, we artificially inflate this.
                    current_tid = get_current_oid_tid(oid, -2) + 1
                    min_allowed = current_tid if current_tid > min_allowed else min_allowed
                    current = newest_entries[oid] = [
                        oid, -1, None, 0,
                        entry.ring_name,
                        min_allowed,
                        False
                    ]
                current[3] += entry.frequency
                state, tid = entry.value
                current_tid = current[1]
                if tid > current_tid:
                    current[1] = tid
                    current[2] = state
                elif tid == current_tid:
                    if state != current[2]:
                        raise CacheCorruptedError(
                            "The object %x has two different states for transaction %x" % (
                                current[0], tid
                            )
                        )

                current[6] = (
                    current_tid >= current[5]
                    # basically, if we only saw a OID in the protected generation,
                    # it must have been used more than once.
                    and current[3] > thresholds[current[4]]
                )
                del state
                del current
            del all_entries

            # TODO: Need a specific test that this is whitling down to the objects
            # we want.
            # Iterate this; we produce lots of garbage from the slicing, etc,
            # and we want to reclaim as we go.
            final_entry_count = 0
            for key in list(newest_entries):
                value = newest_entries.pop(key)
                if value[6]:
                    final_entry_count += 1
                    yield value[:4]
                    # We're able to satisfy this, so we don't need to consider
                    # it in our min_allowed set anymore.
                    pop_min_required_tid(value[0], None)

        logger.debug("Consolidated from %d entries to %d entries in %s",
                     all_entries_len, final_entry_count,
                     t.duration)

    @_log_timed
    def write_to_sqlite(self, connection):

        # pylint:disable=too-many-locals
        cur = connection.cursor()

        db = _DatabaseModel(connection)
        begin = time.time()

        # The batch size depends on how many params a stored proc can
        # have; if we go too big we get OperationalError: too many SQL
        # variables. The default is 999.
        # Note that the multiple-value syntax was added in
        # 3.7.11, 2012-03-20.
        with _timer() as batch_timer:
            cur.execute('BEGIN')
            stored_oid_tid = db.oid_to_tid
            fetch_current = time.time()
            count_written, bytes_written = db.store_temp(self._items_to_write(stored_oid_tid))
            cur.execute("COMMIT")


        # Take out (exclusive) locks now; if we don't, we can get
        # 'OperationalError: database is locked' But beginning
        # immediate lets us stand in line.
        with _timer() as exclusive_timer:
            cur.execute('BEGIN IMMEDIATE')
            # During the time it took for us to commit, and the time that we
            # got the lock, it's possible that someone else committed and
            # changed the data in object_state
            # Things might have changed in the database since our
            # snapshot where we put temp objects in, so we can't rely on
            # just assuming that's the truth anymore.
            db.move_from_temp()
            if self.checkpoints:
                db.update_checkpoints(*self.checkpoints)

            cur.execute('COMMIT')
        # TODO: Maybe use BTrees.family.intersection to get the common keys?
        # Or maybe use a SQLite Python function?
        # 'DELETE from object_state where is_stale(zoid, tid)'
        # What's fastest? The custom function version would require scanning the entire
        # table; unfortunately this sqlite interface doesn't allow defining virtual tables.
        with _timer() as trim_timer:
            # Delete anything we still know is stale, because we
            # saw a new TID for it, but we had nothing to replace it with.
            min_allowed_writeback = OID_TID_MAP_TYPE()
            for k, v in self.__min_allowed_writeback.items():
                if stored_oid_tid.get(k) < v:
                    min_allowed_writeback[k] = v
            db.trim_to_size(self.limit, min_allowed_writeback)
            del min_allowed_writeback
        # Typically we write when we're closing and don't expect to do
        # anything else, so there's no need to keep tracking this info.
        self.__min_allowed_writeback = OID_TID_MAP_TYPE()

        # Cleanups
        cur.close()

        del db
        del stored_oid_tid

        with _timer() as gc_timer:
            import gc
            gc.collect()

        end = time.time()
        stats = self.stats()
        logger.info(
            "Wrote %d items (%d bytes) to %s in %s "
            "(%s to fetch current, %s to insert batch, %s to write, %s to trim, %s to gc). "
            "Total hits %s; misses %s; ratio %s (stores: %s)",
            count_written, bytes_written, connection, end - begin,
            fetch_current - begin, batch_timer.duration, exclusive_timer.duration,
            trim_timer.duration, gc_timer.duration,
            stats['hits'], stats['misses'], stats['ratio'], stats['sets'])

        return count_written

class _SimpleQueryProperty(object):
    """
    Wraps a query that returns one value in one row.
    """

    def __init__(self, sql):
        self.sql = sql

    def __get__(self, inst, klass):
        if inst is None:
            return self

        inst.cursor.execute(self.sql)
        return inst.cursor.fetchone()[0]

class _DatabaseModel(object):
    SUPPORTS_UPSERT = sqlite3.sqlite_version_info >= (3, 28)
    SUPPORTS_PAREN_UPDATE = sqlite3.sqlite_version_info >= (3, 15)

    def __new__(cls,
                connection, # pylint:disable=unused-argument
                use_upsert=SUPPORTS_UPSERT,
                use_paren_update=SUPPORTS_PAREN_UPDATE):
        kind = _UpsertUpdateDatabaseModel
        if not use_upsert:
            kind = _ParenUpdateDatabaseModel if use_paren_update else _OldUpdateDatabaseModel
        return object.__new__(kind)

    def __init__(self, connection,
                 use_upsert=SUPPORTS_UPSERT,
                 use_paren_update=SUPPORTS_PAREN_UPDATE):
        self.connection = connection
        self.cursor = connection.cursor()
        self.cursor.arraysize = 100
        self.create_schema()
        self.use_Upsert = use_upsert
        self.use_paren_update = use_paren_update

    def close(self):
        self.connection.close()

    _schema = """
    CREATE TABLE IF NOT EXISTS object_state (
        zoid INTEGER PRIMARY KEY,
        tid INTEGER NOT NULL ,
        frequency INTEGER NOT NULL,
        state BLOB
    );

    -- This loses all the constraints and primary key
    -- Does that matter?
    CREATE TEMPORARY TABLE temp_state
    AS
    SELECT * FROM object_state
    LIMIT 0;

    CREATE TABLE IF NOT EXISTS checkpoints (
        id INTEGER PRIMARY KEY, cp0 INTEGER, cp1 INTEGER
    );

    CREATE INDEX IF NOT EXISTS IX_object_state_f_tid
    ON object_state (frequency DESC, tid DESC);
    """

    total_state_len = _SimpleQueryProperty(
        "SELECT SUM(LENGTH(state)) FROM object_state"
    )

    total_state_count = _SimpleQueryProperty(
        "SELECT COUNT(zoid) FROM object_state"
    )

    def create_schema(self):
        self.cursor.executescript(self._schema)

    @property
    def oid_to_tid(self):
        """
        A map from OID to its corresponding TID, for
        all the data in the database.
        """
        self.cursor.execute('SELECT zoid, tid FROM object_state')
        return OID_TID_MAP_TYPE(list(self.cursor))

    @property
    def checkpoints(self):
        self.cursor.execute("SELECT cp0, cp1 FROM checkpoints")
        return self.cursor.fetchone()

    def fetch_rows_by_priority(self):
        """
        The returned cursor will iterate ``(zoid, tid, state)``
        from most useful to least useful.
        """
        # Do this in a new cursor so it can interleave.

        # Read these in priority order; as a tie-breaker, choose newer transactions
        # over older transactions.
        # We could probably use a window function over SUM(LENGTH(state)) to only select
        # the rows that will actually fit.
        cur = self.connection.execute("""
        SELECT zoid, tid, state
        FROM object_state
        ORDER BY frequency DESC, tid DESC
        """)
        cur.arraysize = 100
        return cur

    def store_temp(self, rows):
        # The batch size depends on how many params a stored proc can
        # have; if we go too big we get OperationalError: too many SQL
        # variables. The default is 999.
        # Note that the multiple-value syntax was added in
        # 3.7.11, 2012-03-20.

        batch = RowBatcher(self.cursor, row_limit=999 // 4)

        for row in rows:
            size = len(row[2])
            batch.insert_into(
                'temp_state(zoid, tid, state, frequency)',
                '?, ?, ?, ?',
                row,
                row[0],
                size
            )

        batch.flush()
        return batch.total_rows_inserted, batch.total_size_inserted

    def move_from_temp(self):
        raise NotImplementedError

    def update_checkpoints(self, cp0, cp1):
        raise NotImplementedError

    def trim_to_size(self, limit, min_allowed_oid):
        # Manipulates a transaction.
        if not min_allowed_oid and self.total_state_len <= limit:
            # Nothing to do.
            return

        # TODO: Write tests for this
        # Take out the lock and check again.
        cur = self.cursor
        if min_allowed_oid:
            # This could be easily optimized for a small number of rows,
            # or use a custom RowBatcher that handles <= instead of =
            # operator.
            logger.info("Checking table of size %d against %d stale entries",
                        self.total_state_count, len(min_allowed_oid))
            def is_stale(zoid, tid, min_allowed=min_allowed_oid.get):
                return min_allowed(zoid, tid) > tid

            self.connection.create_function('is_stale', 2, is_stale)

        cur.execute('BEGIN IMMEDIATE')

        if min_allowed_oid:
            cur.execute('DELETE FROM object_state WHENE is_stale(zoid, tid)')

        byte_count = self.total_state_len
        if byte_count <= limit:
            # Someone else did it, yay!
            cur.execute('COMMIT')
            return

        really_big = byte_count > limit * 2
        how_much_to_trim = byte_count - limit
        logger.info(
            "State too large; need to trim %d to reach %d",
            how_much_to_trim,
            limit,
        )

        rows_deleted = self._trim_state(how_much_to_trim)

        cur.execute('COMMIT')
        # Rewrite the file? If we were way over our target, that probably
        # matters. And sometimes we might want to do it just to do it and
        # optimize the tables.
        if really_big:
            cur.execute('VACUUM')
        logger.info(
            "Trimmed %d rows (desired: %d actual: %d)",
            rows_deleted, limit, byte_count
        )


    def _trim_state(self, how_much_to_trim):
        # Try to get the oldest, least used objects we can.
        # We could probably use a window function over SUM(LENGTH(state))
        # to limit the select to just the rows we want.
        cur = self.cursor
        # We'll be interleaving statements so we must use a
        # separate cursor
        batch_cur = self.connection.cursor()
        cur.execute("""
        SELECT zoid, LENGTH(state)
        FROM object_state
        ORDER BY frequency ASC, tid ASC, zoid ASC
        """)
        batch = RowBatcher(batch_cur, row_limit=999 // 1)
        for row in cur:
            zoid, size = row
            how_much_to_trim -= size
            batch.delete_from('object_state', zoid=zoid)
            if how_much_to_trim <= 0:
                break
        batch.flush()
        batch_cur.close()
        return batch.total_rows_deleted

class _UpsertUpdateDatabaseModel(_DatabaseModel):

    def move_from_temp(self):
        self.cursor.execute("""
        INSERT INTO object_state (zoid, tid, frequency, state)
        SELECT zoid, tid, frequency, state
        FROM temp_state
        WHERE true
        ON CONFLICT(zoid) DO UPDATE
        SET tid = excluded.tid,
            state = excluded.state,
            frequency = excluded.frequency + object_state.frequency
        WHERE excluded.tid > tid
        """)

    def update_checkpoints(self, cp0, cp1):
        self.cursor.execute("""
        INSERT INTO checkpoints (id, cp0, cp1)
        VALUES (0, ?, ?)
        ON CONFLICT(id) DO UPDATE SET cp0 = excluded.cp0, cp1 = excluded.cp1
        WHERE excluded.cp0 > cp0
        """, (cp0, cp1))

class _ParenUpdateDatabaseModel(_DatabaseModel):
    def move_from_temp(self):
        self._update_existing_values()

        self.cursor.execute("""
        INSERT INTO object_state (zoid, tid, state, frequency)
        SELECT zoid, tid, state, frequency
        FROM temp_state
        WHERE zoid NOT IN (SELECT zoid FROM object_state)
        """)

    def _update_existing_values(self):
        self.cursor.execute("""
            WITH newer_values AS (SELECT temp_state.*
                FROM temp_state
                INNER JOIN object_state on temp_state.zoid = object_state.zoid
                WHERE object_state.tid < temp_state.tid
            )
            UPDATE object_state
            SET (tid, frequency, state) = (SELECT newer_values.tid,
                                            newer_values.frequency + object_state.frequency,
                                            newer_values.state
                                           FROM newer_values WHERE newer_values.zoid = zoid)
            WHERE zoid IN (SELECT zoid FROM newer_values)
        """)

    def update_checkpoints(self, cp0, cp1):
        cur = self.cursor
        cur.execute("SELECT cp0, cp1 FROM checkpoints")
        row = cur.fetchone()
        if not row:
            # First time in.
            cur.execute("""
            INSERT INTO checkpoints (id, cp0, cp1)
            VALUES (0, ?, ?)
            """, (cp0, cp1))
        elif row[0] < cp0:
            cur.execute("""
            UPDATE checkpoints SET cp0 = ?, cp1 = ?
            """, (cp0, cp1))


class _OldUpdateDatabaseModel(_ParenUpdateDatabaseModel):
    def _update_existing_values(self):
        self.cursor.execute("""
        WITH newer_values AS (SELECT temp_state.*
            FROM temp_state
            INNER JOIN object_state on temp_state.zoid = object_state.zoid
            WHERE object_state.tid < temp_state.tid
        )
        UPDATE object_state
        SET tid = (SELECT newer_values.tid
                   FROM newer_values WHERE newer_values.zoid = zoid),
        frequency = (SELECT  newer_values.frequency + object_state.frequency
                     FROM newer_values WHERE newer_values.zoid = zoid),
            state = (SELECT newer_values.state
                     FROM newer_values WHERE newer_values.zoid = zoid)
        WHERE zoid IN (SELECT zoid FROM newer_values)
        """)

class _ExplainCursor(object):
    def __init__(self, cur):
        self.cur = cur

    def __getattr__(self, name):
        return getattr(self.cur, name)

    def __iter__(self):
        return iter(self.cur)

    def execute(self, sql, *args):
        if sql.strip().startswith(('INSERT', 'SELECT', 'DELETE')):
            exp = 'EXPLAIN QUERY PLAN ' + sql.lstrip()
            print(sql)
            self.cur.execute(exp, *args)
            for row in self.cur:
                print(*row)
        return self.cur.execute(sql, *args)
