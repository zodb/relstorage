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
import sqlite3
import threading
import zlib

from zope import interface

from relstorage._compat import iteritems
from relstorage._compat import get_memory_usage
from relstorage.cache.persistence import sqlite_connect
from relstorage.cache.interfaces import IStateCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import OID_OBJECT_MAP_TYPE
from relstorage.cache.interfaces import OID_TID_MAP_TYPE
from relstorage.cache.interfaces import CacheCorruptedError
from relstorage.cache.mapping import SizedLRUMapping as LocalClientBucket

logger = __import__('logging').getLogger(__name__)

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

    def save(self, overwrite=False):
        options = self.options
        if options.cache_local_dir and self.__bucket.size:
            conn, _ = sqlite_connect(options, self.prefix, overwrite=overwrite)
            with conn:
                try:
                    self.write_to_sqlite(conn)
                except CacheCorruptedError:
                    # The cache_trace_analysis.rst test fills
                    # us with junk data and triggers this.
                    logger.exception("Failed to save cache")
                conn.execute('PRAGMA optimize')

    def restore(self):
        """
        Return ``(max_tids, delta_map)``.

        *max_tids* is a list of the three biggest tids that we just loaded.

        *delta_map* is a map from OID to the exact TID we have for it.

        If no data could be loaded, returns ``None``.
        """
        options = self.options
        if options.cache_local_dir:
            conn, fname = sqlite_connect(options, self.prefix)
            with conn:
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

    def reset_stats(self):
        self.__bucket.reset_stats()

    def stats(self):
        return self.__bucket.stats()

    def __getitem__(self, oid_tid):
        return self(*oid_tid)

    def __call__(self, oid, tid1, tid2=None):
        decompress = self._decompress
        get = self.__bucket.get_and_bubble_all
        if tid2 is not None:
            keys = ((oid, tid1), (oid, tid2))
        else:
            keys = ((oid, tid1),)

        with self._lock:
            res = get(keys)
            if tid2 is not None and keys[0] not in res and keys[1] in res:
                # A hit on the backup data. Move it to the
                # preferred location.
                data = res[keys[1]]
                self.__bucket[keys[0]] = data
                res[keys[0]] = data

        # Finally, while not holding the lock, decompress if needed
        try:
            state, tid = res[keys[0]]
        except KeyError:
            return None

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
            self.__bucket[oid_tid] = (cvalue, tid)

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

    def read_from_sqlite(self, connection, storage=None):
        mem_before = get_memory_usage()
        try:
            cur = connection.execute("SELECT cp0, cp1 FROM checkpoints")
        except sqlite3.OperationalError:
            # No table, we must not have saved here before.
            return

        checkpoints = cur.fetchone()
        if checkpoints:
            cp0, cp1 = checkpoints
            self.store_checkpoints(cp0, cp1)
        else:
            cp0, cp1 = (0, 0)

        delta_after0 = OID_TID_MAP_TYPE()
        delta_after1 = OID_TID_MAP_TYPE()
        # TODO: Read these in priority order. We don't have that
        # stored yet. So as a proxy that looks good in benchmarks,
        # we read them in descending TID order to get the most recently
        # updated items in the cache.
        cur.execute('SELECT COUNT(zoid) FROM object_state')
        total_count = cur.fetchone()[0]

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
        # XXX: TODO: Index on tid.
        cur.execute("""
        SELECT zoid, tid, state
        FROM object_state
        ORDER BY tid desc
        """)

        def data():
            rows = []
            bytes_read = 0
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
                    # TODO: Track the frequency in the database.
                    key = (oid, cp0)

                rows.append((key, (state, tid)))
                bytes_read += len(state) + 32 + 16
                if bytes_read > self.limit:
                    break
            cur.close()
            return rows

        self.__bucket.bulk_update(data(),
                                  source=storage or connection,
                                  log_count=total_count,
                                  mem_usage_before=mem_before)

        return delta_after0, delta_after1

    def _items_to_write(self):
        base_entries = self.__bucket.items_to_write()
        # Only write the newest entry for each OID.

        # {oid -> (oid, actual_tid, state)}
        newest_entries = OID_OBJECT_MAP_TYPE()
        for k, v, _ in base_entries:
            oid, _ = k
            state, actual_tid = v

            entry_for_oid = newest_entries.get(oid)
            stored_tid = entry_for_oid[1] if entry_for_oid else -1
            if stored_tid < actual_tid:
                newest_entries[oid] = (oid, actual_tid, state)
            elif stored_tid == actual_tid and state != newest_entries[oid][2]:
                other_entries = [
                    (k, v) for k, v, _ in base_entries
                    if k[0] == oid
                ]
                raise CacheCorruptedError(
                    "Cache corrupted; OID %s has two different states for tid %s:\n%r\n%r\n"
                    "All entries:\n%r"
                    % (k, actual_tid, state, newest_entries[oid][2], other_entries)
                )

        del base_entries
        return newest_entries.values()

    def write_to_sqlite(self, connection):
        import time
        from relstorage.adapters.batch import RowBatcher

        supports_upsert = sqlite3.sqlite_version_info >= (3, 28)

        # Create the table, if needed

        create_stmt = """
            CREATE TABLE IF NOT EXISTS object_state (
                zoid INTEGER PRIMARY KEY, tid INTEGER, state BLOB
            )"""
        connection.execute(create_stmt)

        tcreate_stmt = create_stmt.replace("CREATE TABLE IF NOT EXISTS",
                                           'CREATE TEMPORARY TABLE')
        tcreate_stmt = tcreate_stmt.replace("object_state", 'temp_state')
        connection.execute(tcreate_stmt)

        create_stmt = """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY, cp0 INTEGER, cp1 INTEGER
            )"""
        connection.execute(create_stmt)

        now = time.time()

        bytes_written = 0
        count_written = 0

        cur = connection.cursor()
        batch = RowBatcher(cur, row_limit=300)
        # The batch size depends on how many params a stored proc can
        # have; if we go too big we get OperationalError: too many SQL
        # variables. Note that the multiple-value syntax was added in
        # 3.7.11, 2012-03-20

        # TODO: Tracking of frequency data and aging old
        # data out of the cache.
        cur.execute('BEGIN')
        # No need to even put these into the database if we're
        # not going to use it
        # TODO: How does this interact with aging out?
        cur.execute('SELECT zoid, tid FROM object_state')
        stored_oid_tid = OID_TID_MAP_TYPE(list(cur))

        row = (1, )
        for row in self._items_to_write():
            if stored_oid_tid.get(row[0], 0) >= row[1]:
                continue

            size = len(row[2])
            bytes_written += size
            count_written += 1

            batch.insert_into(
                'temp_state(zoid, tid, state)',
                '?, ?, ?',
                row,
                row[0],
                size
            )

        batch.flush()
        cur.execute("COMMIT")
        batch_time = time.time()

        # Take out locks now; if we don't, we can get 'OperationalError: database is locked'
        # But beginning immediate lets us stand in line.
        cur.execute('BEGIN IMMEDIATE')
        if supports_upsert:
            cur.execute("""
                INSERT INTO object_state (zoid, tid, state)
                SELECT zoid, tid, state
                FROM temp_state
                WHERE true
                ON CONFLICT(zoid) DO UPDATE SET tid = excluded.tid, state = excluded.state
                WHERE excluded.tid > tid
                """)
        else:
            cur.execute("""
            DELETE FROM object_state
            WHERE zoid IN (SELECT zoid FROM temp_state)
            """)
            cur.execute("""
                INSERT INTO object_state (zoid, tid, state)
                SELECT zoid, tid, state
                FROM temp_state
            """)

        if self.checkpoints:
            if supports_upsert:
                cur.execute("""
                INSERT INTO checkpoints (id, cp0, cp1)
                VALUES (0, ?, ?)
                ON CONFLICT(id) DO UPDATE SET cp0 = excluded.cp0, cp1 = excluded.cp1
                WHERE excluded.cp0 > cp0
                """, (self.checkpoints[0], self.checkpoints[1]))
            else:
                cur.execute("SELECT cp0, cp1 FROM checkpoints")
                row = cur.fetchone()
                if not row:
                    # First time in.
                    cur.execute("""
                    INSERT INTO checkpoints (id, cp0, cp1)
                    VALUES (0, ?, ?)
                    """, (self.checkpoints[0], self.checkpoints[1]))
                elif row[0] < self.checkpoints[0]:
                    cur.execute("""
                    UPDATE checkpoints SET cp0 = ?, cp1 = ?
                    """, (self.checkpoints[0], self.checkpoints[1]))

        cur.execute('COMMIT')
        then = time.time()
        stats = self.stats()
        logger.info(
            "Wrote %d items (%d bytes) to %s in %s (%s to insert batch). "
            "Total hits %s; misses %s; ratio %s",
            count_written, bytes_written, connection, then - now, batch_time - now,
            stats['hits'], stats['misses'], stats['ratio'])
