# -*- coding: utf-8 -*-
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
"""
The definition of how local cache databases are manipulated.

This is the bare bones byte-shuffling layer, it defines as little policy
as possible (while keeping in mind its purpose.)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import abstractmethod
from contextlib import closing
import sqlite3

from relstorage._compat import ABC
from relstorage._compat import OID_TID_MAP_TYPE
from relstorage._util import log_timed
from relstorage.adapters.batch import RowBatcher

from .persistence import SQ3_SUPPORTS_UPSERT as SUPPORTS_UPSERT
from .persistence import SQ3_SUPPORTS_PAREN_UPDATE as SUPPORTS_PAREN_UPDATE


logger = __import__('logging').getLogger(__name__)

class SimpleQueryProperty(object):
    """
    Wraps a query that returns one value in one row.
    """

    def __init__(self, sql, param_names=()):
        self.query = sql
        self.param_names = param_names

    def __get__(self, inst, klass):
        if inst is None: # pragma: no cover
            return self

        cur = inst.connection.execute(
            self.query,
            [getattr(inst, n) for n in self.param_names])

        result = cur.fetchone()[0]
        cur.close()
        return result



class Database(ABC):
    """
    The database stores only one state for each object.

    This should generally be the latest state found in the cache.
    """

    @classmethod
    def from_connection(cls,
                        connection,
                        use_upsert=SUPPORTS_UPSERT,
                        use_paren_update=SUPPORTS_PAREN_UPDATE):
        kind = _UpsertUpdateDatabase
        if not use_upsert:
            kind = _ParenUpdateDatabase if use_paren_update else _OldUpdateDatabaseModel
        return kind(connection)

    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.cursor.arraysize = 100
        self.create_schema()
        self.close = self.connection.close

    # The main repository of our data. This uses the OID of the object
    # as the INTEGER PRIMARY KEY --- that's a special type of key that
    # means this table is a clustered table, organized with that
    # column as its primary key.
    #
    # This reduces overhead of having a secondary (hidden) 'rowid' column
    # to do the clustering on, to it's important.
    _state_table_schema = """
    CREATE TABLE IF NOT EXISTS object_state (
        zoid INTEGER PRIMARY KEY,
        tid INTEGER NOT NULL ,
        frequency INTEGER NOT NULL,
        state BLOB
    );
    """

    # We want to keep the clustering for the temporary table,
    # so the integer primary key matters.
    _temp_table_schema = _state_table_schema.replace(
        "object_state", 'temp_state'
    ).replace('TABLE', 'TEMPORARY TABLE')

    _schema = _state_table_schema + '\n' + _temp_table_schema + """
    CREATE TABLE IF NOT EXISTS checkpoints (
        id INTEGER PRIMARY KEY, cp0 INTEGER, cp1 INTEGER
    );

    CREATE INDEX IF NOT EXISTS IX_object_state_f_tid
    ON object_state (frequency DESC, tid DESC);
    """

    total_state_len = SimpleQueryProperty(
        "SELECT TOTAL(LENGTH(state)) FROM object_state"
    )

    total_state_count = SimpleQueryProperty(
        "SELECT COUNT(zoid) FROM object_state"
    )

    def create_schema(self):
        self.cursor.executescript(self._schema)

    @property
    @log_timed
    def oid_to_tid(self):
        """
        A map from OID to its corresponding TID, for
        all the data in the database.
        """
        cur = self.connection.execute('SELECT zoid, tid FROM object_state')
        with closing(cur):
            return OID_TID_MAP_TYPE(cur.fetchall())

    @property
    def checkpoints(self):
        """
        The checkpoints in the database, or None if there are none.
        """
        self.cursor.execute("SELECT cp0, cp1 FROM checkpoints")
        return self.cursor.fetchone()

    def _remove_invalid_persistent_oids(self, bad_oids, cur):
        cur.execute("BEGIN")
        batch = RowBatcher(cur,
                           row_limit=999 // 1,
                           delete_placeholder='?')
        for oid in bad_oids:
            batch.delete_from('object_state', zoid=oid)
        batch.flush()
        cur.execute("COMMIT")
        return batch.total_rows_deleted

    def remove_invalid_persistent_oids(self, bad_oids):
        # The database might be locked by others, either someone in
        # this method or someone actually closing the cache and
        # writing to the database, a situation we won't detect until
        # we try to actually remove a row (it's entirely possible the
        # rows we want to remove are already gone, so we don't BEGIN
        # IMMEDIATE to force the issue.)
        #
        # Our workaround is to try a few times and then give up. Longer timeouts
        # make it less likely we need to do this.
        tries = 3
        cur = self.cursor
        while tries:
            tries -= 1
            try:
                return self._remove_invalid_persistent_oids(bad_oids, cur)
            except sqlite3.OperationalError:
                # If we don't rollback, we get 'cannot BEGIN inside a transaction'
                cur.execute('ROLLBACK')
                # No need to sleep, that's built in to the timeout parameter
                # when we connect.
                logger.debug("Failed to lock database to remove OIDs; tries left: %d", tries)
        return -1

    def fetch_rows_by_priority(self):
        """
        The returned cursor will iterate ``(zoid, tid, state, tid)``
        from most useful to least useful.

        The extra tid is to allow for a uniform row syntax when we
        don't want to do any key tid transforms. The row can neatly be
        sliced as ``[r:2], r[2:]`` to get ``(key, value)`` pairs.
        """
        # Do this in a new cursor so it can interleave.

        # Read these in priority order; as a tie-breaker, choose newer transactions
        # over older transactions.
        # We could  use a window function over SUM(LENGTH(state)) to only select
        # the rows that will actually fit:
        #
        # SELECT * FROM (
        #  SELECT zoid, tid, state,
        #   sum(length(state)) over (order by frequency desc, tid desc) as cum_size
        #  FROM object_state
        # ORDER BY frequency DESC, tid DESC
        # )
        # WHERE cum_size < ?
        #
        # However, that seems to generate a poor query plan that actually looks
        # at all the rows (it doesn't understand that cum_size can only increase.)
        # Plus, window functions were only added to sqlite 3.25
        cur = self.connection.execute("""
        SELECT zoid, tid, state, tid
        FROM object_state
        ORDER BY frequency DESC, tid DESC
        """)
        cur.arraysize = 100
        return cur

    def store_temp(self, rows):
        """
        Given an iterator of ``(oid, tid, state, frequency)`` values,
        store them in a temporary table for this session.
        """
        # The batch size depends on how many params a stored proc can
        # have; if we go too big we get OperationalError: too many SQL
        # variables. The default is 999.
        # Note that the multiple-value syntax was added in
        # 3.7.11, 2012-03-20.

        # Benchmarking shows essentially no difference between this
        # simple method and using our RowBatcher to produce
        # multi-value statements. vmprof shows all of the time spent
        # in *this* function right here, nothing any lower (the next lower function it shows is
        # _pysqlite_fetch_one_row, taking  1.1% of the execution of *this* function).
        # I'm Not entirely sure what that means.
        rows = list(rows) # materialize
        self.cursor.executemany(
            'INSERT INTO temp_state(zoid, tid, state, frequency) '
            'VALUES (?, ?, ?, ?)',
            rows
        )

        return len(rows), -1

    @abstractmethod
    def move_from_temp(self):
        """
        Take rows in the temporary table and put them in the permanent table,
        overwriting rows for the same object that are older.

        If there is a row that is newer, then it is preserved and the temporary
        row is discarded.

        The temporary table will be clear after this.
        """
        raise NotImplementedError

    @abstractmethod
    def update_checkpoints(self, cp0, cp1):
        """
        Save these checkpoints, if they are newer than the current checkpoints.
        """
        raise NotImplementedError

    def trim_to_size(self, limit, min_allowed_oid):
        # Manipulates a transaction.
        if not min_allowed_oid and self.total_state_len <= limit:
            # Nothing to do.
            return

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
            cur.execute('DELETE FROM object_state WHERE is_stale(zoid, tid)')

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
            rows_deleted, limit, self.total_state_len
        )


    def _trim_state(self, how_much_to_trim):
        # Try to get the oldest, least used, biggest objects we can.
        # We could probably use a window function over SUM(LENGTH(state))
        # to limit the select to just the rows we want.

        # We'll be interleaving statements so we must use a
        # separate cursor
        batch_cur = self.connection.cursor()
        # In fact, because of the way PyPy wants you to fetch all rows
        # or it considers some statements to still be open and thus
        # refuses to allow things like VACUUM, we need to use two
        # cursors, so we can close the fetch cursor too
        fetch_cur = self.connection.cursor()
        fetch_cur.execute("""
        SELECT zoid, LENGTH(state)
        FROM object_state
        ORDER BY frequency ASC, tid ASC, LENGTH(state) DESC, zoid ASC
        """)
        batch = RowBatcher(batch_cur,
                           row_limit=999 // 1,
                           delete_placeholder='?')
        for row in fetch_cur:
            zoid, size = row
            how_much_to_trim -= size
            batch.delete_from('object_state', zoid=zoid)
            if how_much_to_trim <= 0:
                break
        batch.flush()
        batch_cur.close()
        fetch_cur.close()
        return batch.total_rows_deleted

class _UpsertUpdateDatabase(Database):

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
        self.cursor.execute("DELETE FROM temp_state")

    def update_checkpoints(self, cp0, cp1):
        self.cursor.execute("""
        INSERT INTO checkpoints (id, cp0, cp1)
        VALUES (0, ?, ?)
        ON CONFLICT(id) DO UPDATE SET cp0 = excluded.cp0, cp1 = excluded.cp1
        WHERE excluded.cp0 > cp0
        """, (cp0, cp1))

class _ParenUpdateDatabase(Database):
    def move_from_temp(self):
        self._update_existing_values()

        self.cursor.execute("""
        INSERT INTO object_state (zoid, tid, state, frequency)
        SELECT zoid, tid, state, frequency
        FROM temp_state
        WHERE zoid NOT IN (SELECT zoid FROM object_state)
        """)
        self.cursor.execute("DELETE FROM temp_state")

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


class _OldUpdateDatabaseModel(_ParenUpdateDatabase):
    """
    Fallback to using multiple sub-selects to update
    multiple columns.
    """
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

class _ExplainCursor(object): # pragma: no cover (A debugging aid)
    def __init__(self, cur):
        self.cur = cur

    def __getattr__(self, name):
        return getattr(self.cur, name)

    def __iter__(self):
        return iter(self.cur)

    def execute(self, sql, *args):
        if sql.strip().startswith(('INSERT', 'SELECT', 'DELETE', 'WITH')):
            exp = 'EXPLAIN QUERY PLAN ' + sql.lstrip()
            print(sql)
            self.cur.execute(exp, *args)
            for row in self.cur:
                print(*row)
        return self.cur.execute(sql, *args)
