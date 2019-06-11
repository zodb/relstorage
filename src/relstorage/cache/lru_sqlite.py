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
sqlite3 based implementation of ``ILRUCache``.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope import interface

from .interfaces import ILRUEntry
from .interfaces import ILRUCache
from .persistence import sqlite_connect
from .local_database import SimpleQueryProperty
from .local_database import SUPPORTS_UPSERT
from .mapping import SizedLRUMapping

logger = __import__('logging').getLogger(__name__)

@interface.implementer(ILRUCache)
class SQLiteCache(object):

    def __init__(self, limit, key_weight=None, value_weight=None): # pylint:disable=unused-argument
        # The weight functions are ignored, we don't need them.
        self.limit = limit
        conn = self.connection = sqlite_connect("", 'ignored', close_async=False)
        # isolation_level = None (autocommit)
        # WITHOUT ROWID
        #        pop    epop    read    mix
        # mem    5s     8.4s    2.3s    7.95s
        #        856MB  782MB
        # temp   6080ms 22.2s   3080s   13.6s
        #        1.4MB  2.2MB
        #
        # WITH ROWID
        # mem    2.5    3.2ms   1660ms   4.2s
        #        200MB  189M
        # temp   2.8s   3360ms  1930ms   5050ms
        #        2.1MB  172KB
        #
        # CFFI ring
        #        832ms  982ms   355ms    1340ms
        #        8.16MB 13.7MB
        conn.executescript("""
        CREATE TABLE object_state(
            zoid INTEGER NOT NULL,
            key_tid INTEGER NOT NULL,
            state BLOB,
            state_tid INTEGER NOT NULL,
            frequency INTEGER NOT NULL DEFAULT 1,
            time_of_use INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (zoid, key_tid)
        );

        CREATE INDEX ix_ttl ON object_state (time_of_use);
        """)

        self.cursor = conn.cursor()
        self.time_of_use_counter = 0

    size = weight = total_state_len = SimpleQueryProperty(
        "SELECT TOTAL(LENGTH(state)) FROM object_state"
    )

    total_state_count = SimpleQueryProperty(
        "SELECT COUNT(zoid) FROM object_state"
    )

    def __repr__(self):
        return "<%s at %x using %r>" % (
            self.__class__.__name__, id(self),
            self.connection
        )

    def stats(self):
        return {}

    def __len__(self):
        return self.total_state_count

    if SUPPORTS_UPSERT:
        _insert_stmt = """
        INSERT
        INTO object_state (zoid, key_tid, state, state_tid, time_of_use)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (zoid, key_tid)
        DO UPDATE SET state = excluded.state, state_tid = excluded.state_tid,
                      time_of_use = excluded.time_of_use,
                      frequency = frequency + 1
        """
    else:
        # XXX: This seems really slow on conflicts.
        _insert_stmt = """
        INSERT OR REPLACE
        INTO object_state (zoid, key_tid, state, state_tid, time_of_use)
        VALUES (?, ?, ?, ?, ?)
        """

    def __setitem__(self, key, value):
        self.time_of_use_counter += 1
        cur = self.connection.execute(
            self._insert_stmt,
            key + value + (self.time_of_use_counter, )
        )
        cur.close()

    def add_MRU(self, key, value):
        self[key] = value
        return Item(key, value)

    def add_MRUs(self, key_values, return_count_only=False):
        items = []
        if return_count_only:
            items.append(0)

        def k(begin_size, counter):
            written = begin_size
            for k, v in key_values:
                state, state_tid = v
                weight = len(state)
                if written + weight <= self.limit:
                    written += weight
                    if return_count_only:
                        items[0] += 1
                    else:
                        items.append(Item(k, v, weight))
                    oid, key_tid = k
                    counter += 1
                    yield oid, key_tid, state, state_tid, counter
            self.time_of_use_counter = counter

        cur = self.connection.cursor()

        cur.execute("BEGIN")
        cur.execute('SELECT TOTAL(LENGTH(state)), COALESCE(MAX(time_of_use), 0) FROM object_state')
        begin_size, begin_counter = cur.fetchone()
        self.cursor.executemany(
            """
            INSERT OR REPLACE
            INTO object_state (zoid, key_tid, state, state_tid, time_of_use)
            VALUES (?, ?, ?, ?, ?)
            """,
            k(begin_size, begin_counter)
        )
        self._trim(cur)
        cur.execute("COMMIT")
        cur.close()
        return items if not return_count_only else items[0]

    def _trim(self, cur):
        size = self.size
        limit = self.limit
        if size > self.limit:
            # TODO: Can we write this with a CTE?
            # A window function doesn't help.

            cur.execute(
                'SELECT rowid, length(state) FROM object_state '
                'ORDER BY time_of_use, frequency, LENGTH(state) DESC'
            )

            to_remove = []
            for row in cur:
                to_remove.append(row[:1])
                size -= row[1]
                if size <= limit:
                    break

            cur.executemany("""
            DELETE FROM object_state
            WHERE rowid = ?
            """, to_remove)


    def age_frequencies(self):
        cur = self.connection.cursor()
        cur.execute('BEGIN')
        cur.execute("UPDATE object_state SET frequency = frequency / 2")
        # Remove 0 frequency items before trying more expensive
        # trimming.
        cur.execute('DELETE FROM object_state WHERE frequency = 0')
        self._trim(cur)
        cur.execute('COMMIT')

    age_lists = age_frequencies

    def entries(self):
        cur = self.connection.execute(
            "SELECT zoid, key_tid, state, state_tid, frequency "
            "FROM object_state "
            "ORDER BY time_of_use "
        )
        for row in cur:
            yield Item(row[:2], row[2:4], row[4])
        cur.close()

    def __contains__(self, key):
        return self.peek(key) is not None

    def __getitem__(self, key):
        cur = self.connection.execute(
            'SELECT state, state_tid, rowid FROM object_state '
            'WHERE zoid = ? and key_tid = ? ',
            key
        )
        result = None
        row = cur.fetchone()
        if row:
            self.time_of_use_counter += 1
            state, state_tid, rowid = row
            cur.execute(
                "UPDATE object_state SET frequency = frequency + 1, time_of_use = ? "
                "WHERE rowid = ?",
                (self.time_of_use_counter, rowid)
            )
            result = state, state_tid
        cur.close()
        return result

    def peek(self, key):
        cur = self.connection.execute(
            'SELECT state, state_tid FROM object_state '
            'WHERE zoid = ? and key_tid = ? ',
            key
        )
        row = cur.fetchone()
        cur.close()
        return row

    def get_from_key_or_backup_key(self, pref_key, backup_key):
        oid = pref_key[0]
        pref_tid = pref_key[1]
        if backup_key is not None:
            # Must be for the same zoid
            # Apparently we can't get row results from executescript()
            # even if a select is the last thing.
            assert backup_key[0] == oid
            backup_tid = backup_key[1]
            cur = self.connection.execute("""
            UPDATE object_state
            SET key_tid = {pref_tid:d}
            WHERE zoid = {oid:d}
            AND key_tid = {backup_tid:d}
            AND NOT EXISTS (
                SELECT 1 FROM object_state
                WHERE zoid = {oid:d}
                AND key_tid = {pref_tid:d}
            );
            """.format(oid=oid, backup_tid=backup_tid, pref_tid=pref_tid))

        cur = self.connection.execute(
            'SELECT state, state_tid FROM object_state '
            'WHERE zoid = ? and key_tid = ? ',
            pref_key
        )

        row = cur.fetchone()
        cur.close()
        return row

    def __iter__(self):
        cur = self.connection.execute('SELECT zoid, key_tid FROM object_state')
        for row in cur:
            yield row
        cur.close()

    def __delitem__(self, key):
        self.cursor.execute(
            'delete from object_state where zoid = ? and key_tid = ?',
            key
        )


@interface.implementer(ILRUEntry)
class Item(object):
    weight = None
    frequency = 0

    def __init__(self, key, value, frequency=0, weight=None):
        self.key = key
        self.value = value
        self.frequency = frequency
        self.weight = weight if weight else len(value[0])

class SqlMapping(SizedLRUMapping):
    _cache_type = SQLiteCache
