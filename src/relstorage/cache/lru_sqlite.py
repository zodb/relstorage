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

import sqlite3

from zope import interface

from .interfaces import ILRUEntry
from .interfaces import ILRUCache
from .persistence import sqlite_connect
from .local_database import SimpleQueryProperty
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
            frequencies INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (zoid, key_tid)
        );
        """)

        self.cursor = conn.cursor()


    size = weight = total_state_len = SimpleQueryProperty(
        "SELECT TOTAL(LENGTH(state)) FROM object_state"
    )

    total_state_count = SimpleQueryProperty(
        "SELECT COUNT(zoid) FROM object_state"
    )

    def stats(self):
        return {}

    def __len__(self):
        return self.total_state_count

    def add_MRU(self, key, value):
        # TODO: Conflict resolution
        # TODO: Frequency bumps and generation hopping.
        self.cursor.execute(
            'insert into object_state (zoid, key_tid, state, state_tid) '
            'values (?, ?, ?, ?)',
            key + value
        )
        return Item(key, value), ()

    def add_MRUs(self, key_values):
        def k():
            written = 0
            for k, v in key_values:
                written += len(v[0])
                if written < self.limit:
                    yield k + v
                else:
                    break

        self.cursor.executemany(
            'insert into object_state (zoid, key_tid, state, state_tid) '
            'values (?, ?, ?, ?)',
            k()
        )
        return ()

    def update_MRU(self, item, value):
        # TODO: Conflict resolution
        # TODO: Frequency bumps and generation hopping.
        self.cursor.execute(
            'update object_state set state = ?, state_tid = ?'
            'where zoid = ? and key_tid = ?',
            value + item.key
        )

    def _remove(self, key):
        self.cursor.execute(
            'delete from object_state where zoid = ? and key_tid = ?',
            key
        )

    def remove(self, item):
        self._remove(item.key)

    def age_frequencies(self):
        pass

    age_lists = age_frequencies

    def on_hit(self, entry):
        pass

    def entries(self):
        cur = self.connection.execute("SELECT zoid, key_tid, state, state_tid from object_state")
        for row in cur:
            yield Item(row[:2], row[2:])
        cur.close()

    def __contains__(self, key):
        return self[key] is not None

    def __getitem__(self, key):
        # TODO: Make this count as a hit.
        cur = self.connection.execute(
            'SELECT state, state_tid FROM object_state '
            'WHERE zoid = ? and key_tid = ? ',
            key
        )
        row = cur.fetchone()
        cur.close()
        return row

    peek = __getitem__ # peek is not supposed to count as a hit

    def __iter__(self):
        cur = self.connection.execute('SELECT zoid, key_tid FROM object_state')
        for row in cur:
            yield row
        cur.close()

    def __setitem__(self, key, value):
        # TODO: Upserts where possible.
        try:
            self.add_MRU(key, value)
        except sqlite3.IntegrityError:
            self.update_MRU(Item(key, value), value)

    def __delitem__(self, key):
        self._remove(key)

@interface.implementer(ILRUEntry)
class Item(object):
    weight = None
    frequency = 0

    def __init__(self, key, value):
        self._key = key
        self._value = value

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value



class SqlMapping(SizedLRUMapping):
    _cache_type = SQLiteCache
