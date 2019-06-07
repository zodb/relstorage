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

from .interfaces import ILRUItem
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
        conn = self.connection = sqlite_connect(":memory:", 'ignored', close_async=False)
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
        self.cursor.execute(
            'insert into object_state (zoid, key_tid, state, state_tid) '
            'values (?, ?, ?, ?)',
            key + value
        )
        return Item(key, value), ()

    def add_MRUs(self, key_values):
        self.cursor.executemany(
            'insert into object_state (zoid, key_tid, state, state_tid) '
            'values (?, ?, ?, ?)',
            (k + v for k, v in key_values)
        )
        return ()

    def update_MRU(self, item, value):
        self.cursor.execute(
            'update object_state set state = ?, state_tid = ?'
            'where zoid = ? and key_tid = ?',
            value + item.key
        )

    def remove(self, item):
        self.cursor.execute(
            'delete from object_state where zoid = ? and key_tid = ?',
            (item.key[0], item.key[1])
        )


    def age_frequencies(self):
        pass

    def on_hit(self, entry):
        pass

@interface.implementer(ILRUItem)
class Item(object):
    key = None
    value = None
    weight = None
    frequency = 0

    def __init__(self, key, value):
        self.key = key
        self.value = value

# This is temporary until we move the data storage down.
class ItemDict(object):
    def __init__(self, cache):
        self.lru_cache = cache
        self.conn = cache.connection

    def __bool__(self):
        return len(self.lru_cache) > 0

    __nonzero__ = __bool__

    def __len__(self):
        return len(self.lru_cache)

    def __setitem__(self, key, value):
        pass

    def values(self):
        cur = self.conn.execute("SELECT zoid, key_tid, state, state_tid from object_state")
        for row in cur:
            yield Item(row[:2], row[2:])

    itervalues = values

class SqlMapping(SizedLRUMapping):
    _cache_type = SQLiteCache

    def __init__(self, *args, **kwargs):
        super(SqlMapping, self).__init__(*args, **kwargs)
        self._dict = ItemDict(self._cache)
