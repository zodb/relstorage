# -*- coding: utf-8 -*-
"""
Helpers for cache testing.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.cache.lru_cffiring import CFFICache as _BaseCache
from relstorage.cache.mapping import SizedLRUMapping as _BaseSizedLRUMapping
from relstorage.cache.local_client import LocalClient as _BaseLocalClient

from relstorage.tests import MockOptions
from relstorage.tests import MockCursor

class MockOptionsWithFakeMemcache(MockOptions):
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'

class MockConnmanager(object):
    def open_and_call(self, callback):
        return callback(None, None)

class MockAdapter(object):
    def __init__(self):
        self.mover = MockObjectMover()
        self.poller = MockPoller()
        self.connmanager = MockConnmanager()

class MockObjectMover(object):
    def __init__(self):
        self.data = {}  # {oid_int: (state, tid_int)}

    def load_current(self, _cursor, oid_int):
        return self.data.get(oid_int, (None, None))

    def current_object_tids(self, _cursor, oids):
        return {
            oid: self.data[oid][1]
            for oid in oids
            if oid in self.data
        }

class MockPoller(object):
    def __init__(self):
        self.changes = []  # [(oid, tid)]
    def list_changes(self, _cursor, after_tid, last_tid):
        # Return a list, because the caller is allowed
        # to assume a length. Return exactly the item in the list because
        # it may be a type other than a tuple
        return [
            item
            for item in self.changes
            if item[1] > after_tid and item[1] <= last_tid
        ]


class Cache(_BaseCache):
    # Tweak the generation sizes to match what we developed the tests with
    _gen_protected_pct = 0.8
    _gen_eden_pct = 0.1


class SizedLRUMapping(_BaseSizedLRUMapping):
    _cache_type = Cache


class LocalClient(_BaseLocalClient):
    _bucket_type = SizedLRUMapping

def list_lrukeys(mapping, generation_name):
    # Remember, these lists will be from LRU to MRU
    return [e.key for e in getattr(mapping, '_' + generation_name)]

def list_lrufreq(mapping, generation_name):
    return [e.frequency for e in getattr(mapping, '_' + generation_name)]
