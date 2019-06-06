# -*- coding: utf-8 -*-
"""
Helpers for cache testing.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from relstorage.options import Options
from relstorage.cache.cache_ring import Cache as _BaseCache
from relstorage.cache.mapping import SizedLRUMapping as _BaseSizedLRUMapping
from relstorage.cache.local_client import LocalClient as _BaseLocalClient

class MockOptions(Options):
    cache_module_name = '' # disable
    cache_servers = ''
    cache_local_mb = 1
    cache_local_dir_count = 1 # shrink

    @classmethod
    def from_args(cls, **kwargs):
        inst = cls()
        for k, v in kwargs.items():
            setattr(inst, k, v)
        return inst

    def __setattr__(self, name, value):
        if name not in Options.valid_option_names():
            raise AttributeError("Invalid option", name) # pragma: no cover
        object.__setattr__(self, name, value)

class MockOptionsWithFakeMemcache(MockOptions):
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'

class MockAdapter(object):
    def __init__(self):
        self.mover = MockObjectMover()
        self.poller = MockPoller()

class MockObjectMover(object):
    def __init__(self):
        self.data = {}  # {oid_int: (state, tid_int)}
    def load_current(self, _cursor, oid_int):
        return self.data.get(oid_int, (None, None))

class MockPoller(object):
    def __init__(self):
        self.changes = []  # [(oid, tid)]
    def list_changes(self, _cursor, after_tid, last_tid):
        # Return a generator, because the caller shouldn't assume
        # a length. Return exactly the item in the list because
        # it may be a type other than a tuple
        for change in self.changes:
            _, tid = change
            if tid > after_tid and tid <= last_tid:
                yield change

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
