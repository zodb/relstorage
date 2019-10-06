# -*- coding: utf-8 -*-
"""
Helpers for cache testing.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope import interface

from relstorage.cache.cache import PyCache as _BaseCache
from relstorage.cache.local_client import LocalClient as _BaseLocalClient

from relstorage.tests import MockOptions
from relstorage.tests import MockCursor
from relstorage.tests import MockConnectionManager
from relstorage.tests import MockAdapter

class MockOptionsWithFakeMemcache(MockOptions):
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'


class Cache(object):
    # Tweak the generation sizes to match what we developed the tests with
    _gen_protected_pct = 0.8
    _gen_eden_pct = 0.1
    _gen_probation_pct = 0.1
    _dict_type = dict


    def __init__(self, byte_limit):
        self.__cache = _BaseCache(
            byte_limit * self._gen_eden_pct,
            byte_limit * self._gen_protected_pct,
            byte_limit * self._gen_probation_pct
        )
        interface.alsoProvides(self, interface.providedBy(self.__cache))

    def __getitem__(self, key):
        return self.__cache[key]

    def __len__(self):
        return len(self.__cache)

    def __setitem__(self, k, v):
        self.__cache[k] = v

    def __delitem__(self, k):
        del self.__cache[k]

    def __iter__(self):
        return iter(self.__cache)

    def __contains__(self, k):
        return k in self.__cache

    def __getattr__(self, name):
        return getattr(self.__cache, name)

    @property
    def size(self):
        return self.__cache.weight

class LocalClient(_BaseLocalClient):
    _cache_type = Cache

    def __init__(self, options, prefix=None):
        if isinstance(options, int):
            options = MockOptions(cache_local_mb=options)
        super(LocalClient, self).__init__(options, prefix=prefix)

def list_lrukeys(mapping, generation_name):
    # Remember, these lists will be from LRU to MRU
    return [e.key for e in getattr(mapping, generation_name)]

def list_lrufreq(mapping, generation_name):
    return [e.frequency for e in getattr(mapping, generation_name)]
