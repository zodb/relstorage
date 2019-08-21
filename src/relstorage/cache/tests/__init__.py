# -*- coding: utf-8 -*-
"""
Helpers for cache testing.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.cache.lru_cffiring import CFFICache as _BaseCache
from relstorage.cache.local_client import LocalClient as _BaseLocalClient

from relstorage.tests import MockOptions
from relstorage.tests import MockCursor
from relstorage.tests import MockConnectionManager
from relstorage.tests import MockAdapter

class MockOptionsWithFakeMemcache(MockOptions):
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'


class Cache(_BaseCache):
    # Tweak the generation sizes to match what we developed the tests with
    _gen_protected_pct = 0.8
    _gen_eden_pct = 0.1
    _dict_type = dict


class LocalClient(_BaseLocalClient):
    _cache_type = Cache

def list_lrukeys(mapping, generation_name):
    # Remember, these lists will be from LRU to MRU
    return [e.key for e in getattr(mapping, generation_name)]

def list_lrufreq(mapping, generation_name):
    return [e.frequency for e in getattr(mapping, generation_name)]
