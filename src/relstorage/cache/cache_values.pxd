# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True
"""
Python wrappers for the values stored in the cache.

These objects accept shared pointers to the data stored in the cache,
which is in control of their lifetime.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

cimport cython
from cython.operator cimport dereference as deref
from cython.operator import postincrement as postinc

from libcpp.memory cimport shared_ptr

from relstorage.cache.lru_cache cimport TID_t
from relstorage.cache.lru_cache cimport OID_t
from relstorage.cache.lru_cache cimport SingleValueEntry
from relstorage.cache.lru_cache cimport SingleValueEntry_p
from relstorage.cache.lru_cache cimport MultipleValueEntry

cdef SingleValue value_from_entry(SingleValueEntry_p entry)
cdef class SingleValue:
    cdef SingleValueEntry_p entry
