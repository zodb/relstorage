# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True
"""
Python wrappers for the generational cache itself.

The cache controls object lifetime.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

cimport cython
from cython.operator cimport typeid
from libcpp.typeinfo cimport type_info

from relstorage.cache.lru_cache cimport TID_t
from relstorage.cache.lru_cache cimport OID_t
from relstorage.cache.lru_cache cimport SingleValueEntry
from relstorage.cache.lru_cache cimport SingleValueEntry_p
from relstorage.cache.lru_cache cimport AbstractEntry_p
from relstorage.cache.lru_cache cimport MultipleValueEntry
from relstorage.cache.lru_cache cimport Cache

from relstorage.cache.cache_values cimport SingleValue
from relstorage.cache.cache_values cimport value_from_entry

cdef class CCache:
    cdef Cache* cache

    def __cinit__(self, eden, protected, probation):
        self.cache = new Cache(eden, protected, probation)

    def __dealloc__(self):
        del self.cache

    # Mapping operations

    def __bool__(self):
        return self.cache.len() > 0

    def __contains__(self, OID_t key):
        return self.cache.contains(key)

    def __len__(self):
        return self.cache.len()

    cpdef get(self, OID_t key):
        cdef AbstractEntry_p entry
        if not self.cache.contains(key):
            return None
        entry = self.cache.get(key)
        # XXX: Convert back to Python.
        # typeid(deref(entry)).name()

    peek = get

    def __getitem__(self, OID_t key):
        value = self.get(key)
        if value is not None:
            self.cache.on_hit(key)
            return value

    def __setitem__(self, OID_t key, value):
        # Do all this down here so we don't give up the GIL.
        cdef object state
        cdef TID_t tid
        cdef AbstractEntry_p existing
        state, tid = value
        if key in self:
            existing = self.cache.get(key)
            # To Python and merge. Or implement the algorithm lower.
            # existing += value
            # Then back to C...
            # and into the cache
            self.cache.update_MRU(existing.get())
            raise NotImplementedError
        else:
            self.cache.add_to_eden(new SingleValueEntry(key, state, tid, False))

    def __delitem__(self, OID_t key):
        self.cache.delitem(key)

    def age_frequencies(self):
        self.cache.age_frequencies()
