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
from cython.operator cimport dereference as deref
from libcpp.memory cimport shared_ptr
from libcpp.memory cimport make_shared
from libcpp.memory cimport dynamic_pointer_cast
from libcpp.cast cimport static_cast
from libcpp.string cimport string

from relstorage.cache.lru_cache cimport TID_t
from relstorage.cache.lru_cache cimport OID_t
from relstorage.cache.lru_cache cimport AbstractEntry
from relstorage.cache.lru_cache cimport SingleValueEntry
from relstorage.cache.lru_cache cimport SingleValueEntry_p
from relstorage.cache.lru_cache cimport AbstractEntry_p
from relstorage.cache.lru_cache cimport MultipleValueEntry
from relstorage.cache.lru_cache cimport MultipleValueEntry_p
from relstorage.cache.lru_cache cimport Cache

from relstorage.cache.cache_values cimport value_from_entry
from relstorage.cache.cache_values cimport entry_from_python

from relstorage._compat import iteroiditems

cdef class CCache:
    cdef Cache* cache

    def __cinit__(self, eden, protected, probation):
        self.cache = new Cache(eden, protected, probation)

    def __dealloc__(self):
        del self.cache

    # Mapping operations

    def __bool__(self):
        return self.cache.len() > 0

    __nonzero__ = __bool__

    def __contains__(self, OID_t key):
        return self.cache.contains(key)

    def __len__(self):
        return self.cache.len()

    cpdef object get(self, OID_t key):
        if not self.cache.contains(key):
            return None
        return value_from_entry(self.cache.get(key))

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
        cdef AbstractEntry_p from_python
        cdef string state_str
        if isinstance(value, tuple):
            # It must not have already been in self
            # to get a raw value passed down from Python.
            assert key not in self
            state, tid = value
            state_str = (state or b'')
            self.cache.add_to_eden(make_shared[SingleValueEntry](key, state_str, tid))
        else:
            # This could either be a merged value created
            # in Python by += or a shrunk value from delitems()
            from_python = entry_from_python(value)
            self.cache.update_MRU(from_python)

    def __delitem__(self, OID_t key):
        self.cache.delitem(key)

    def __iter__(self):
        """
        Iterate across all the contained OID/TID pairs (the keys).

        Note that the number of items in the iterator may exceed
        the number of items in the cache due to aliasing.

        This is not thread safe.
        """
        for pair in self.cache.getData():
            # XXX: we shouldn't have to go to python to get this.
            python = value_from_entry(pair.second)
            oid = pair.first
            if pair.second.get().len() > 1:
                for entry in python:
                    yield (oid, entry.tid)
            else:
                yield (oid, python.tid)

    def iteritems(self):
        """
        Iterate across the oid/cache_value pairs.

        Not thread safe.
        """
        for thing in self.cache.getData():
            yield (thing.first, value_from_entry(thing.second))

    def keys(self):
        """
        Iterate across the OIDs in the cache.

        Not thread safe.
        """
        for oid, _ in self:
            yield oid

    # Cache specific operations

    def age_frequencies(self):
        self.cache.age_frequencies()

    def delitems(self, oids_tids):
        """
        For each OID/TID pair in the items, remove all cached values
        for OID that are older than TID.
        """
        cdef OID_t oid
        cdef TID_t expected_tid
        cdef AbstractEntry_p from_python

        for oid, expected_tid in iteroiditems(oids_tids):
            value = self.get(oid)
            if value is not None:
                orig_value = value
                orig_weight = value.weight
                value -= expected_tid
                if value is None:
                    # Whole thing should be removed.
                    del self[oid]
                elif value is not orig_value or value.weight != orig_weight:
                    # Even if it's the same object it may have
                    # mutated in place and must be replaced.
                    self.cache.replace_entry(
                        entry_from_python(value),
                        entry_from_python(orig_value),
                        orig_weight
                    )
    def freeze(self, oids_tids):
        # The idea is to *move* the data, or make it available,
        # *without* copying it.
        cdef OID_t oid
        cdef TID_t tid

        for oid, tid in iteroiditems(oids_tids):
            value = self.get(oid)
            if value is not None:
                orig_value = value
                orig_weight = value.weight
                value <<= tid
                assert value is not None
                if value is not orig_value or value.weight != orig_weight:
                    self.cache.replace_entry(
                        entry_from_python(value),
                        entry_from_python(orig_value),
                        orig_weight
                    )



    @property
    def weight(self):
        return self.cache.weight()
