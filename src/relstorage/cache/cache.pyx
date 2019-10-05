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

from relstorage.cache.c_cache cimport TID_t
from relstorage.cache.c_cache cimport OID_t
from relstorage.cache.c_cache cimport AbstractEntry
from relstorage.cache.c_cache cimport SingleValueEntry
from relstorage.cache.c_cache cimport SingleValueEntry_p
from relstorage.cache.c_cache cimport AbstractEntry_p
from relstorage.cache.c_cache cimport MultipleValueEntry
from relstorage.cache.c_cache cimport MultipleValueEntry_p
from relstorage.cache.c_cache cimport Cache
from relstorage.cache.c_cache cimport Generation
from relstorage.cache.c_cache cimport rsc_eden_add_many
from relstorage.cache.c_cache cimport RSCache

from relstorage.cache.cache_values cimport value_from_entry
from relstorage.cache.cache_values cimport entry_from_python
from relstorage.cache.cache_values cimport CachedValue
from relstorage.cache.cache_values cimport pickle_from_python

from relstorage.cache.interfaces import NoSuchGeneration

from relstorage._compat import iteroiditems

cdef extern from *:
    """
    template <typename T>
    T* array_new(int n) {
        return new T[n];
    }
    template <typename T>
    void array_delete(T* t) {
        delete[] t;
    }
    template <typename T>
    std::shared_ptr<T> shared_array_new(int n) {
        return std::shared_ptr<T>(new T[n], array_delete<T>);
    }
    """
    T* array_new[T](int)
    shared_ptr[T] shared_array_new[T](int)
    void array_delete[T](T* t)

cdef class PyGeneration:
    cdef Generation* generation
    cdef readonly object __name__
    cdef PyCache _cache

    @staticmethod
    cdef from_generation(Generation* gen, name, cache):
        cdef PyGeneration pygen = PyGeneration.__new__(PyGeneration)
        pygen.generation = gen
        pygen.__name__ = name
        pygen._cache = cache
        return pygen

    @property
    def generation_number(self):
        return self.generation.generation

    @property
    def limit(self):
        return self.generation.max_weight

    @property
    def weight(self):
        return self.generation.sum_weights

    def __len__(self):
        return self.generation.len

    # Cython still uses __nonzero__
    def __nonzero__(self):
        return self.generation.len > 0

    def __iter__(self):
        "Not thread safe."
        cdef AbstractEntry* here
        if self.generation.ring_is_empty():
            return ()
        here = <AbstractEntry*>self.generation.r_next
        while here != <void*>self.generation:
            yield self._cache.get(here.key)
            here = <AbstractEntry*>here.r_next

@cython.final
cdef class PyCache:
    cdef Cache* cache
    cdef readonly size_t sets
    cdef readonly size_t hits
    cdef readonly size_t misses

    def __cinit__(self, eden, protected, probation):
        self.cache = new Cache(eden, protected, probation)
        self.sets = self.hits = self.misses = 0

    def __dealloc__(self):
        del self.cache

    cpdef reset_stats(self):
        self.hits = self.sets = self.misses = 0

    @property
    def limit(self):
        return (self.cache.ring_eden.max_weight
                + self.cache.ring_protected.max_weight
                + self.cache.ring_probation.max_weight)

    # Access to generations
    @property
    def eden(self):
        return PyGeneration.from_generation(<Generation*>self.cache.ring_eden,
                                            'eden',
                                            self)

    @property
    def protected(self):
        return PyGeneration.from_generation(<Generation*>self.cache.ring_protected,
                                            'protected',
                                            self)

    @property
    def probation(self):
        return PyGeneration.from_generation(<Generation*>self.cache.ring_probation,
                                            'probation',
                                            self)
    @property
    def generations(self):
        return [NoSuchGeneration(0),
                self.eden,
                self.protected,
                self.probation,]

    # Mapping operations

    def __nonzero__(self):
        # Cython still uses __nonzero__
        return self.cache.len() > 0

    def __contains__(self, OID_t key):
        return self.cache.contains(key)

    def __len__(self):
        return self.cache.len()

    cpdef CachedValue get(self, OID_t key):
        if not self.cache.contains(key):
            return None
        return value_from_entry(self.cache.get(key))

    cpdef CachedValue peek(self, OID_t key):
        return self.get(key)

    cpdef object peek_item_with_tid(self, OID_t key, TID_t tid):
        cdef CachedValue value = self.get(key)
        if value is not None:
            value = value.get_if_tid_matches(tid)
        return value

    def __getitem__(self, OID_t key):
        return self.get(key)

    cpdef get_item_with_tid(self, OID_t key, tid):
        cdef CachedValue value = self.get(key)
        if value is not None:
            value = value.get_if_tid_matches(tid)

        if value is not None:
            self.cache.on_hit(key)
            self.hits += 1
        else:
            self.misses += 1
        return value

    def __setitem__(self, OID_t key, tuple value):
        # Do all this down here so we don't give up the GIL.
        cdef object state
        cdef TID_t tid
        if not self.cache.contains(key): # the long way to avoid type conversion
            state, tid = value
            pickle = pickle_from_python(state)

            self.cache.add_to_eden(
                make_shared[SingleValueEntry](
                    key,
                    pickle,
                    tid
                )
            )
        else:
            # We need to merge values.
            # TODO: Avoid the trip to Python. Implement with_later
            # in C++?
            # The pointer may or may not be stored in the dict,
            # depending if we grew or not.
            self.cache.update_MRU(
                entry_from_python(
                    self.get(key).with_later(value)
                )
            )

        self.sets += 1

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

    def values(self):
        """
        Iterate across the values in the cache.

        Not thread safe.
        """
        for thing in self.cache.getData():
            yield value_from_entry(thing.second)

    # Cache specific operations

    def add_MRUs(self, ordered_keys, bint return_count_only=False):
        cdef SingleValueEntry* array
        cdef SingleValueEntry* single_entry
        cdef SingleValueEntry_p shared_ptr_to_array
        cdef SingleValueEntry_p ptr_to_entry
        cdef OID_t key
        cdef bytes state
        cdef TID_t tid
        cdef int i
        cdef int number_nodes = len(ordered_keys)
        cdef int added_count
        if not number_nodes:
            return 0 if return_count_only else ()

        shared_ptr_to_array = shared_array_new[SingleValueEntry](number_nodes)
        array = shared_ptr_to_array.get()

        for i, (key, (state, tid)) in enumerate(ordered_keys):
            single_entry = array + i
            single_entry.key = key
            single_entry.state = state
            single_entry.tid = tid

        # We're done with ordered_keys, free its memory
        ordered_keys = None

        added_count = rsc_eden_add_many[SingleValueEntry](
            <RSCache&>deref(self.cache),
            array,
            number_nodes)

        # Things that didn't get added have -1 for their generation.
        i = 0
        result = []
        while i < added_count:
            if array[i].generation != -1:

                key = array[i].key
                self.cache.getData()[key] = dynamic_pointer_cast[
                    AbstractEntry, SingleValueEntry](
                        SingleValueEntry_p(shared_ptr_to_array, array + i)
                    )
                if not return_count_only:
                    result.append(self.get(key))
            i += 1

        return result if not return_count_only else added_count

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
            if not self.cache.contains(oid):
                continue

            value = self.get(oid)

            orig_value = value
            orig_weight = value.weight
            value = value.discarding_tids_before(expected_tid)
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

    def del_oids(self, oids):
        """
        For each oid in OIDs, remove it.
        """
        for oid in oids:
            self.cache.delitem(oid)

    def freeze(self, oids_tids):
        # The idea is to *move* the data, or make it available,
        # *without* copying it.
        cdef OID_t oid
        cdef TID_t tid
        cdef CachedValue value

        for oid, tid in iteroiditems(oids_tids):
            if not self.cache.contains(oid):
                continue
            value = self.get(oid)
            orig_value = value
            orig_weight = value.weight
            value = value.freeze_to_tid(tid)
            if value is None:
                del self[oid]
                continue
            if value is not orig_value or value.weight != orig_weight:
                self.cache.replace_entry(
                    entry_from_python(value),
                    entry_from_python(orig_value),
                    orig_weight
                )
    @property
    def weight(self):
        return self.cache.weight()
