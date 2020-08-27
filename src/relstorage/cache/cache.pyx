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
from cython.operator cimport preincrement as preincr
from cpython.buffer cimport PyBuffer_FillInfo
from cpython.bytes cimport PyBytes_AsString # Does NOT copy

from libcpp.pair cimport pair
from libcpp.cast cimport static_cast
from libcpp.cast cimport dynamic_cast
from libcpp.string cimport string

from relstorage.cache.c_cache cimport TID_t
from relstorage.cache.c_cache cimport OID_t
from relstorage.cache.c_cache cimport ICacheEntry
from relstorage.cache.c_cache cimport SVCacheEntry
from relstorage.cache.c_cache cimport MVCacheEntry
from relstorage.cache.c_cache cimport Cache
from relstorage.cache.c_cache cimport Generation
from relstorage.cache.c_cache cimport move
from relstorage.cache.c_cache cimport TempCacheFiller
from relstorage.cache.c_cache cimport ProposedCacheEntry

import sys
from relstorage.cache.interfaces import NoSuchGeneration
from relstorage.cache.interfaces import CacheConsistencyError

from relstorage._compat import iteroiditems

cdef extern from *:
    """
    #ifdef __clang__
    #pragma clang diagnostic push
    /* Cython generates lots of unreachable code diagnostics that flood the output */
    #pragma clang diagnostic ignored "-Wunreachable-code"
    /* As of Cython 3.0a6 and CPython 3.8 (at least) Cython generates
       deprecation warnings for tp_print */
    #pragma clang diagnostic ignored "-Wdeprecated-declarations"
    #endif

    template <typename T>
    T* array_new(int n) {
        return new T[n];
    }
    template <typename T>
    void array_delete(T* t) {
        delete[] t;
    }
    """
    T* array_new[T](int)
    void array_delete[T](T* t)

ctypedef const SVCacheEntry* SVCacheEntry_p
ctypedef const MVCacheEntry* MVCacheEntry_p
ctypedef const ICacheEntry*  ICacheEntry_p
ctypedef ICacheEntry** ICacheEntry_pp

cdef class CachedValue:
    """
    The base class for cached values.
    """

cdef inline CachedValue python_from_entry_p(ICacheEntry_p entry):
    cdef const SVCacheEntry* sve_p = dynamic_cast[SVCacheEntry_p](entry)
    cdef const MVCacheEntry* mve_p = dynamic_cast[MVCacheEntry_p](entry)
    if not sve_p and not mve_p:
        # Most likely, entry is null
        raise AssertionError("Invalid pointer cast")

    # Keeping in mind the semantics of our constructors
    # and Cython destructors, we'll let the class constructors handle that.
    cdef SingleValue sv
    if sve_p:
        sv = SingleValue.from_entry(sve_p)
        return sv

    return MultipleValues.from_entry(mve_p)


cdef inline CachedValue python_from_entry(const ICacheEntry& entry):
    return python_from_entry_p(&entry)

cdef inline object bytes_from_pickle(const SVCacheEntry_p entry):
    return entry.as_object()

ctypedef fused ConcreteCacheEntry:
    SVCacheEntry_p
    MVCacheEntry_p

cdef inline void release_entry(ConcreteCacheEntry* entry) except *:
    # Docs say to be very careful during __dealloc__
    if entry[0] and entry[0].Py_release():
        del entry[0]
    entry[0] = NULL


@cython.final
@cython.internal
cdef class SingleValue(CachedValue):
    cdef SVCacheEntry_p entry

    def __cinit__(self):
        self.entry = <SVCacheEntry_p>0

    @staticmethod
    cdef SingleValue from_entry(SVCacheEntry_p entry):
        cdef SingleValue sv = SingleValue.__new__(SingleValue)
        sv.entry = entry.Py_use[SVCacheEntry]()
        return sv

    def __dealloc__(self):
        release_entry(&self.entry)

    def sizeof(self):
        # At this writing, reports 88
        return sizeof(SVCacheEntry)

    def __iter__(self):
        return iter((
            bytes_from_pickle(self.entry),
            self.entry.tid()
        ))

    @property
    def frozen(self):
        return self.entry.frozen()

    @property
    def value(self):
        return self.state

    @property
    def key(self):
        return self.entry.key

    @property
    def frequency(self):
        return self.entry.frequency

    @property
    def state(self):
        return bytes_from_pickle(self.entry)

    @property
    def tid(self):
        return self.entry.tid()

    @property
    def max_tid(self):
        return self.entry.tid()

    @property
    def newest_value(self):
        return self

    @property
    def weight(self):
        return self.entry.weight()

    def __eq__(self, other):
        cdef SingleValue p
        if other is self:
            return True

        if isinstance(other, SingleValue):
            p = <SingleValue>other
            my_entry = self.entry
            other_entry = p.entry
            return my_entry == other_entry

        if isinstance(other, tuple):
            return len(other) == 2 and self.tid == other[1] and self.value == other[0]
        return NotImplemented

    def __getitem__(self, int i):
        if i == 0:
            return self.state()
        if i == 1:
            return self.entry.tid()
        raise IndexError

    def __repr__(self):
        return "%s(%r, %s, frozen=%s)" % (
            self.__class__.__name__,
            self.state,
            self.tid,
            self.frozen,
        )

@cython.final
cdef class MultipleValues(CachedValue):
    cdef MVCacheEntry_p entry

    @staticmethod
    cdef MultipleValues from_entry(MVCacheEntry_p entry):
        cdef MultipleValues mv = MultipleValues.__new__(MultipleValues)
        mv.entry = entry.Py_use[MVCacheEntry]()
        return mv

    def __dealloc__(self):
        release_entry(&self.entry)

    def sizeof(self):
        # At this writing, reports 72.
        return sizeof(MVCacheEntry)

    @property
    def value(self):
        return list(self)

    @property
    def key(self):
        return self.entry.key

    @property
    def frequency(self):
        return self.entry.frequency

    @property
    def weight(self):
        return self.entry.weight()

    @property
    def max_tid(self):
        return self.entry.newest_tid()

    @property
    def newest_value(self):
        return python_from_entry_p(self.entry.copy_newest_entry());


cdef class PyGeneration:
    cdef Generation* generation
    cdef readonly object __name__
    # This reference is to keep the cache object alive while we're
    # alive since it owns the Generation*.
    cdef PyCache _cache

    @staticmethod
    cdef from_generation(Generation& gen, name, cache):
        cdef PyGeneration pygen = PyGeneration.__new__(PyGeneration)
        pygen.generation = &gen
        pygen.__name__ = name
        pygen._cache = cache
        return pygen

    @property
    def generation_number(self):
        return self.generation.generation

    @property
    def limit(self):
        return self.generation.max_weight()

    @property
    def weight(self):
        return self.generation.sum_weights()

    def __len__(self):
        return self.generation.size()

    # Cython still uses __nonzero__
    def __nonzero__(self):
        return not self.generation.empty()

    def __iter__(self):
        "Not thread safe."
        if self.generation.empty():
            return ()
        it = self.generation.begin()
        while it != self.generation.end():
            yield python_from_entry(deref(it))
            preincr(it)

@cython.final
cdef class PyCache:
    cdef Cache cache
    cdef readonly size_t sets
    cdef readonly size_t hits
    cdef readonly size_t misses

    def __cinit__(self, eden, protected, probation):
        self.cache.resize(eden, protected, probation)
        self.sets = self.hits = self.misses = 0

    cpdef reset_stats(self):
        self.hits = self.sets = self.misses = 0

    @property
    def limit(self):
        return self.cache.max_weight()

    # Access to generations
    @property
    def eden(self):
        return PyGeneration.from_generation(self.cache.ring_eden,
                                            'eden',
                                            self)

    @property
    def protected(self):
        return PyGeneration.from_generation(self.cache.ring_protected,
                                            'protected',
                                            self)

    @property
    def probation(self):
        return PyGeneration.from_generation(self.cache.ring_probation,
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
        return self.cache.size() > 0

    def __contains__(self, OID_t key):
        return self.cache.contains(key)

    def __len__(self):
        return self.cache.size()

    cpdef CachedValue get(self, OID_t key):
        entry = self.cache.get(key)
        if not entry:
            return None
        return python_from_entry_p(entry)

    cpdef CachedValue peek(self, OID_t key):
        return self.get(key)

    cpdef object peek_item_with_tid(self, OID_t key, TID_t tid):
        value = self.cache.peek(key, tid)
        if value:
            return python_from_entry_p(value)

    cpdef bint contains_oid_with_tid(self, OID_t key, tid):
        """
        For use during cache validation. Can only be used when
        the cache only contains SingleValue entries; failure
        to do this will lead to memory leaks.
        """
        cdef TID_t native_tid = -1 if tid is None else tid
        cdef SVCacheEntry* entry = self.cache.peek(key, native_tid)
        if not entry:
            return False
        # We're guaranteed that entry is an existing object, not one we need
        # to manage the lifetime of, because of the requirement that only
        # single values can be in the cache at this time.
        return True

    def __getitem__(self, OID_t key):
        return self.get(key)

    cpdef get_item_with_tid(self, OID_t key, tid):
        cdef TID_t native_tid = -1 if tid is None else tid
        cdef SVCacheEntry* cvalue = self.cache.get(key, native_tid)

        if cvalue:
            self.hits += 1
            return SingleValue.from_entry(cvalue)

        self.misses += 1

    def __setitem__(self, OID_t key, tuple value):
        self._do_set(key, value[0], value[1])

    cdef _do_set(self, OID_t key, object state, TID_t tid):
        # Do all this down here so we don't give up the GIL.
        cdef object b_state = state if state is not None else b''
        cdef ProposedCacheEntry proposed = ProposedCacheEntry(key, tid, b_state)
        if not self.cache.contains(key): # the long way to avoid type conversion
            self.cache.add_to_eden(proposed)
        else:
            # We need to merge values.
            try:
                self.cache.store_and_make_MRU(proposed)
            except RuntimeError as e:
                raise CacheConsistencyError(str(e))

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
        it = self.cache.begin()
        end = self.cache.end()

        while it != end:
            oid = deref(it).key
            # This should be a small list. It mustn't persist
            # across the yield boundary, we don't own it but Cython
            # tries to keep it as a reference
            tids = list(deref(it).all_tids())
            for t in tids:
                yield (oid, t)
            preincr(it)

    def iteritems(self):
        """
        Iterate across the oid/cache_value pairs.

        Not thread safe.
        """
        it = self.cache.begin()
        end = self.cache.end()

        while it != end:
            yield (deref(it).key, python_from_entry(deref(it)))
            preincr(it)

    def keys(self):
        """
        Iterate across the OIDs in the cache.

        Not thread safe.
        """
        it = self.cache.begin()
        end = self.cache.end()

        while it != end:
            yield deref(it).key
            preincr(it)

    def values(self):
        """
        Iterate across the values in the cache.

        Not thread safe.
        """
        it = self.cache.begin()
        end = self.cache.end()

        while it != end:
            yield python_from_entry(deref(it))
            preincr(it)

    # Cache specific operations

    cpdef set_all_for_tid(self, TID_t tid_int, state_oid_iter, compress, Py_ssize_t value_limit):
        # Do all this down here so we don't give up the GIL, assuming the iter is actually a list
        # or otherwise not implemented in pure-python
        cdef OID_t oid_int
        for state_bytes, oid_int, _ in state_oid_iter:
            state_bytes = compress(state_bytes) if compress is not None else state_bytes
            state_bytes = state_bytes if state_bytes is not None else b''
            if len(state_bytes) >= value_limit:
                # This value is too big, so don't cache it.
                continue
            self._do_set(oid_int, state_bytes, tid_int)


    def add_MRUs(self, ordered_keys, return_count_only=False):
        cdef OID_t key
        cdef TID_t tid
        cdef int i
        cdef int number_nodes = len(ordered_keys)
        cdef TempCacheFiller filler
        if not number_nodes:
            return 0 if return_count_only else ()

        filler = TempCacheFiller(number_nodes)

        for i, (key, (state, tid, frozen, frequency)) in enumerate(ordered_keys):
            state = state or b''
            filler.set_once_at(i, key, tid, state, frozen, frequency)


        # We're done with ordered_keys, free its memory
        ordered_keys = None

        added_oids = self.cache.add_many(filler)

        # Things that didn't get added have -1 for their generation.
        if return_count_only:
            return added_oids.size()

        result = [self.get(key) for key in added_oids]

        return result

    def age_frequencies(self):
        self.cache.age_frequencies()

    def delitems(self, oids_tids):
        """
        For each OID/TID pair in the items, remove all cached values
        for OID that are older than TID.
        """
        cdef OID_t oid
        cdef TID_t tid

        for oid, tid in iteroiditems(oids_tids):
            self.cache.delitem(oid, tid)

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

        for oid, tid in iteroiditems(oids_tids):
            self.cache.freeze(oid, tid)

    @property
    def weight(self):
        return self.cache.weight()

# Local Variables:
# flycheck-cython-cplus: t
# End:
