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
from relstorage.cache.c_cache cimport Pickle_t
from relstorage.cache.c_cache cimport ICacheEntry
from relstorage.cache.c_cache cimport SVCacheEntry
from relstorage.cache.c_cache cimport MVCacheEntry
from relstorage.cache.c_cache cimport Cache
from relstorage.cache.c_cache cimport Generation
from relstorage.cache.c_cache cimport move

import sys
from relstorage.cache.interfaces import NoSuchGeneration
from relstorage.cache.interfaces import CacheConsistencyError

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
    """
    T* array_new[T](int)
    void array_delete[T](T* t)

ctypedef const SVCacheEntry* SVCacheEntry_p
ctypedef const MVCacheEntry* MVCacheEntry_p

cdef inline object python_from_entry_p(const ICacheEntry* entry):
    cdef const SVCacheEntry* sve_p = dynamic_cast[SVCacheEntry_p](entry)
    cdef const MVCacheEntry* mve_p = dynamic_cast[MVCacheEntry_p](entry)

    cdef SingleValue sv

    if sve_p:
        if sve_p.frozen():
            sv = FrozenValue.from_entry(sve_p)
        else:
            sv = SingleValue.from_entry(sve_p)
        return sv

    if not mve_p:
        raise AssertionError("Invalid pointer cast",
                             entry.key)
    return MultipleValues.from_entry(mve_p)

cdef inline object python_from_entry(const ICacheEntry& entry):
    return python_from_entry_p(&entry)

cdef inline bytes bytes_from_pickle(const SVCacheEntry_p entry):
    return entry.as_object()


cdef class CachedValue:
    """
    The base class for cached values.
    """

    cpdef get_if_tid_matches(self, object tid):
        raise NotImplementedError


cdef class SingleValue(CachedValue):
    cdef SVCacheEntry_p entry
    frozen = False

    @staticmethod
    cdef SingleValue from_entry(SVCacheEntry_p entry):
        cdef SingleValue sv = SingleValue.__new__(SingleValue)
        sv.entry = entry
        return sv

    def sizeof(self):
        # At this writing, reports 88
        return sizeof(SVCacheEntry)

    def __iter__(self):
        return iter((
            bytes_from_pickle(self.entry),
            self.entry.tid()
        ))

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
            return my_entry.state_eq(other_entry.state())

        if isinstance(other, tuple):
            return len(other) == 2 and self.tid == other[1] and self.value == other[0]
        return NotImplemented

    cpdef get_if_tid_matches(self, object tid):
        if self.entry.tid_matches(-1 if tid is None else tid):
            return self

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
@cython.internal
cdef class FrozenValue(SingleValue):

    frozen = True

    @staticmethod
    cdef SingleValue from_entry(const SVCacheEntry*& entry):
        cdef FrozenValue sv = FrozenValue.__new__(FrozenValue, 0, SingleValue, 0, 0)
        sv.entry = entry
        return sv


@cython.final
cdef class MultipleValues(CachedValue):
    cdef MVCacheEntry_p entry
# TODO: we should keep this sorted by tid, yes?
# A std::map<tid, SVCacheEntry_p> sounds almost ideal
# for accessing max_tid and newest_value, except for whatever space
# overhead that adds.

    @staticmethod
    cdef MultipleValues from_entry(MVCacheEntry_p entry):
        cdef MultipleValues mv = MultipleValues.__new__(MultipleValues)
        mv.entry = entry
        return mv

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
        return self.newest_value.tid

    @property
    def newest_value(self):
        cdef TID_t tid = -1
        cdef const SVCacheEntry* p = <SVCacheEntry_p>0
        it = self.entry.begin()
        end = self.entry.end()
        result = []
        while it != end:
            if deref(it).tid() > tid:
                tid = deref(it).tid()
                p = &deref(it)
            preincr(it)
        return python_from_entry_p(p)

    cpdef get_if_tid_matches(self, tid):
        cdef TID_t native_tid = -1 if tid is None else tid
        it = self.entry.begin()
        end = self.entry.end()
        while it != end:
            if deref(it).tid_matches(native_tid):
                return python_from_entry(deref(it))
            preincr(it)

    def __iter__(self):
        # Why the funny self.entry[0] even though we declare begin()
        # and end() methods? Well, because self.entry is a pointer
        # type (MVCacheEntry*), Cython gives an error that "arrays
        # must have a known length" to be iterated (recall we can't have a
        # reference because those can't be declared as a class attribute:
        # there's no way to construct that).
        # If we cast with cython.operator.dereference(),
        # we sometimes get a different error that only the [0] deref
        # syntax solves.
        it = self.entry.begin()
        end = self.entry.end()
        result = []
        while it != end:
            result.append(python_from_entry(deref(it)))
            preincr(it)
        return result

    def __repr__(self):
        return repr([
            tuple(v)
            for v in self
        ])

cdef class PyGeneration:
    cdef Generation* generation
    cdef readonly object __name__
    # This reference is to keep the cache object alive while we're
    # alive.
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
        return self.generation.len()

    # Cython still uses __nonzero__
    def __nonzero__(self):
        return not self.generation.empty()

    def __iter__(self):
        "Not thread safe."
        if self.generation.empty():
            return ()
        it = self.generation.begin()
        end = self.generation.end()
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
        return self.cache.len() > 0

    def __contains__(self, OID_t key):
        return self.cache.contains(key)

    def __len__(self):
        return self.cache.len()

    cpdef CachedValue get(self, OID_t key):
        entry = self.cache.get(key)
        if not entry:
            return None
        return python_from_entry(deref(entry))

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
        self._do_set(key, value[0], value[1])

    cdef _do_set(self, OID_t key, object state, TID_t tid):
        # Do all this down here so we don't give up the GIL.
        cdef bytes b_state = state if state is not None else b''
        if not self.cache.contains(key): # the long way to avoid type conversion
            self.cache.add_to_eden(
                key,
                b_state,
                tid
            )
        else:
            # We need to merge values.
            try:
                self.cache.store_and_make_MRU(key, b_state, tid)
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
        cdef SVCacheEntry* array
        cdef OID_t key
        cdef TID_t tid
        cdef int i
        cdef int number_nodes = len(ordered_keys)
        cdef int added_count
        if not number_nodes:
            return 0 if return_count_only else ()

        array = array_new[SVCacheEntry](number_nodes)

        for i, (key, (state, tid)) in enumerate(ordered_keys):
            state = state or b''
            array[i].force_update_during_bulk_load(key,
                                                   state, # TODO: copying
                                                   tid)

        # We're done with ordered_keys, free its memory
        ordered_keys = None

        added_count = self.cache.add_many(
            array,
            number_nodes)

        # Things that didn't get added have -1 for their generation.
        if return_count_only:
            return added_count

        i = 0
        result = []
        while i < number_nodes:
            if array[i].in_cache():
                key = array[i].key
                result.append(self.get(key))
            i += 1

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
# flycheck-cython-executable: "cython -ERS_COPY_STRING=0"
# End:
