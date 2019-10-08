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
from cpython.buffer cimport PyBuffer_FillInfo
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.bytes cimport PyBytes_AsString # Does NOT copy

from libcpp.pair cimport pair
from libcpp.memory cimport shared_ptr
from libcpp.memory cimport make_shared
from libcpp.memory cimport dynamic_pointer_cast
from libcpp.cast cimport static_cast
from libcpp.string cimport string

from relstorage.cache.c_cache cimport TID_t
from relstorage.cache.c_cache cimport OID_t
from relstorage.cache.c_cache cimport Pickle_t
from relstorage.cache.c_cache cimport AbstractEntry
from relstorage.cache.c_cache cimport SingleValueEntry
from relstorage.cache.c_cache cimport SingleValueEntry_p
from relstorage.cache.c_cache cimport AbstractEntry_p
from relstorage.cache.c_cache cimport MultipleValueEntry
from relstorage.cache.c_cache cimport MultipleValueEntry_p
from relstorage.cache.c_cache cimport Cache
from relstorage.cache.c_cache cimport Generation

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
    template <typename T>
    std::shared_ptr<T> shared_array_new(int n) {
        return std::shared_ptr<T>(new T[n], array_delete<T>);
    }
    """
    T* array_new[T](int)
    shared_ptr[T] shared_array_new[T](int)
    void array_delete[T](T* t)


cdef object python_from_entry(const AbstractEntry_p& entry):
    cdef SingleValueEntry_p sve_p
    cdef MultipleValueEntry_p mve_p

    cdef SingleValue sv
    cdef MultipleValues mv

    sve_p = dynamic_pointer_cast[SingleValueEntry, AbstractEntry](entry)
    if sve_p:
        if sve_p.get().frozen():
            sv = FrozenValue.from_entry(sve_p)
        else:
            sv = SingleValue.from_entry(sve_p)
        return sv

    mve_p = dynamic_pointer_cast[MultipleValueEntry, AbstractEntry](entry)
    if not mve_p:
        raise AssertionError("Invalid pointer cast",
                             entry.get().key)
    return MultipleValues.from_entry(mve_p)

cdef object python_from_sve(SingleValueEntry_p& entry):
    cdef AbstractEntry_p ae = dynamic_pointer_cast[AbstractEntry, SingleValueEntry](entry)
    return python_from_entry(ae)

cdef inline bytes bytes_from_pickle(const SingleValueEntry_p& entry):
    return entry.get().as_object()

# Memory management notes:
#
# Converting from Pickle_t to Python bytes creates a copy of the
# memory under Python control. We avoid that when we're read by
# using a buffer object, which can be read by cStringIO and io.BytesIO.
#
# Freelists only work on classes that do not inherit from
# anything except object. I think they also must be final.
# So we could use them between SingleValue and FrozenValue if we
# implemented the later with composition.

cdef bint PY2 = sys.version_info[0] == 2

@cython.final
@cython.internal
cdef class StringWrapper:
    cdef SingleValueEntry_p entry

    @staticmethod
    cdef from_entry(const SingleValueEntry_p& entry):
        if PY2:
            # Too many things on Python 2 don't handle the
            # new-style buffers that Cython creates. Notably, file.writelines()
            # and zlib.decompress() require old-style buffers
            return bytes_from_pickle(entry)
        cdef StringWrapper w = StringWrapper.__new__(StringWrapper)
        w.entry = entry
        return w

    cdef from_offset(self, offset):
        return StringWrapper.from_entry(
            SingleValueEntry_p(self.entry, # share ownership with this existing pointer.
                               self.entry.get().from_offset(offset)))

    def __getbuffer__(self, Py_buffer* view, int flags):
        PyBuffer_FillInfo(view, self,
                          <void*>self.entry.get().c_data(),
                          self.entry.get().size(),
                          1,
                          flags)

    def __releasebuffer__(self, Py_buffer* view):
        pass

    def __len__(self):
        return self.entry.get().size()

    def __getitem__(self, ix):
        if ix == slice(None, 2, None):
            # We need to be able to hash this, it's the compression prefix.
            return self.entry.get().first_two_as_object()
        if ix == slice(2, None, None) and self.entry.get().size() > 2:
            return self.from_offset(2)

        return self.entry.get().as_object()[ix]

    def __eq__(self, other):
        if isinstance(other, StringWrapper):
            return (<StringWrapper>other).entry == self.entry
        return bytes(self) == other

    def __str__(self):
        cdef bytes b = bytes_from_pickle(self.entry)
        return str(b)

    def __bytes__(self):
        cdef bytes b = bytes_from_pickle(self.entry)
        return b

    def __repr__(self):
        cdef bytes b = bytes_from_pickle(self.entry)
        return repr(b)

cdef class CachedValue:
    """
    The base class for cached values.
    """

    cpdef get_if_tid_matches(self, object tid):
        raise NotImplementedError


cdef class SingleValue(CachedValue):
    cdef SingleValueEntry_p entry
    frozen = False

    @staticmethod
    cdef SingleValue from_entry(const SingleValueEntry_p& entry):
        cdef SingleValue sv = SingleValue.__new__(SingleValue)
        sv.entry = entry
        return sv

    def sizeof(self):
        # At this writing, reports 88
        return sizeof(SingleValueEntry)

    def __iter__(self):
        value = self.entry.get()
        return iter((
            StringWrapper.from_entry(self.entry),
            value.tid()
        ))

    @property
    def value(self):
        return self.state

    @property
    def key(self):
        return self.entry.get().key

    @property
    def frequency(self):
        return self.entry.get().frequency

    @property
    def state(self):
        return StringWrapper.from_entry(self.entry)

    @property
    def tid(self):
        return self.entry.get().tid()

    @property
    def max_tid(self):
        return self.entry.get().tid()

    @property
    def newest_value(self):
        return self

    @property
    def weight(self):
        return self.entry.get().weight()

    def __eq__(self, other):
        cdef SingleValue p
        if isinstance(other, SingleValue):
            p = <SingleValue>other
            my_entry = self.entry.get()
            other_entry = p.entry.get()
            return (
                self.entry.get().eq_for_python(other_entry)
            )
        if isinstance(other, tuple):
            return len(other) == 2 and self.tid == other[1] and self.value == other[0]
        return NotImplemented

    cpdef get_if_tid_matches(self, object tid):
        if self.entry.get().tid_matches(-1 if tid is None else tid):
            return self

    def __getitem__(self, int i):
        if i == 0:
            return StringWrapper.from_entry(self.entry)
        if i == 1:
            return self.entry.get().tid()
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
    cdef SingleValue from_entry(const SingleValueEntry_p& entry):
        cdef FrozenValue sv = FrozenValue.__new__(FrozenValue, 0, SingleValue, 0, 0)
        sv.entry = entry
        return sv

@cython.final
cdef class MultipleValues(CachedValue):
    cdef MultipleValueEntry_p entry
# TODO: we should keep this sorted by tid, yes?
# A std::map<tid, SingleValueEntry_p> sounds almost ideal
# for accessing max_tid and newest_value, except for whatever space
# overhead that adds.

    @staticmethod
    cdef MultipleValues from_entry(const MultipleValueEntry_p& entry):
        cdef MultipleValues mv = MultipleValues.__new__(MultipleValues)
        mv.entry = entry
        return mv

    def sizeof(self):
        # At this writing, reports 72.
        return sizeof(MultipleValueEntry)

    @property
    def value(self):
        return list(self)

    @property
    def key(self):
        return self.entry.get().key

    @property
    def frequency(self):
        return self.entry.get().frequency

    @property
    def weight(self):
        return self.entry.get().weight()

    @property
    def max_tid(self):
        cdef TID_t result = 0
        values = self.entry.get().p_values
        for p in values:
            if p.get().tid() > result:
                result = p.get().tid()
        return result

    @property
    def newest_value(self):
        cdef SingleValueEntry_p entry = self.entry.get().front()
        values = self.entry.get().p_values
        for p in values:
            if p.get().tid() > entry.get().tid():
                entry = p
        return python_from_sve(entry)

    cpdef get_if_tid_matches(self, tid):
        cdef TID_t native_tid = -1 if tid is None else tid
        values = self.entry.get().p_values
        for entry in values:
            if entry.get().tid_matches(native_tid):
                return python_from_sve(entry)
        return None

    def __iter__(self):
        return iter([
            python_from_sve(v)
            for v
            in self.entry.get().p_values
        ])

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
        return self.generation.max_weight

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
        it = self.generation.iter()

        for ptr in it:
            yield self._cache.get(ptr.key)


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
        if not self.cache.contains(key):
            return None
        return python_from_entry(self.cache.get(key))

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
        cdef TID_t tid = value[1]
        state = value[0] or b''
        if not self.cache.contains(key): # the long way to avoid type conversion
            self.cache.add_to_eden(
                key,
                PyBytes_AsString(state),
                len(state),
                tid
            )
        else:
            # We need to merge values.
            try:
                self.cache.store_and_make_MRU(key, PyBytes_AsString(state), len(state), tid)
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
        for pair in self.cache.getData():
            # XXX: we shouldn't have to go to python to get this.
            python = python_from_entry(pair.second)
            oid = pair.first
            if pair.second.get().value_count() > 1:
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
            yield (thing.first, python_from_entry(thing.second))

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
            yield python_from_entry(thing.second)

    # Cache specific operations

    def add_MRUs(self, ordered_keys, bint return_count_only=False):
        cdef SingleValueEntry* array
        cdef SingleValueEntry* single_entry
        cdef SingleValueEntry_p shared_ptr_to_array
        cdef SingleValueEntry_p ptr_to_entry
        cdef OID_t key
        cdef TID_t tid
        cdef int i
        cdef int number_nodes = len(ordered_keys)
        cdef int added_count
        if not number_nodes:
            return 0 if return_count_only else ()

        shared_ptr_to_array = shared_array_new[SingleValueEntry](number_nodes)
        array = shared_ptr_to_array.get()

        for i, (key, (state, tid)) in enumerate(ordered_keys):
            state = state or b''
            array[i].force_update_during_bulk_load(key,
                                                   PyBytes_AsString(state), len(state),
                                                   tid)

        # We're done with ordered_keys, free its memory
        ordered_keys = None

        added_count = self.cache.add_many(
            shared_ptr_to_array,
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

        return result if not return_count_only else added_count

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
        cdef CachedValue value

        for oid, tid in iteroiditems(oids_tids):
            self.cache.freeze(oid, tid)

    @property
    def weight(self):
        return self.cache.weight()
