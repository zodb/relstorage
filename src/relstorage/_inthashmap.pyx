# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True,language_level=3str
"""
Python wrappers for C++ integer hash maps and hash sets, with
some added functionality.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from cython.operator import dereference as deref

from libcpp.algorithm cimport copy
from libcpp.algorithm cimport unique
from libcpp.algorithm cimport sort

from relstorage._rs_types cimport OID_t
from relstorage._rs_types cimport TID_t

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
    static typename T::first_type get_key(T pair) {
        return pair.first;
    }

    template <typename S, typename I>
    static void set_insert_bulk(S& s, I begin, I end)
    {
        s.insert(begin, end);
    }
    """
    OID_t get_key(pair[OID_t, TID_t])
    void set_insert_bulk[S, I](S&, I, I)


cimport cython
from cython.operator cimport dereference as deref
from cython.operator cimport preincrement as preincr

from libcpp.vector cimport vector
from libcpp.iterator cimport back_inserter
from libcpp.iterator cimport inserter
from libcpp.algorithm cimport transform
from libcpp.algorithm cimport copy_n
from libcpp.algorithm cimport copy
from libcpp.algorithm cimport find
from libcpp.algorithm cimport sort


@cython.final
cdef class OidSet:
    cdef SetType _set

    def __init__(self, oids=None):
        if oids is not None:
            self.update(oids)

    def add(self, OID_t oid):
        self._set.insert(oid)

    cdef void c_add(self, OID_t oid) except +:
        self._set.insert(oid)

    cpdef update(self, data):
        if isinstance(data, OidSet):
            self.update_from_other_set(<OidSet>data)
        else:
            for k in data:
                self._set.insert(k)

    cdef void update_from_other_set(self, OidSet other) except +:
        # Unlike map.insert(), which only adds missing values and doesn't
        # update, well...this actually only adds missing values too, but
        # that's exactly what we want. It's a union operation.
        self._set.reserve(self._set.size() + other._set.size())
        set_insert_bulk(self._set, other._set.begin(), other._set.end())

    cdef void update_from_vector(self, VectorOidType& data) except +:
        self._set.reserve(self._set.size() + data.size())
        set_insert_bulk(self._set, data.begin(), data.end())

    def __len__(self):
        return self._set.size()

    def __iter__(self):
        for k in self._set:
            yield k

    def __eq__(self, other):
        if not isinstance(other, OidSet):
            return NotImplemented
        return self._set == (<OidSet>other)._set

@cython.freelist(1000)
@cython.final
cdef class _OidTidMapKeysView:
    cdef OidTidMap _parent

    def __cinit__(self, OidTidMap parent):
        self._parent = parent

    def __iter__(self):
        for pair in self._parent._map:
            yield pair.first

@cython.freelist(1000)
@cython.final
cdef class _OidTidMapValuesView:
    cdef OidTidMap _parent

    def __cinit__(self, OidTidMap parent):
        self._parent = parent

    def __iter__(self):
        for pair in self._parent._map:
            yield pair.second

@cython.freelist(1000)
@cython.final
cdef class _OidTidMapItemsView:
    cdef OidTidMap _parent

    def __cinit__(self, OidTidMap parent):
        self._parent = parent

    def __iter__(self):
        for pair in self._parent._map:
            yield pair


@cython.final
cdef class OidTidMap:

    def __init__(self, data=()):
        self._map.clear()
        if data:
            self.update(data)

    def __len__(self):
        return self.size()

    def __eq__(self, other):
        if not isinstance(other, OidTidMap):
            return NotImplemented
        return self._map == (<OidTidMap>other)._map

    cdef MapSizeType size(self):
        return self._map.size()

    def __setitem__(self, k, v):
        self.set(k, v)

    cdef int set(self, OID_t key, TID_t value) except -1:
        if key < 0 or value < 0:
            raise TypeError((key, value))
        self._map[key] = value
        return 1

    def __getitem__(self, OID_t key):
        search = self._map.find(key)
        if search != self._map.end():
            return deref(search).second
        raise KeyError(key)

    def get(self, key, default=None):
        search = self._map.find(key)
        if search != self._map.end():
            return deref(search).second
        return default

    def __delitem__(self, OID_t key):
        search = self._map.find(key)
        if search == self._map.end():
            raise KeyError(key)
        self._map.erase(search)

    def __contains__(self, OID_t key):
        return self.contains(key)

    cdef bint contains(self, OID_t key) except -1:
        search = self._map.find(key)
        return search != self._map.end()

    cpdef update(self, data):
        cdef OID_t k
        cdef TID_t v
        if isinstance(data, OidTidMap):
            self.update_from_other_map(<OidTidMap>data)
            return

        if hasattr(data, 'items'):
            # Support dicts or sequences
            data = data.items()

        for k, v in data:
            if k < 0 or v < 0:
                raise TypeError((k, v))
            self._map[k] = v

    cdef void update_from_other_map(self, OidTidMap other) except +:
        # The insert(InputIt, InputIt) member isn't available
        # because of...overriding reasons? We can work around that, but,
        # more seriously, the variants on insert only put in *missing* keys.
        # We want update to *replace* keys. merge has the same problem,
        # plus some of its own. Perhaps one of the copy() algorithms?
        for pair in other._map:
            self._map[pair.first] = pair.second

    cpdef OidTidMap difference(self, OidTidMap other):
        """
        Return a new OidTidMap containing the keys from *self* for which
        there is no corresponding key in *other*.
        """
        cdef OidTidMap result = OidTidMap()
        for pair in self._map:
            search = other._map.find(pair.first)
            if search == other._map.end():
                result.set(pair.first, pair.second)
        return result

    def values(self):
        return _OidTidMapValuesView.__new__(_OidTidMapValuesView, self)

    def items(self):
        return _OidTidMapItemsView.__new__(_OidTidMapItemsView, self)

    def keys(self):
        return _OidTidMapKeysView.__new__(_OidTidMapKeysView, self)

    def __iter__(self):
        for pair in self._map:
            yield pair.first

    cpdef TID_t minValue(self) except -1:
        cdef TID_t result
        if not self._map.size():
            raise ValueError("Empty")
        result = deref(self._map.begin()).second
        for pair in self._map:
            if pair.second < result:
                result = pair.second
        return result

    cpdef TID_t maxValue(self) except -1:
        cdef TID_t result
        if not self._map.size():
            raise ValueError("Empty")
        result = deref(self._map.begin()).second
        for pair in self._map:
            if pair.second > result:
                result = pair.second
        return result

    # This class briefly provided ``multiunion`` and
    # ``keys_in_both`` methods on the way to developing
    # the new cache MVCC vacuum approach in _objectindex.
    # They are not used and were removed; however,
    # a thin wrapper for the ``multiunion()`` function is provided for
    # testing directly from Python.
    # See earlier versions of this file for notes on timing and approaches.

    @staticmethod
    def _multiunion(maps):
        """
        Given a Python iterable in *maps* of `OidTidMap` objects,
        compute the union of all the keys (OIDs) found in the maps.

        Returns a sorted list of unique keys.
        """
        # In tests of 2,000 maps with 250 distinct keys,
        # it seems that most of the time is spent converting
        # from C++ to Python. However, returning the vector and letting
        # Cython handle the conversion is faster than creating
        # a OidSet from the vector (25ms vs 73ms).
        # However, since we added the PythonAllocator to the template,
        # we can't use the default conversion; Cython doesn't get the
        # template arguments right. There seems to be very little difference,
        # though, with the list comprehension.
        return [
            x for x in multiunion(list(maps), 0)
        ]




cdef VectorOidType multiunion(list maps, size_t total_size) except +:
    """
    Given a Python iterable in *maps* of `OidTidMap` objects,
    compute the union of all the keys (OIDs) found in the maps.

    Returns a vector of all the unique keys.

    If *total_size* is not 0, it should be enough space to hold
    all the keys of each map, e.g., ``sum(len(x) for x in maps)``;
    but only pass this if you can do so cheaply (without type checks or
    calling into Python).
    """
    cdef VectorOidType all_oids
    cdef OidTidMap a_map

    # This algorithm is based on the BTrees ``multiunion`` algorithm.
    # Get a list of *all* the keys in arbitrary order, then sort them
    # in place, ignoring duplicates; then toss away the duplicates
    # without compacting the container (because it's likely
    # temporary). It seems that the C++ library does a better job of
    # this than the BTrees sorters do.

    # If we reserve space the size of the map in all_oids as we
    # iterate and transform, we radically slow down this process, by
    # orders of magnitude. Possibly because we wind up making many
    # fewer allocations rather than letting the vector grow by bigger
    # chunks?

    # But we can do better if we reserve the *total* space ahead of time,
    # which we'll do if the caller can cheaply get it for us.
    if total_size:
        all_oids.reserve(total_size + 1)

    for a_map in maps:
        transform(a_map._map.begin(), a_map._map.end(), back_inserter(all_oids), get_key)

    if all_oids.size():
        sort(all_oids.begin(), all_oids.end())
        new_end = unique(all_oids.begin(), all_oids.end())
        all_oids.erase(new_end, all_oids.end())
    return all_oids


# Local Variables:
# flycheck-cython-cplus: t
# End:
