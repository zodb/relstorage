# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True
"""
Python wrappers for C++ integer hash maps and hash sets, with
some added functionality.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
    T* array_new(int n) {
        return new T[n];
    }
    template <typename T>
    void array_delete(T* t) {
        delete[] t;
    }

    template <typename T>
    typename T::first_type get_key(T pair) {
        return pair.first;
    }

    template <typename K, typename V, typename I>
    class KeyInputIterator {
    private:
        I it;
    public:
        typedef K value_type;
        typedef std::input_iterator_tag iterator_category;
        typedef typename I::difference_type difference_type;
        typedef typename I::pointer pointer;
        typedef typename I::reference reference;
        KeyInputIterator(I it) : it(it) {};
        KeyInputIterator& operator++() { this->it++; return *this; }
        K operator*() { return (*it).first; }
        bool operator==(KeyInputIterator& other) {
           return this->it == other.it;
        }
        bool operator!=(KeyInputIterator& other) {
           return this->it != other.it;
        }
    };

    template <typename S, typename I>
    void bulk_set_insert(S& s, I begin, I end)
    {
        s.insert(begin, end);
    }

    template <typename M, typename I>
    void map_insert_bulk(M* map, const I begin, const I end) {
        map->insert(begin, end);
    }

    template <typename S, typename I>
    S new_set(const I begin, const I end) {
        return S(begin, end);
    }

    #define KEY_TYPE long long
    #define ZODB_64BIT_INTS
    """
    OID_t get_key(pair[OID_t, TID_t])
    cdef cppclass KeyInputIterator[K, V, I]:
        KeyInputIterator(I it)
    void bulk_set_insert[S, I](S&, I, I)
    void map_insert_bulk[M, I](M*, I, I)
    S new_set[S, I](I, I)

cdef extern from "_sorters.c":
    size_t sort_int_nodups(OID_t* array, size_t n)


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

# from libcpp.set cimport set as Set
# from libcpp.map cimport map as Map
# ctypedef Map[OID_t, TID_t] MapType
# ctypedef Set[OID_t] SetType

# ctypedef unordered_map[OID_t, TID_t] MapType
# ctypedef unordered_set[OID_t] SetType

# ctypedef vector[OID_t] VectorOidType
# ctypedef VectorOidType.iterator VectorOidIterator
# ctypedef vector[MapType*] VectorMapPtrType

@cython.final
cdef class OidSet:
    cdef SetType _set

    def __init__(self, oids=None):
        if oids:
            self.update(oids)

    cpdef keeping_only_keys_in_map(self, OidTidMap map):
        # most likely, the map is smaller than the set,
        # for this operation.
        cdef OidSet result = OidSet()
        cdef MapType.iterator it = map._map.begin()
        while it != map._map.end():
            pair = deref(it)
            if self._set.find(pair.first) != self._set.end():
                result._set.insert(pair.first)
            preincr(it)
        return result

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
        # I'd like to use copy() with inserter(self._set), but
        # I can't get that to work with Cython
        for k in other._set:
            self._set.insert(k)

    def __len__(self):
        return self._set.size()

    def __iter__(self):
        for k in self._set:
            yield k

@cython.final
cdef class OidTidMap:

    def __init__(self, data=()):
        self._map.clear()
        if data:
            self.update(data)

    def __len__(self):
        return self.size()

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
        else:
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
                result[pair.first] = pair.second
        return result

    def values(self):
        for pair in self._map:
            yield pair.second

    def items(self):
        for pair in self._map:
            yield pair

    def keys(self):
        for pair in self._map:
            yield pair.first

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

    @staticmethod
    def multiunion(maps):
        # 700 equal sets:
        # BTree multiunion: 1.4ms
        # Same algorithm (get all sorted, truncate excess): 4.11ms
        # Simple set inserting: 2.02ms (1.93ms without the final set conversion)
        # Same (simple insert, no final conversion) but with unordered_set:
        #   1.37ms
        # Same but with boost unordered_set: 1.12ms (but no return to python at all)
        # HOWEVER:
        # With 700 non-overlapping maps (arranged in increasing order)
        # BTree multiunion: 3.48ms
        # The simple algorithm with boost unordered_set: 24.5ms (yikes!)
        cdef VectorOidType all_oids
        cdef VectorOidType sorted_no_dups
        cdef OidTidMap a_map
        cdef OidSet result = OidSet()
        # Get them all in sorted order
        # Reserving space the size of the map in all_oids takes the simple copy loop from
        # 596us up to 305ms!
        # TODO: Surely there's a standard pair iterator that returns
        # only the first or second?
        for a_map in maps:
            #all_oids.reserve(all_oids.size() + a_map._map.size())
            transform(a_map._map.begin(), a_map._map.end(), back_inserter(all_oids), get_key)
        # Sorting takes 2.5ms
        how_many = sort_int_nodups(all_oids.data(), all_oids.size())
        sorted_no_dups.reserve(how_many)
        # But making the set, either way, takes 20ms!
        #for i in range(how_many):
        #    result._set.insert(all_oids[i])
        # bulk_set_insert(
        #     result._set,
        #     all_oids.begin(), all_oids.begin() + how_many)
        # Copying and returning a list takes 14ms, of which only 1ms or so of that
        # is the copy! Mostly its converting.
        copy_n(all_oids.begin(), how_many, back_inserter(sorted_no_dups))
        #result._set.reserve(how_many)
        #return result
        #return result # automatic C++ Set -> set conversion happens here. But not for boost set.
        # cdef OidTidMap a_map
        # cdef vector[OID_t] all_oids
        # cdef vector[OID_t] sorted_no_dups
        # # Get them all in sorted order
        # # TODO: Reserve extra space with reserve()
        # # TODO: Surely there's a standard pair iterator that returns
        # # only the first or second?
        # for a_map in maps:
        #     transform(a_map._map.begin(), a_map._map.end(), back_inserter(all_oids), get_key)

        # how_many = sort_int_nodups(all_oids.data(), all_oids.size())
        # copy_n(all_oids.begin(), how_many, back_inserter(sorted_no_dups))
        return sorted_no_dups


    @staticmethod
    def keys_in_both(maps, OidTidMap a_map):
        cdef OidSet result = OidSet()
        # XXX: Should we nogil this whole function body?

        # Using boost's unordered containers:
        # multiunion of maps into sorted order, and also sorting
        # ``a_map`` and then searching the sorted ``maps`` took 38s
        # for the big set of OIDs and 25s for the small set of OIDs.
        # Profiling a debug (-Og) version showed all the time was in the ``back_inserter``
        # in the ``transform`` call in ``multiunion_into``. Changing to a
        # manual loop had unclear results.
        #
        # Switching to a strategy of iterating the (presumably small) single map
        # and probing each individual map to see if it contains each key
        # takes a mear 5s for the small set of oids! BUT: It takes 41s
        # for the 172,558 unique OIDs. It's obvious why: In the small case, we
        # need only ever look at the first map.
        #
        # Using the stdlib sorted containers, the big case was 105s, while
        # the small case was 5.9s.
        cdef VectorOidType a_map_sorted
        cdef VectorOidType sorted_no_dups
        cdef VectorOidIterator search_fwd
        cdef VectorOidIterator search_end
        cdef OID_t candidate

        multiunion_into(maps, &sorted_no_dups)
        search_fwd = sorted_no_dups.begin()
        search_end = sorted_no_dups.end()
        # We know there are no duplicates in ``a_map``, so we just
        # need a sorted vector.
        a_map_sorted.reserve(a_map._map.size())
        transform(a_map._map.begin(), a_map._map.end(), back_inserter(a_map_sorted), get_key)
        sort(a_map_sorted.begin(), a_map_sorted.end())

        # Now, we can walk forward in sorted order, looking for each
        # integer. We either find it, in which case its in both, or we don't,
        # in which case we're done. This only walks through each list
        # one full time (because the list we're searching gets smaller and smaller)
        for candidate in a_map_sorted:
            search_fwd = find(search_fwd, search_end, candidate)
            if search_fwd == search_end:
                break
            result.c_add(candidate)

        # cdef OidTidMap t
        # cdef MapType* m
        # cdef VectorMapPtrType c_maps
        # for t in maps:
        #     c_maps.push_back(&t._map)

        # for pair in a_map._map:
        #     for m in c_maps:
        #         if m.find(pair.first) != m.end():
        #             result.c_add(pair.first)
        #             break

        return result



cdef size_t multiunion_into(maps, VectorOidType* result) except +:
    cdef VectorOidType all_oids
    cdef VectorOidType sorted_no_dups
    cdef OidTidMap a_map
    cdef size_t i
    cdef size_t how_many = 0
    # Get them all in sorted order
    # Reserving space the size of the map in all_oids takes the simple copy loop from
    # 596us up to 305ms!
    # TODO: Surely there's a standard pair iterator that returns
    # only the first or second?
    for a_map in maps:
        #all_oids.reserve(all_oids.size() + a_map._map.size())
        transform(a_map._map.begin(), a_map._map.end(), back_inserter(all_oids), get_key)
    # Sorting takes 2.5ms
    if all_oids.size():
        how_many = sort_int_nodups(all_oids.data(), all_oids.size())
    # But making the set, either way, takes 20ms!
    #for i in range(how_many):
    #    result._set.insert(all_oids[i])
    # bulk_set_insert(
    #     result._set,
    #     all_oids.begin(), all_oids.begin() + how_many)
    # Copying and returning a list takes 14ms, of which only 1ms or so of that
    # is the copy! Mostly its converting.
    if result is not NULL and how_many:
        result.reserve(how_many)
        for i in range(how_many):
            result.push_back(all_oids[i])
    return how_many


# Local Variables:
# flycheck-cython-cplus: t
# End:
