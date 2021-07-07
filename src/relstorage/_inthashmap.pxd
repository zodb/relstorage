# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True,language_level=3str

cimport cython

from libcpp.pair cimport pair
from libcpp.vector cimport vector

from relstorage._rs_types cimport OID_t
from relstorage._rs_types cimport TID_t
from relstorage._rs_types cimport PythonAllocator


# XXX: The unordered_set/unordered_map definitions are just copied
# from Cython. They may not be correct or complete.
cdef extern from "_inthashmap.h" namespace "boost" nogil:
    cdef cppclass unordered_set[T,HASH=*,PRED=*,ALLOCATOR=*]:
        ctypedef T value_type
        ctypedef size_t size_type
        cppclass iterator:
            T& operator*()
            iterator operator++()
            iterator operator--()
            bint operator==(iterator)
            bint operator!=(iterator)
        cppclass reverse_iterator:
            T& operator*()
            iterator operator++()
            iterator operator--()
            bint operator==(reverse_iterator)
            bint operator!=(reverse_iterator)
        cppclass const_iterator(iterator):
            pass
        cppclass const_reverse_iterator(reverse_iterator):
            pass
        unordered_set() except +
        unordered_set(unordered_set&) except +
        #unordered_set(key_compare&)
        #unordered_set& operator=(unordered_set&)
        bint operator==(unordered_set&, unordered_set&)
        bint operator!=(unordered_set&, unordered_set&)
        bint operator<(unordered_set&, unordered_set&)
        bint operator>(unordered_set&, unordered_set&)
        bint operator<=(unordered_set&, unordered_set&)
        bint operator>=(unordered_set&, unordered_set&)
        iterator begin()
        const_iterator const_begin "begin"()
        void clear()
        size_t count(T&)
        bint empty()
        iterator end()
        const_iterator const_end "end"()
        pair[iterator, iterator] equal_range(T&)
        pair[const_iterator, const_iterator] const_equal_range "equal_range"(T&)
        iterator erase(iterator)
        iterator erase(iterator, iterator)
        size_t erase(T&)
        iterator find(T&)
        const_iterator const_find "find"(T&)
        pair[iterator, bint] insert(T&)
        iterator insert(iterator, T&)
        #key_compare key_comp()
        iterator insert(iterator, iterator)
        iterator lower_bound(T&)
        const_iterator const_lower_bound "lower_bound"(T&)
        size_t max_size()
        reverse_iterator rbegin()
        const_reverse_iterator const_rbegin "rbegin"()
        reverse_iterator rend()
        const_reverse_iterator const_rend "rend"()
        size_t size()
        void swap(unordered_set&)
        iterator upper_bound(T&)
        const_iterator const_upper_bound "upper_bound"(T&)
        #value_compare value_comp()
        void max_load_factor(float)
        float max_load_factor()
        void rehash(size_t)
        void reserve(size_t)
        size_t bucket_count()
        size_t max_bucket_count()
        size_t bucket_size(size_t)
        size_t bucket(const T&)

cdef extern from "_inthashmap.h" namespace "boost" nogil:
    cdef cppclass unordered_map[T, U, HASH=*, PRED=*, ALLOCATOR=*]:
        ctypedef T key_type
        ctypedef U mapped_type
        ctypedef pair[const T, U] value_type
        ctypedef size_t size_type
        cppclass iterator:
            pair[T, U]& operator*()
            iterator operator++()
            iterator operator--()
            bint operator==(iterator)
            bint operator!=(iterator)
        cppclass reverse_iterator:
            pair[T, U]& operator*()
            iterator operator++()
            iterator operator--()
            bint operator==(reverse_iterator)
            bint operator!=(reverse_iterator)
        cppclass const_iterator(iterator):
            pass
        cppclass const_reverse_iterator(reverse_iterator):
            pass
        unordered_map() except +
        unordered_map(unordered_map&) except +
        #unordered_map(key_compare&)
        U& operator[](T&)
        #unordered_map& operator=(unordered_map&)
        bint operator==(unordered_map&, unordered_map&)
        bint operator!=(unordered_map&, unordered_map&)
        bint operator<(unordered_map&, unordered_map&)
        bint operator>(unordered_map&, unordered_map&)
        bint operator<=(unordered_map&, unordered_map&)
        bint operator>=(unordered_map&, unordered_map&)
        U& at(const T&)
        const U& const_at "at"(const T&)
        iterator begin()
        const_iterator const_begin "begin"()
        void clear()
        size_t count(T&)
        bint empty()
        iterator end()
        const_iterator const_end "end"()
        pair[iterator, iterator] equal_range(T&)
        pair[const_iterator, const_iterator] const_equal_range "equal_range"(const T&)
        iterator erase(iterator)
        iterator erase(iterator, iterator)
        size_t erase(T&)
        iterator find(T&)
        const_iterator const_find "find"(T&)
        pair[iterator, bint] insert(pair[T, U]) # XXX pair[T,U]&
        iterator insert(iterator, pair[T, U]) # XXX pair[T,U]&
        iterator insert(iterator, iterator)
        #key_compare key_comp()
        iterator lower_bound(T&)
        const_iterator const_lower_bound "lower_bound"(T&)
        size_t max_size()
        reverse_iterator rbegin()
        const_reverse_iterator const_rbegin "rbegin"()
        reverse_iterator rend()
        const_reverse_iterator const_rend "rend"()
        size_t size()
        void swap(unordered_map&)
        iterator upper_bound(T&)
        const_iterator const_upper_bound "upper_bound"(T&)
        #value_compare value_comp()
        void max_load_factor(float)
        float max_load_factor()
        void rehash(size_t)
        void reserve(size_t)
        size_t bucket_count()
        size_t max_bucket_count()
        size_t bucket_size(size_t)
        size_t bucket(const T&)

# The allocator is the last argument in the template, so we have to
# repeat a lot of the defaults from the headers to use it.
cdef extern from *:
    """
    typedef boost::hash<OID_t> Hasher;
    typedef std::equal_to<OID_t> Comparer;
    typedef relstorage::PythonAllocator<std::pair<OID_t, TID_t> > MapAlloc;
    typedef relstorage::PythonAllocator<OID_t> SetAlloc;

    typedef boost::unordered_map<
        OID_t, TID_t,
        Hasher,
        Comparer,
        MapAlloc > MapType;

    typedef boost::unordered_set<
        OID_t,
        Hasher,
        Comparer,
        SetAlloc > SetType;

    typedef MapType* MapTypePtr;
    typedef relstorage::PythonAllocator<MapTypePtr> MapTypePtrAlloc;

    typedef std::vector<MapTypePtr, MapTypePtrAlloc > VectorMapPtrType;
    """
    cppclass hash[T]:
        pass
    ctypedef hash[OID_t] Hasher
    cppclass equal_to[T]:
        pass
    ctypedef equal_to[OID_t] Comparer
    ctypedef PythonAllocator[pair[OID_t, TID_t]] MapAlloc
    ctypedef PythonAllocator[OID_t] SetAlloc

    ctypedef unordered_map[OID_t, TID_t, Hasher, Comparer, MapAlloc] MapType
    ctypedef unordered_set[OID_t, Hasher, Comparer, SetAlloc] SetType

    ctypedef MapType* MapTypePtr
    ctypedef PythonAllocator[MapTypePtr] MapTypePtrAlloc
    ctypedef vector[MapTypePtr, MapTypePtrAlloc] VectorMapPtrType


ctypedef MapType.size_type MapSizeType

ctypedef vector[OID_t, PythonAllocator[OID_t]] VectorOidType
ctypedef VectorOidType.iterator VectorOidIterator


@cython.final
@cython.freelist(1000)
cdef class OidTidMap:
    cdef MapType _map

    cpdef update(self, data)
    cpdef OidTidMap difference(self, OidTidMap other)
    cdef MapSizeType size(self)
    cpdef TID_t minValue(self) except -1
    cpdef TID_t maxValue(self) except -1
    cdef bint contains(self, OID_t key) except -1
    cdef int set(self, OID_t key, TID_t value) except -1
    cdef void update_from_other_map(self, OidTidMap other) except +

cdef VectorOidType multiunion(list maps, size_t total_size) except +
