# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True

from libcpp.vector cimport vector
from libcpp cimport bool


cdef extern from *:
    ctypedef signed long int64_t
    ctypedef signed long uint64_t

cdef extern from * namespace "boost":
    T& move[T](T&)


cdef extern from "c_cache.h" namespace "relstorage::cache":
    ctypedef int64_t TID_t
    ctypedef int64_t OID_t

    ctypedef enum generation_num:
        GEN_UNKNOWN,
        GEN_EDEN,
        GEN_PROTECTED,
        GEN_PROBATION


    cdef cppclass ICacheEntry:
        OID_t key
        size_t weight() except +
        size_t frequency
        bint in_cache() except +
        vector[TID_t] all_tids() except +
        # Memory management
        bool can_delete() except +
        const T* Py_use[T]()
        bool Py_release() except + # Throwing during dev because of assertions


    cdef cppclass SVCacheEntry(ICacheEntry):
        TID_t tid() const
        bint frozen() const
        # Using -1 for None
        object as_object()
        size_t size()
        bint operator==(SVCacheEntry&)


    cdef cppclass MVCacheEntry(ICacheEntry):
        SVCacheEntry* copy_newest_entry() except +
        TID_t newest_tid()


    cdef cppclass TempCacheFiller:
        TempCacheFiller()
        TempCacheFiller(size_t number_nodes) except +
        void set_once_at(size_t, OID_t, TID_t, object, bool frozen, int frequency)


    cdef cppclass Generation:
        cppclass iterator:
            bool operator==(iterator)
            bool operator!=(iterator)
            const ICacheEntry& operator*()
            iterator& operator++()
        size_t size() const
        size_t sum_weights() const
        size_t max_weight()
        const generation_num generation
        bool empty()
        iterator begin()
        iterator end()

    cdef cppclass ProposedCacheEntry:
        ProposedCacheEntry()
        ProposedCacheEntry(OID_t, TID_t, object)


    cdef cppclass Cache:
        cppclass iterator:
            bool operator==(iterator)
            bool operator!=(iterator)
            ICacheEntry& operator*()
            iterator& operator++()
            # Sadly this is not supported:
            # ICacheEntry& operator->()

        Generation ring_eden
        Generation ring_protected
        Generation ring_probation
        Cache()
        Cache(size_t eden, size_t protected, size_t probation) except +
        void __del__() except +
        void resize(size_t, size_t, size_t)
        size_t max_weight()
        void add_to_eden(ProposedCacheEntry) except +
        void store_and_make_MRU(ProposedCacheEntry) except +
        void delitem(OID_t key) except +
        void delitem(OID_t key, TID_t tid) except +
        void freeze(OID_t key, TID_t tid) except +
        bool contains(OID_t key)
        void age_frequencies()
        ICacheEntry* get(OID_t key)
        SVCacheEntry* get(OID_t, TID_t)
        SVCacheEntry* peek(OID_t, TID_t)
        size_t size()
        size_t weight()
        vector[OID_t] add_many(TempCacheFiller&) except +

        iterator begin()
        iterator end()


# Local Variables:
# flycheck-cython-cplus: t
# End:
