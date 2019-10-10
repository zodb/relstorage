# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True


from libcpp.vector cimport vector
from libcpp cimport bool
from libcpp.string cimport string

cdef extern from *:
    ctypedef signed long int64_t
    ctypedef signed long uint64_t

cdef extern from * namespace "boost":
    T& move[T](T&)


cdef extern from "c_cache.h" namespace "relstorage::cache":
    ctypedef int64_t TID_t
    ctypedef int64_t OID_t

    IF RS_COPY_STRING:
        ctypedef string Pickle_t
    ELSE:
        ctypedef bytes Pickle_t

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


    cdef cppclass _SVCacheEntry(ICacheEntry):
        TID_t tid() const
        bint frozen() const
        # Using -1 for None
        bint tid_matches(TID_t tid)
        void force_update_during_bulk_load(OID_t key, Pickle_t, TID_t tid)
        bytes as_object()
        size_t size()
    IF RS_COPY_STRING:
        cdef cppclass SVCacheEntry(_SVCacheEntry):
            bint state_eq(const Pickle_t&) const
            const Pickle_t& state() const
    ELSE:
        cdef cppclass SVCacheEntry(_SVCacheEntry):
            bint state_eq(Pickle_t) const
            Pickle_t state() const

    cdef cppclass MVCacheEntry(ICacheEntry):
        cppclass iterator:
            bool operator==(iterator)
            bool operator!=(iterator)
            const SVCacheEntry& operator*()
            iterator& operator++()

        const SVCacheEntry& front() except +
        bint empty()
        bool degenerate()
        iterator begin()
        iterator end()

    cdef cppclass Generation:
        cppclass iterator:
            bool operator==(iterator)
            bool operator!=(iterator)
            const ICacheEntry& operator*()
            iterator& operator++()
        size_t len() const
        size_t sum_weights() const
        size_t max_weight()
        const generation_num generation
        bool empty()
        iterator begin()
        iterator end()


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
        Cache(size_t eden, size_t protected, size_t probation)
        void resize(size_t, size_t, size_t)
        size_t max_weight()
        void add_to_eden(OID_t key, Pickle_t, TID_t tid) except +
        void store_and_make_MRU(OID_t, Pickle_t, const TID_t) except +
        void delitem(OID_t key) except +
        void delitem(OID_t key, TID_t tid) except +
        void freeze(OID_t key, TID_t tid) except +
        bool contains(OID_t key)
        void age_frequencies()
        ICacheEntry* get(OID_t key)
        size_t len()
        size_t weight()
        void on_hit(OID_t key)
        int add_many(SVCacheEntry*, int count) except +

        iterator begin()
        iterator end()


# Local Variables:
# flycheck-cython-cplus: t
# flycheck-cython-executable: "cython -ERS_COPY_STRING=0"
# End:
