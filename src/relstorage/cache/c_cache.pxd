# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp.list cimport list
from libcpp cimport bool
from libcpp.unordered_map cimport unordered_map


cdef extern from "c_cache.h":
    ctypedef signed long int64_t # Size doesn't actually matter.
    ctypedef unsigned long uint64_t
    ctypedef int64_t TID_t
    ctypedef int64_t OID_t
    ctypedef string Pickle_t

cdef extern from "c_ring.h":
    ctypedef size_t rs_counter_t
    ctypedef enum generation_num:
        GEN_UNKNOWN,
        GEN_EDEN,
        GEN_PROTECTED,
        GEN_PROBATION

    cdef cppclass RSRingNode:
        RSRingNode* r_prev
        RSRingNode* r_next
        generation_num generation

    cdef cppclass RSCache(RSRingNode):
        pass

    int rsc_eden_add_many[T](RSCache& cache, T* entry_array, int entry_count)


cdef extern from "c_cache.h" namespace "relstorage::cache":

    cdef cppclass AbstractEntry(RSRingNode):
        AbstractEntry(OID_t key)
        OID_t key
        size_t weight() except +
        size_t len() except +
        size_t frequency
    ctypedef shared_ptr[AbstractEntry] AbstractEntry_p

    cdef cppclass SingleValueEntry(AbstractEntry):
        Pickle_t state
        TID_t tid
        bool frozen
        SingleValueEntry(OID_t key, Pickle_t state, TID_t tid)
        SingleValueEntry(OID_t key, pair[Pickle_t, TID_t], bool frozen)
        SingleValueEntry(OID_t key, Pickle_t state, TID_t tid, bool frozen)

    ctypedef shared_ptr[SingleValueEntry] SingleValueEntry_p

    cdef cppclass MultipleValueEntry(AbstractEntry):
        MultipleValueEntry(OID_t key)
        list[SingleValueEntry_p] p_values;
        void push_back(SingleValueEntry_p) except +
        void remove_tids_lte(TID_t tid) except +
        void remove_tids_lt(TID_t tid) except +


    ctypedef shared_ptr[MultipleValueEntry] MultipleValueEntry_p


    cdef cppclass Generation(RSRingNode):
        size_t max_weight
        size_t sum_weights
        size_t len
        int generation
        bool ring_is_empty()

    cdef cppclass Cache:
        Generation* ring_eden
        Generation* ring_protected
        Generation* ring_probation
        Cache(uint64_t eden, uint64_t protected, uint64_t probation)
        void add_to_eden(SingleValueEntry_p sve_p) except +
        void update_MRU(AbstractEntry_p entry) except +
        void replace_entry(AbstractEntry_p new_entry,
                           AbstractEntry_p prev_entry,
                           size_t prev_weight) except +
        void delitem(OID_t key) except +
        bool contains(OID_t key)
        void age_frequencies()
        shared_ptr[AbstractEntry] get(OID_t key)
        size_t len()
        size_t weight()
        void on_hit(OID_t key)
        unordered_map[OID_t, AbstractEntry_p]& getData()
