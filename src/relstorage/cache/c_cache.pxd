# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True

from libcpp.memory cimport shared_ptr
from libcpp.pair cimport pair
from libcpp.list cimport list
from libcpp cimport bool
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map



cdef extern from "c_cache.h":
    ctypedef signed long int64_t # Size doesn't actually matter.
    ctypedef unsigned long uint64_t
    ctypedef int64_t TID_t
    ctypedef int64_t OID_t

    ctypedef string Pickle_t

cdef extern from "c_cache.h" namespace "relstorage::cache":
    ctypedef size_t rs_counter_t
    ctypedef enum generation_num:
        GEN_UNKNOWN,
        GEN_EDEN,
        GEN_PROTECTED,
        GEN_PROBATION


    cdef cppclass AbstractEntry:
        AbstractEntry()
        AbstractEntry(OID_t key)
        OID_t key
        size_t weight() except +
        size_t value_count()
        size_t frequency
        bool in_cache()
    ctypedef shared_ptr[AbstractEntry] AbstractEntry_p

    cdef cppclass SingleValueEntry(AbstractEntry):
        # const Pickle_t& state() const
        TID_t tid() const
        bool frozen() const
        # Using -1 for None
        bool tid_matches(TID_t tid)
        void force_update_during_bulk_load(OID_t key, char*, size_t len, TID_t tid)
        bytes as_object()
        bytes first_two_as_object()
        SingleValueEntry* from_offset(size_t offset)
        size_t size()
        char* c_data()
        bint eq_for_python(const SingleValueEntry* other) const


    ctypedef shared_ptr[SingleValueEntry] SingleValueEntry_p

    cdef cppclass MultipleValueEntry(AbstractEntry):
        MultipleValueEntry(OID_t key)
        list[SingleValueEntry_p] p_values
        void push_back(const SingleValueEntry_p const) except +
        const SingleValueEntry_p front() except +
        bool empty()
        bool degenerate()

    ctypedef shared_ptr[MultipleValueEntry] MultipleValueEntry_p

    ctypedef unordered_map[OID_t, AbstractEntry_p] OidEntryMap
    ctypedef AbstractEntry* AbstractEntry_raw_p
    ctypedef list[AbstractEntry_raw_p] EntryList

    cdef cppclass Generation:
        size_t len() const
        size_t sum_weights() const
        const size_t max_weight
        const generation_num generation
        bool empty()
        EntryList iter()


    cdef cppclass Cache:
        Generation ring_eden
        Generation ring_protected
        Generation ring_probation
        Cache(uint64_t eden, uint64_t protected, uint64_t probation)
        void add_to_eden(OID_t key, char* buf, size_t len, TID_t tid) except +
        void store_and_make_MRU(OID_t, char* buf, size_t len, const TID_t) except +
        void delitem(OID_t key) except +
        void delitem(OID_t key, TID_t tid) except +
        void freeze(OID_t key, TID_t tid) except +
        bool contains(OID_t key)
        void age_frequencies()
        AbstractEntry_p get(OID_t key)
        size_t len()
        size_t weight()
        void on_hit(OID_t key)
        OidEntryMap& getData()
        int add_many(SingleValueEntry_p&, int count) except +
