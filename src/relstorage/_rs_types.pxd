# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True


cdef extern from "_rs_types.h":
    ctypedef signed long int64_t
    ctypedef signed long uint64_t
    ctypedef int64_t TID_t
    ctypedef int64_t OID_t

cdef extern from "_rs_types.h" namespace "relstorage":
    cdef cppclass PythonAllocator[T]:
        pass

cdef extern from * namespace "boost":
    T& move[T](T&)
