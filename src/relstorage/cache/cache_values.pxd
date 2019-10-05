# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True
"""
Python wrappers for the values stored in the cache.

These objects accept shared pointers to the data stored in the cache,
which is in control of their lifetime.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

cimport cython
from cython.operator cimport dereference as deref
from cython.operator import postincrement as postinc

from libcpp.memory cimport shared_ptr

from relstorage.cache.c_cache cimport Pickle_t
from relstorage.cache.c_cache cimport TID_t
from relstorage.cache.c_cache cimport AbstractEntry_p
from relstorage.cache.c_cache cimport SingleValueEntry_p
from relstorage.cache.c_cache cimport MultipleValueEntry_p

cdef object value_from_entry(const AbstractEntry_p& entry)
cdef AbstractEntry_p entry_from_python(object value) except *


cdef inline Pickle_t pickle_from_python(state):
    # Cython sadly won't let us use const return value :(
    if state is None:
        return b''
    return state



cdef class CachedValue:
    cpdef get_if_tid_matches(self, object tid)

    cpdef freeze_to_tid(self, TID_t tid)

    cpdef with_later(self, tuple value)

    cpdef discarding_tids_before(self, TID_t tid)
