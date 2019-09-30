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

from relstorage.cache.lru_cache cimport TID_t
from relstorage.cache.lru_cache cimport OID_t
from relstorage.cache.lru_cache cimport SingleValueEntry
from relstorage.cache.lru_cache cimport SingleValueEntry_p
from relstorage.cache.lru_cache cimport MultipleValueEntry

from relstorage.cache.interfaces import CacheConsistencyError

cdef SingleValue value_from_entry(SingleValueEntry_p entry):
    cdef SingleValue sv
    cdef FrozenValue fv
    if entry.get().frozen:
        fv = FrozenValue.__new__(FrozenValue, -1, SingleValue, 0, True)
        sv = fv
    else:
        sv = SingleValue.__new__(SingleValue, -1, SingleValue, 0, False)
    sv.entry = entry
    return sv

# Memory management notes:
#
# Converting from Pickle_t to Python bytes creates a copy of the
# memory under Python control. Ideally we could avoid that while still
# keeping lifetimes correct through our shared pointers using
# a...memoryview?

# Freelists only work on classes that do not inherit from
# anything except object. I think they also must be final.
# So we could use them between SingleValue and FrozenValue if we
# implemented the later with composition.

cdef class SingleValue:
    frozen = False

    def __cinit__(self, OID_t oid, object state, TID_t tid, bint frozen):
        cdef SingleValueEntry* entry
        if state is SingleValue:
            # Marker passed in from value_from_entry
            # not to do anything, we're shared.
            return
        if state is None:
            state = b''

        # implicit cast and copy state from bytes to std::string.
        entry = new SingleValueEntry(oid, state, tid, frozen)
        self.entry.reset(entry)

    def __iter__(self):
        value = self.entry.get()
        return iter((
            value.state,
            value.tid
        ))

    @property
    def max_tid(self):
        return self.entry.get().tid

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
                my_entry.state == other_entry.state
                and my_entry.tid == other_entry.tid
                and self.frozen == other.frozen
            )
        return NotImplemented

    def __mod__(self, tid):
        cdef SingleValue me
        cdef TID_t native_tid
        if tid is None:
            return None
        me = (<SingleValue>self)
        native_tid = <TID_t>tid
        if native_tid == me.entry.get().tid:
            return me

    def __ilshift__(self, TID_t tid):
        # We could be newer
        cdef FrozenValue fv
        entry = self.entry.get()
        if entry.tid > tid:
            return self
        if tid == entry.tid:
            fv = FrozenValue.__new__(FrozenValue, b'', 0)
            fv.entry = self.entry
            return fv
        # if we're older, fall off the end and discard.

    def __iadd__(self, SingleValue value):
        if (self.entry.get().state == value.entry.get().state
            and self.entry.get().tid == value.entry.get().tid):
            return value # Let us become frozen if desired.

        if (value.entry.get().tid == self.entry.get().tid and
            value.entry.get().state != self.entry.get().state):
            raise CacheConsistencyError(
                "Detected two different values for same TID",
                self,
                value
            )

        return _MultipleValues.__new__(_MultipleValues, self, value)

    def __isub__(self, TID_t tid):
        if tid <= self.entry.get().tid:
            return None
        return self

    def __getitem__(self, int i):
        if i == 0:
            return self.entry.get().state
        if i == 1:
            return self.entry.get().tid
        raise IndexError

    def __repr__(self):
        return repr(tuple(self))

@cython.final
cdef class FrozenValue(SingleValue):

    frozen = True

    def __mod__(self, tid):
        cdef SingleValue me
        cdef TID_t native_tid
        if tid is None:
            return self
        me = (<SingleValue>self)
        native_tid = <TID_t>tid
        if native_tid == me.entry.get().tid:
            return me

    def __ilshift__(self, TID_t tid):
        # This method can get called if two different transaction views
        # tried to load an object at the same time and store it in the cache.
        if tid == self.entry.get().tid:
            return self

cdef class _MultipleValues:
# TODO: we should keep this sorted by tid, yes?
# A std::map<tid, SingleValueEntry_p> sounds almost ideal
# for accessing max_tid and newest_value, except for whatever space
# overhead that adds.
    cdef shared_ptr[MultipleValueEntry] entry

    def __cinit__(self, SingleValue mv1, SingleValue mv2):
        entry = new MultipleValueEntry(mv1.entry.get().key)
        self.entry.reset(entry)
        entry.push_back(mv1.entry)
        entry.push_back(mv2.entry)

    @property
    def weight(self):
        cdef int result = 0
        values = self.entry.get().p_values
        for p in values:
            result += p.get().weight()
        return result

    @property
    def max_tid(self):
        cdef TID_t result = 0
        values = self.entry.get().p_values
        for p in values:
            if p.get().tid > result:
                result = p.get().tid
        return result

    @property
    def newest_value(self):
        cdef SingleValueEntry_p entry = self.entry.get().p_values.front()
        values = self.entry.get().p_values
        for p in values:
            if p.get().tid > entry.get().tid:
                entry = p
        return value_from_entry(entry)

    def __mod__(self, tid):
        cdef _MultipleValues me = <_MultipleValues>self
        cdef SingleValue result
        values = me.entry.get().p_values
        for entry in values:
            result = value_from_entry(entry).__mod__(tid)
            if result is not None:
                return result
        return None

    def __ilshift__(self, TID_t tid):
        # If we have the TID, everything else should be older,
        # unless we just overwrote and haven't made the transaction visible yet.
        # By (almost) definition, nothing newer, but if there is, we shouldn't
        # drop it.
        # So this works like invalidation: drop everything older than the
        # tid; if we still have anything left, find and freeze the tid;
        # if that's the *only* thing left, return that, otherwise return ourself.
        entry = self.entry.get()
        entry.remove_tids_lt(tid)

        if entry.p_values.empty():
            return None

        if entry.p_values.size() == 1:
            # One item, either it or not
            sve_p = entry.p_values.front()
            result = value_from_entry(sve_p)
            result <<= tid
            return result

        # Multiple items, possibly in the future.
        begin = entry.p_values.begin()
        end = entry.p_values.end()
        while begin != end:
            sve_p = deref(begin)
            if sve_p.get().tid == tid:
                entry.p_values.erase(begin)
                value = value_from_entry(sve_p)
                value <<= tid
                entry.p_values.insert(begin, (<SingleValue>value).entry)
                break
            postinc(begin)
        return self

    def __iadd__(self, SingleValue value):
        self.entry.get().push_back(value.entry)
        return self

    def __isub__(self, TID_t tid):
        self.entry.get().remove_tids_lte(tid)

        if self.entry.get().p_values.empty():
            return None

        if self.entry.get().p_values.size() == 1:
            return value_from_entry(self.entry.get().p_values.front())

        return self

    def __iter__(self):
        return iter([
            value_from_entry(v)
            for v
            in self.entry.get().p_values
        ])

    def __repr__(self):
        return repr([
            tuple(v)
            for v in self
        ])
