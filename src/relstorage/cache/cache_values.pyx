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

from cpython.buffer cimport PyBuffer_FillInfo

from libcpp.memory cimport shared_ptr
from libcpp.memory cimport make_shared
from libcpp.memory cimport dynamic_pointer_cast
from libcpp.pair cimport pair
from libcpp.string cimport string

from relstorage.cache.c_cache cimport TID_t
from relstorage.cache.c_cache cimport OID_t
from relstorage.cache.c_cache cimport Pickle_t
from relstorage.cache.c_cache cimport SingleValueEntry
from relstorage.cache.c_cache cimport SingleValueEntry_p
from relstorage.cache.c_cache cimport AbstractEntry
from relstorage.cache.c_cache cimport AbstractEntry_p
from relstorage.cache.c_cache cimport MultipleValueEntry

import sys
from relstorage.cache.interfaces import CacheConsistencyError

cdef extern from *:
    """
    template <typename T>
    static
    void it_assign(T& it,
                   relstorage::cache::SingleValueEntry_p& p) {
        *it = p;
    }
    """
    void it_assign[T](T&, SingleValueEntry_p&)

cdef object value_from_entry(const AbstractEntry_p& entry):
    cdef SingleValueEntry_p sve_p
    cdef MultipleValueEntry_p mve_p

    cdef SingleValue sv
    cdef MultipleValues mv

    sve_p = dynamic_pointer_cast[SingleValueEntry, AbstractEntry](entry)
    if sve_p:
        if sve_p.get().frozen:
            sv = FrozenValue.from_entry(sve_p)
        else:
            sv = SingleValue.from_entry(sve_p)
        return sv

    mve_p = dynamic_pointer_cast[MultipleValueEntry, AbstractEntry](entry)
    if not mve_p:
        print("Unable to get object type", entry.get().key)
        raise AssertionError("Invalid pointer cast", entry.get().key)
    return MultipleValues.from_entry(mve_p)

cdef object python_from_sve(SingleValueEntry_p& entry):
    cdef AbstractEntry_p ae = dynamic_pointer_cast[AbstractEntry, SingleValueEntry](entry)
    return value_from_entry(ae)

cdef AbstractEntry_p entry_from_python(object value) except *:
    cdef SingleValue sv
    cdef MultipleValues mv
    if isinstance(value, SingleValue):
        sv = <SingleValue>value
        return dynamic_pointer_cast[AbstractEntry, SingleValueEntry](sv.entry)
    if isinstance(value, MultipleValues):
        mv = <MultipleValues>value
        return dynamic_pointer_cast[AbstractEntry, MultipleValueEntry](mv.entry)
    raise TypeError("Object %r is not a cache value" % (value,))

# Memory management notes:
#
# Converting from Pickle_t to Python bytes creates a copy of the
# memory under Python control. We avoid that when we're read by
# using a buffer object, which can be read by cStringIO and io.BytesIO.
#
# Freelists only work on classes that do not inherit from
# anything except object. I think they also must be final.
# So we could use them between SingleValue and FrozenValue if we
# implemented the later with composition.

cdef bint PY2 = sys.version_info[0] == 2

@cython.final
@cython.internal
cdef class StringWrapper:
    cdef SingleValueEntry_p entry

    @staticmethod
    cdef from_entry(const SingleValueEntry_p& entry):
        if PY2:
            # Too many things on Python 2 don't handle the
            # new-style buffers that Cython creates. Notably, file.writelines()
            # and zlib.decompress() require old-style buffers
            return entry.get().state
        cdef StringWrapper w = StringWrapper.__new__(StringWrapper)
        w.entry = entry
        return w

    cdef from_substring(self, const string substr):
        return StringWrapper.from_entry(
            SingleValueEntry_p(self.entry,
                               new SingleValueEntry(self.entry.get().key,
                                                    substr,
                                                    self.entry.get().tid,
                                                    False))
        )

    def __getbuffer__(self, Py_buffer* view, int flags):
        PyBuffer_FillInfo(view, self,
                          <void*>self.entry.get().state.data(),
                          self.entry.get().state.size(),
                          1,
                          flags)

    def __releasebuffer__(self, Py_buffer* view):
        pass

    def __len__(self):
        return self.entry.get().state.size()

    def __getitem__(self, ix):
        cdef string* s = &self.entry.get().state
        if ix == slice(None, 2, None):
            # We need to be able to hash this, it's the compression prefix.
            return <bytes>s.substr(0, 2)
        if ix == slice(2, None, None):
            return self.from_substring(s.substr(2))

        return (<bytes>deref(s))[ix]

    def __eq__(self, other):
        if isinstance(other, StringWrapper):
            return (<StringWrapper>other).entry == self.entry
        return bytes(self) == other

    def __str__(self):
        return str(<bytes>self.entry.get().state)

    def __bytes__(self):
        return <bytes>self.entry.get().state

    def __repr__(self):
        return repr(<bytes>self.entry.get().state)

cdef class CachedValue:
    """
    The base class for cached values.
    """

    cpdef get_if_tid_matches(self, object tid):
        raise NotImplementedError

    cpdef freeze_to_tid(self, TID_t tid):
        raise NotImplementedError

    cpdef with_later(self, tuple value):
        raise NotImplementedError

    cpdef discarding_tids_before(self, TID_t tid):
        raise NotImplementedError


cdef class SingleValue(CachedValue):
    cdef SingleValueEntry_p entry
    frozen = False

    def __cinit__(self, OID_t oid, object state, TID_t tid, bint frozen=False):
        if state is SingleValue:
            # Marker passed in from value_from_entry
            # not to do anything, we're shared.
            return


        # implicit cast and copy state from bytes to std::string.
        self.entry = SingleValue.make_shared(oid, state, tid, frozen)

    @staticmethod
    cdef SingleValueEntry_p make_shared(OID_t oid, object state, TID_t tid, bint frozen=False):
        if state is None:
            state = b''

        return make_shared[SingleValueEntry](oid, pair[Pickle_t, TID_t](state, tid), frozen)

    @staticmethod
    cdef SingleValue from_entry(const SingleValueEntry_p& entry):
        cdef SingleValue sv = SingleValue.__new__(SingleValue, 0, SingleValue, 0, 0)
        sv.entry = entry
        return sv

    def sizeof(self):
        # At this writing, reports 88
        return sizeof(SingleValueEntry)

    def __iter__(self):
        value = self.entry.get()
        return iter((
            StringWrapper.from_entry(self.entry),
            value.tid
        ))

    @property
    def value(self):
        return self.state

    @property
    def key(self):
        return self.entry.get().key

    @property
    def frequency(self):
        return self.entry.get().frequency

    @property
    def state(self):
        return StringWrapper.from_entry(self.entry)

    @property
    def tid(self):
        return self.entry.get().tid

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
        if isinstance(other, tuple):
            return len(other) == 2 and self.tid == other[1] and self.value == other[0]
        return NotImplemented

    cpdef get_if_tid_matches(self, object tid):
        cdef SingleValue me
        cdef TID_t native_tid
        if tid is None:
            return None
        me = (<SingleValue>self)
        native_tid = <TID_t>tid
        if native_tid == me.entry.get().tid:
            return me

    cpdef freeze_to_tid(self, TID_t tid):
        # We could be newer
        cdef FrozenValue fv
        cdef const SingleValueEntry* entry = self.entry.get()
        if entry.tid > tid:
            return self
        if tid == entry.tid:
            fv = FrozenValue.from_entry(self.entry)
            # We are discarding ourself now, but preserving this item's
            # location in the generations. This is the only reason that
            # Entry.frozen is mutable.
            fv.entry.get().frozen = True
            return fv
        # if we're older, fall off the end and discard.

    cpdef with_later(self, tuple value):
        cdef object state = value[0] or b''
        cdef TID_t tid = value[1]
        cdef const SingleValueEntry* sve = self.entry.get()
        cdef bint state_equal = (<const Pickle_t>state == sve.state)
        cdef bint tid_equal = (tid == sve.tid)

        if (state_equal and tid_equal):
            return self

        if (not state_equal and tid_equal):
            raise CacheConsistencyError(
                "Detected two different values for same TID",
                self,
                value
            )

        return MultipleValues.__new__(MultipleValues, self, bytes(state), tid)

    cpdef discarding_tids_before(self, const TID_t tid):
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
        return "%s(%r, %s, frozen=%s)" % (
            self.__class__.__name__,
            self.state,
            self.tid,
            self.frozen,
        )



@cython.final
@cython.internal
cdef class FrozenValue(SingleValue):

    frozen = True

    @staticmethod
    cdef SingleValue from_entry(const SingleValueEntry_p& entry):
        cdef FrozenValue sv = FrozenValue.__new__(FrozenValue, 0, SingleValue, 0, 0)
        sv.entry = entry
        return sv


    cpdef get_if_tid_matches(self, tid):
        cdef SingleValue me
        cdef TID_t native_tid
        if tid is None:
            return self
        me = (<SingleValue>self)
        native_tid = <TID_t>tid
        if native_tid == me.entry.get().tid:
            return me

    cpdef freeze_to_tid(self, TID_t tid):
        # This method can get called if two different transaction views
        # tried to load an object at the same time and store it in the cache.
        if tid == self.entry.get().tid:
            return self

@cython.final
cdef class MultipleValues(CachedValue):
    cdef MultipleValueEntry_p entry
# TODO: we should keep this sorted by tid, yes?
# A std::map<tid, SingleValueEntry_p> sounds almost ideal
# for accessing max_tid and newest_value, except for whatever space
# overhead that adds.

    def __cinit__(self, SingleValue mv1, bytes state2, TID_t tid2):
        if mv1 is not None:
            self.entry = make_shared[MultipleValueEntry](mv1.entry.get().key)
            self.entry.get().push_back(mv1.entry)
            self.entry.get().push_back(SingleValue.make_shared(mv1.entry.get().key,
                                                               state2, tid2))

    @staticmethod
    cdef MultipleValues from_entry(const MultipleValueEntry_p& entry):
        cdef MultipleValues mv = MultipleValues.__new__(MultipleValues, None, None, 0)
        mv.entry = entry
        return mv

    def sizeof(self):
        # At this writing, reports 72.
        return sizeof(MultipleValueEntry)

    @property
    def value(self):
        return list(self)

    @property
    def key(self):
        return self.entry.get().key

    @property
    def frequency(self):
        return self.entry.get().frequency

    @property
    def weight(self):
        return self.entry.get().weight()

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
        return python_from_sve(entry)

    cpdef get_if_tid_matches(self, tid):
        cdef SingleValue result
        values = self.entry.get().p_values
        for entry in values:
            result = python_from_sve(entry).get_if_tid_matches(tid)
            if result is not None:
                return result
        return None

    cpdef freeze_to_tid(self, TID_t tid):
        # If we have the TID, everything else should be older,
        # unless we just overwrote and haven't made the transaction visible yet.
        # By (almost) definition, nothing newer, but if there is, we shouldn't
        # drop it.
        # So this works like invalidation: drop everything older than the
        # tid; if we still have anything left, find and freeze the tid;
        # if that's the *only* thing left, return that, otherwise return ourself.
        cdef SingleValueEntry_p sve_p
        cdef CachedValue value
        entry = self.entry.get()
        entry.remove_tids_lt(tid)

        if entry.empty():
            return None

        if entry.degenerate():
            # One item, either it or not
            sve_p = entry.front()
            result = python_from_sve(sve_p)
            result = (<CachedValue>result).freeze_to_tid(tid)
            return result

        # Multiple items, possibly in the future.
        begin = entry.p_values.begin()
        end = entry.p_values.end()
        while begin != end:
            sve_p = deref(begin)
            if sve_p.get().tid == tid:
                value = python_from_sve(sve_p)
                new_value = value.freeze_to_tid(tid)
                assert new_value is not None # But it could be, couldn't it?
                if new_value is not value:
                    # Assign the entry via copy constructor;
                    # using erase() invalidates the iterator
                    sve_p = (<SingleValue>new_value).entry
                    it_assign(begin, sve_p)
                break
            postinc(begin)
        return self

    cpdef with_later(self, tuple value):
        self.entry.get().push_back(SingleValue.make_shared(self.entry.get().key, value[0], value[1]))
        return self

    cpdef discarding_tids_before(self, TID_t tid):
        entry = self.entry.get()
        entry.remove_tids_lte(tid)

        if entry.empty():
            return None

        if entry.degenerate():
            return python_from_sve(entry.front())

        return self

    def __iter__(self):
        return iter([
            python_from_sve(v)
            for v
            in self.entry.get().p_values
        ])

    def __repr__(self):
        return repr([
            tuple(v)
            for v in self
        ])
