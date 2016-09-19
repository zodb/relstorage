# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from __future__ import absolute_import, print_function, division

"""
Segmented LRU implementations.

"""

from .ring import Ring

from cffi import FFI
import os
this_dir = os.path.dirname(os.path.abspath(__file__))

# ffi = FFI()
# ffi.cdef("""
# typedef struct CPersistentRing_struct {
#     ...;
# } CPersistentRing;

# typedef struct RSLRUEntry_struct {
# 	CPersistentRing ring_entry;
# 	uint_fast64_t frequency;
# 	uint_fast64_t len;
# } RSLRUEntry_t;

# """
# )

# _FFI_RING = ffi.verify("""
# #include "lru.h"
# """, include_dirs=[this_dir])

# ffi_new = ffi.new

from .ring import ffi
ffi_new = ffi.new
ffi_new_handle = ffi.new_handle

from .ring import _FFI_RING

_lru_update_mru = _FFI_RING.lru_update_mru

class SizedLRURingEntry(object):

    __slots__ = ('key', 'value', '__parent__',
                 'cffi_ring_node', 'cffi_ring_handle')

    def __init__(self, key, value, parent):
        self.key = key
        self.value = value
        self.__parent__ = parent
        self.cffi_ring_handle = ffi_new_handle(self)
        self.cffi_ring_node = ffi_new('CPersistentRing*',
                                      {'len': len(key) + len(value),
                                       'user_data': self.cffi_ring_handle,
                                       'frequency': 1})

    @property
    def len(self):
        return self.cffi_ring_node.len

    frequency = property(lambda self: self.cffi_ring_node.frequency,
                         lambda self, nv: setattr(self.cffi_ring_node, 'frequency', nv))

    def set_value(self, value):
        self.value = value
        self.cffi_ring_node.len = len(self.key) + len(value)

    def __len__(self):
        return self.len

    def __repr__(self):
        return ("<%s key=%r f=%d size=%d>" %
                (type(self).__name__, self.key, self.frequency, self.len))

class SizedLRU(object):
    """
    A LRU list that keeps track of its size.
    """

    def __init__(self, limit):
        self.limit = limit
        self._ring = Ring()
        self.get_LRU = self._ring.lru
        self.make_MRU = self._ring.move_to_head
        self.remove = self._ring.delete

    def __iter__(self):
        return iter(self._ring)

    def __bool__(self):
        return bool(len(self._ring))

    __nonzero__ = __bool__ # Python 2

    def __len__(self):
        return self._ring.ring_home.len

    @property
    def size(self):
        return self._ring.ring_home.frequency

    @property
    def over_size(self):
        return self.size > self.limit

    def add_MRU(self, key, value):
        entry = SizedLRURingEntry(key, value, self)
        self._ring.add(entry)
        #self.size += entry.len
        #entry.frequency += 1
        return entry

    def take_ownership_of_entry_MRU(self, entry):
        #assert entry.__parent__ is None
        old_parent = entry.__parent__
        assert old_parent is not None
        assert old_parent is not self

        # But don't increment here, we're just moving
        # from one ring to another
        self._ring.move_entry_from_other_ring(entry, old_parent._ring)
        entry.__parent__ = self


    def update_MRU(self, entry, value):
        #assert entry.__parent__ is self
        old_size = entry.len
        entry.set_value(value)
        new_size = entry.len
        _lru_update_mru(self._ring.ring_home, entry.cffi_ring_node, old_size, new_size)

    # def remove(self, entry):
    #     #assert entry.__parent__ is self
    #     self._ring.delete(entry)

    #     # XXX We must leave these.
    #     # All released versions of CFFI Ring don't clear this to None;
    #     # that can cause a crash if there's a bug in our code and we reuse
    #     # the node in the wrong place.
    #     #entry._Persistent__ring = None
    #     #entry.__parent__ = None

    def on_hit(self, entry):
        #assert entry.__parent__ is self
        entry.frequency += 1
        self.make_MRU(entry)

    def stats(self):
        return {
            'limit': self.limit,
            'size': self.size,
            'count': len(self._ring),
        }
