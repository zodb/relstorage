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

from .ring import ffi
ffi_new = ffi.new
ffi_new_handle = ffi.new_handle
ffi_from_handle = ffi.from_handle

from .ring import _FFI_RING

_lru_update_mru = _FFI_RING.lru_update_mru
_lru_probation_on_hit = _FFI_RING.lru_probation_on_hit
_eden_add = _FFI_RING.eden_add
_lru_on_hit = _FFI_RING.lru_on_hit
_lru_age_lists = _FFI_RING.lru_age_lists
_eden_add_many = _FFI_RING.eden_add_many

class SizedLRURingEntry(object):

    __slots__ = (
        'key', 'value', 'len',
        'cffi_ring_node', 'cffi_ring_handle',
        # This is an owning pointer that is allocated when we
        # are imported from a persistent file. It keeps a whole array alive
        '_cffi_owning_node'
    )

    def __init__(self, key, value, node=None):
        self.key = key
        self.value = value

        # Passing the string is faster than passing a cdecl because we
        # have the string directly in bytecode without a lookup
        if node is None:
            node = ffi_new('RSRingNode*')
        self.cffi_ring_node = node

        # Directly setting attributes is faster than the initializer
        node.user_data = self.cffi_ring_handle = ffi_new_handle(self)
        node.frequency = 1
        # We denormalize len to avoid accessing through CFFI (but it is needed
        # by the C code)
        self.len = node.len = len(key) + len(value)

    def reset(self, key, value):
        self.key = key
        self.value = value
        node = self.cffi_ring_node
        node.frequency = 1
        self.len = node.len = len(key) + len(value)

    frequency = property(lambda self: self.cffi_ring_node.frequency,
                         lambda self, nv: setattr(self.cffi_ring_node, 'frequency', nv))

    def set_value(self, value):
        self.value = value
        self.len = self.cffi_ring_node.len = len(self.key) + len(value)

    def __len__(self):
        return self.len

    def __repr__(self):
        return ("<%s key=%r f=%d size=%d>" %
                (type(self).__name__, self.key, self.frequency, self.len))

class SizedLRU(object):
    """
    A LRU list that keeps track of its size.
    """

    PARENT_CONST = 0

    @classmethod
    def age_lists(cls, eden):
        _lru_age_lists(eden._cffi_cache)

    def __init__(self, limit):
        self.limit = limit
        self._ring = Ring()
        self._ring.ring_home.max_len = limit
        self._ring.ring_home.r_parent = self.PARENT_CONST
        # caches
        self._ring_home = self._ring.ring_home
        self.get_LRU = self._ring.lru
        self.make_MRU = self._ring.move_to_head
        self.remove = self.delete

    def __iter__(self):
        return iter(self._ring)

    def __bool__(self):
        return bool(self._ring_home.len)

    __nonzero__ = __bool__ # Python 2

    def __len__(self):
        return self._ring_home.len

    @property
    def size(self):
        return self._ring_home.frequency

    def add_MRU(self, key, value):
        entry = SizedLRURingEntry(key, value)
        self._ring.add(entry)
        return entry

    def update_MRU(self, entry, value):
        old_size = entry.len
        entry.set_value(value)
        new_size = entry.len
        # XXX: Need to rebalance, if needed.
        _lru_update_mru(self._ring.ring_home, entry.cffi_ring_node, old_size, new_size)

    def on_hit(self, entry):
        return _lru_on_hit(self._ring_home, entry.cffi_ring_node)

    def delete(self, entry):
        self._ring.delete(entry)

    def stats(self):
        return {
            'limit': self.limit,
            'size': self.size,
            'count': len(self._ring),
        }


class EdenLRU(SizedLRU):

    PARENT_CONST = 1

    def __init__(self, limit, probation_lru, protected_lru, entry_dict):
        SizedLRU.__init__(self, limit)
        self.probation_lru = probation_lru
        self.protected_lru = protected_lru
        self._cffi_cache = ffi_new("RSCache*",
                                   {'eden': self._ring.ring_home,
                                    'protected': self.protected_lru._ring.ring_home,
                                    'probation': self.probation_lru._ring.ring_home})
        probation_lru._cffi_cache = self._cffi_cache
        protected_lru._cffi_cache = self._cffi_cache
        self.entry_dict = entry_dict
        self._node_free_list = []

    def _preallocate_entries(self, ordered_keys_and_values):
        count = len(ordered_keys_and_values)
        nodes = ffi.new('RSRingNode[]', count)
        entries = []
        for i in range(count):
            k, v = ordered_keys_and_values[i]
            node = nodes + i # this gets RSRingNode*; nodes[i] returns the struct
            entry = SizedLRURingEntry(k, v, node)
            entry._cffi_owning_node = nodes
            entries.append(entry)
        return nodes, entries

    def add_MRUs(self, ordered_keys_and_values):
        nodes, entries = self._preallocate_entries(ordered_keys_and_values)
        number_nodes = len(ordered_keys_and_values)
        added_count = _eden_add_many(self._cffi_cache,
                                     nodes,
                                     number_nodes)
        # Only return the objects we added, allowing the rest to become garbage.
        if added_count < number_nodes:
            return entries[:added_count]
        return entries

    def add_MRU(self, key, value):
        node_free_list = self._node_free_list
        if node_free_list:
            new_entry = node_free_list.pop()
            new_entry.reset(key, value)
        else:
            new_entry = SizedLRURingEntry(key, value)
        rejected_items = _eden_add(self._cffi_cache,
                                   new_entry.cffi_ring_node)

        if not rejected_items.r_next:
            # Nothing rejected.
            return new_entry

        dct = self.entry_dict
        node = rejected_items.r_next
        while node:
            old_entry = dct.pop(ffi_from_handle(node.user_data).key)
            node_free_list.append(old_entry)
            old_entry.key = None
            old_entry.value = None
            node = node.r_next
        return new_entry

class ProtectedLRU(SizedLRU):
    PARENT_CONST = 2


class ProbationLRU(SizedLRU):

    PARENT_CONST = 3

    def __init__(self, limit, protected_lru, entry_dict):
        SizedLRU.__init__(self, limit)
        self.protected_lru = protected_lru
        self._cffi_cache = None # Will be set when we're added to eden

    def on_hit(self, entry):
        # Move the entry to the protected LRU on its very first hit, where
        # it becomes the MRU.
        return _lru_probation_on_hit(self._cffi_cache,
                                     entry.cffi_ring_node)
