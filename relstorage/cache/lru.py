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

class Cache(object):
    """
    A sized cache.

    The cache moves items between three rings, as determined by
    an admittance policy.

    * Items begin in *eden*, where they stay until eden grows too
      large.
    * When eden grows too large, the least recently used item
      is then (conceptually) moved to the *probation* ring. If this
      would make the probation ring too large, the *frequency* of
      the least recently used item from the probation ring is compared to the frequency
      of the incoming item. Only if the incoming item is more popular than
      the item it would force off the probation ring is it kept (and the probation item removed).
      Otherwise the eden item is removed.
    * When an item in probation is accessed, it is moved to the *protected* ring.
      The protected ring is the largest ring. When adding an item to it would
      make it too large, the least recently used item is demoted to probation, following
      the same rules as for eden.

    This cache only approximately follows its size limit. It may temporarily become
    larger.
    """

    # Percentage of our byte limit that should be dedicated
    # to the main "protected" generation
    _gen_protected_pct = 0.8
    # Percentage of our byte limit that should be dedicated
    # to the initial "eden" generation
    _gen_eden_pct = 0.1
    # Percentage of our byte limit that should be dedicated
    # to the "probationary"generation
    _gen_probation_pct = 0.1
    # By default these numbers add up to 1.0, but it would be possible to
    # overcommit by making them sum to more than 1.0. (For very small
    # limits, the rounding will also make them overcommit).

    #: A "mapping" between the __parent__ of an entry and the generation
    #: ring that holds it.
    generations = (None, None, None, None)

    def __init__(self, byte_limit):
        # This must hold all the ring entries, no matter which ring they are in.
        self.data = {}


        self.protected = ProtectedRing(int(byte_limit * self._gen_protected_pct))
        self.probation = ProbationRing(int(byte_limit * self._gen_probation_pct),
                                       self.protected)
        self.eden = EdenRing(int(byte_limit * self._gen_eden_pct),
                             self.probation,
                             self.protected)

        self.cffi_cache = ffi_new("RSCache*",
                                   {'eden': self.eden._ring.ring_home,
                                    'protected': self.protected._ring.ring_home,
                                    'probation': self.probation._ring.ring_home})

        self.generations = [None, None, None, None] # 0 isn't used
        for x in (self.protected, self.probation, self.eden):
            self.generations[x.PARENT_CONST] = x
        self.generations = tuple(self.generations)


        for value, name in ((self.data, 'data'),
                            (self.cffi_cache, 'cffi_cache'),
                            ([], 'node_free_list')):
            for ring in self.generations[1:]:
                setattr(ring, name, value)


    def age_lists(self):
        _lru_age_lists(self.cffi_cache)


class CacheRingNode(object):

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
        if value == self.value:
            return
        self.value = value
        self.len = self.cffi_ring_node.len = len(self.key) + len(value)

    def __len__(self):
        return self.len

    def __repr__(self):
        return ("<%s key=%r f=%d size=%d>" %
                (type(self).__name__, self.key, self.frequency, self.len))


class CacheRing(object):

    # The CFFI pointer to the RSCache structure. It should be shared
    # among all the rings of the cache.
    cffi_cache = None

    # The dictionary holding all the entries. This is needed
    # so we can handle eviction. It should be shared among all the
    # rings of a cache.
    data = None

    # The list of free CacheRingNode objects. It should be shared
    # among all the rings of a cache.
    node_free_list = ()

    PARENT_CONST = 0


    def __init__(self, limit):
        self.limit = limit
        self._ring = Ring()
        self._ring.ring_home.max_len = limit
        self._ring.ring_home.r_parent = self.PARENT_CONST

        self.node_free_list = []

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
        node_free_list = self.node_free_list
        if node_free_list:
            new_entry = node_free_list.pop()
            new_entry.reset(key, value)
        else:
            new_entry = CacheRingNode(key, value)


        self._ring.add(new_entry)
        return new_entry

    def update_MRU(self, entry, value):
        old_size = entry.len
        entry.set_value(value)
        new_size = entry.len
        if old_size == new_size:
            # Treat it as a simple hit
            return self.on_hit(entry)

        rejected_items = _lru_update_mru(self.cffi_cache, self._ring_home, entry.cffi_ring_node, old_size, new_size)

        if not rejected_items.r_next:
            # Nothing rejected.
            return

        dct = self.data
        node = rejected_items.r_next
        node_free_list = self.node_free_list
        while node:
            old_entry = dct.pop(ffi_from_handle(node.user_data).key)
            node_free_list.append(old_entry)
            old_entry.key = None
            old_entry.value = None

            node = node.r_next

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


class EdenRing(CacheRing):

    PARENT_CONST = 1

    def __init__(self, limit, probation_lru, protected_lru):
        CacheRing.__init__(self, limit)
        self.probation_lru = probation_lru
        self.protected_lru = protected_lru

    def _preallocate_entries(self, ordered_keys_and_values):
        count = len(ordered_keys_and_values)
        nodes = ffi.new('RSRingNode[]', count)
        entries = []
        for i in range(count):
            k, v = ordered_keys_and_values[i]
            node = nodes + i # this gets RSRingNode*; nodes[i] returns the struct
            entry = CacheRingNode(k, v, node)
            entry._cffi_owning_node = nodes
            entries.append(entry)
        return nodes, entries

    def add_MRUs(self, ordered_keys_and_values):
        nodes, entries = self._preallocate_entries(ordered_keys_and_values)
        number_nodes = len(ordered_keys_and_values)
        added_count = _eden_add_many(self.cffi_cache,
                                     nodes,
                                     number_nodes)
        # Only return the objects we added, allowing the rest to become garbage.
        # TODO: Put them on the node_free_list? Or try to allow the node array to be
        # gc'd (eventually)?
        if added_count < number_nodes:
            return entries[:added_count]
        return entries

    def add_MRU(self, key, value):
        node_free_list = self.node_free_list
        if node_free_list:
            new_entry = node_free_list.pop()
            new_entry.reset(key, value)
        else:
            new_entry = CacheRingNode(key, value)
        rejected_items = _eden_add(self.cffi_cache,
                                   new_entry.cffi_ring_node)

        if not rejected_items.r_next:
            # Nothing rejected.
            return new_entry

        dct = self.data
        node = rejected_items.r_next
        while node:
            old_entry = dct.pop(ffi_from_handle(node.user_data).key)
            # TODO: Should we avoid this if _cffi_owning_node is set?
            # To allow that big array to get GC'd sooner
            node_free_list.append(old_entry)
            old_entry.key = None
            old_entry.value = None
            node = node.r_next
        return new_entry

class ProtectedRing(CacheRing):
    PARENT_CONST = 2


class ProbationRing(CacheRing):

    PARENT_CONST = 3

    def __init__(self, limit, protected_lru):
        CacheRing.__init__(self, limit)
        self.protected_lru = protected_lru

    def on_hit(self, entry):
        # Move the entry to the protected LRU on its very first hit, where
        # it becomes the MRU.
        return _lru_probation_on_hit(self.cffi_cache,
                                     entry.cffi_ring_node)
