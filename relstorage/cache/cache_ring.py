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
"""
Segmented LRU implementations.
"""
from __future__ import absolute_import, print_function, division

import functools
import itertools
try:
    izip = itertools.izip
except AttributeError:
    # Python 3
    izip = zip # pylint:disable=redefined-variable-type

from relstorage.cache import _cache_ring

ffi = _cache_ring.ffi # pylint:disable=no-member
_FFI_RING = _cache_ring.lib # pylint:disable=no-member

_ring_move_to_head = _FFI_RING.rsc_ring_move_to_head
_ring_del = _FFI_RING.rsc_ring_del
_ring_add = _FFI_RING.rsc_ring_add


ffi_new = ffi.new
ffi_new_handle = ffi.new_handle
ffi_from_handle = ffi.from_handle

_lru_update_mru = _FFI_RING.rsc_update_mru
_lru_probation_on_hit = _FFI_RING.rsc_probation_on_hit
_eden_add = _FFI_RING.rsc_eden_add
_lru_on_hit = _FFI_RING.rsc_on_hit
_lru_age_lists = _FFI_RING.rsc_age_lists
_eden_add_many = _FFI_RING.rsc_eden_add_many

class _NoSuchGeneration(object):
    """Marker object for the missing generation ring"""
    # For more specific error messages; if we get an AttributeError
    # on this object, it means we have corrupted the cache. We only
    # expect to wind up with this in generation 0.

    def __init__(self, generation_number):
        self.__gen_num = generation_number

    def __getattr__(self, name):
        msg = "Generation %s has no attribute %r" % (self.__gen_num, name)
        raise AttributeError(msg)


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

    You are responsible for moving items into and out of the `data` and determining
    what a hit is.
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

    # Should we allocate some nodes in a contiguous block on startup?
    _preallocate_entries = True
    # If so, how many? Try to get enough to fill the cache assuming objects are
    # this size on average
    _preallocate_avg_size = 512
    # But no more than this number.
    _preallocate_max_count = 150000 # 8 MB array

    #: A "mapping" between the __parent__ of an entry and the generation
    #: ring that holds it. (Indexing by ints is faster than a dictionary lookup
    #: especially on PyPy.) Initialize to an object that will report a bad lookup
    #: for the right generation. (The generator expression keeps us from leaking the
    #: indexing variable on Py2.)
    generations = tuple((_NoSuchGeneration(i) for i in range(4)))

    def __init__(self, byte_limit):
        self._byte_limit = byte_limit
        # This must hold all the ring entries, no matter which ring they are in.
        self.data = {}

        self.protected = ProtectedRing(int(byte_limit * self._gen_protected_pct))
        self.probation = ProbationRing(int(byte_limit * self._gen_probation_pct))
        self.eden = EdenRing(int(byte_limit * self._gen_eden_pct))

        self.cffi_cache = ffi_new("RSCache*",
                                  {'eden': self.eden.ring_home,
                                   'protected': self.protected.ring_home,
                                   'probation': self.probation.ring_home})

        generations = list(Cache.generations) # Preserve the NoSuchGeneration initializers
        for gen in (self.protected, self.probation, self.eden):
            generations[gen.PARENT_CONST] = gen
        self.generations = tuple(generations)

        # Setup the shared data structures for the generations
        node_free_list = self._make_node_free_list()
        for value, name in ((self.data, 'data'),
                            (self.cffi_cache, 'cffi_cache'),
                            (node_free_list, 'node_free_list')):
            for ring in self.generations[1:]:
                setattr(ring, name, value)

    def _make_node_free_list(self):
        "Create the node free list and preallocate any desired entries"
        node_free_list = []
        if self._preallocate_entries:
            needed_entries = self._byte_limit // self._preallocate_avg_size
            entry_count = min(self._preallocate_max_count, needed_entries)

            keys_and_values = itertools.repeat(('', b''), entry_count)
            _, nodes = self.eden._preallocate_entries(keys_and_values, entry_count)
            node_free_list.extend(nodes)
        return node_free_list

    def age_lists(self):
        _lru_age_lists(self.cffi_cache)


class CacheRingNode(object):

    __slots__ = (
        'key', 'value', 'len',
        'cffi_ring_node', 'cffi_ring_handle',
        'cffi_entry',
        # This is an owning pointer that is allocated when we
        # are imported from a persistent file. It keeps a whole array alive
        '_cffi_owning_node'
    )

    def __init__(self, key, value, node=None):
        self.key = key
        self.value = value
        self._cffi_owning_node = None
        # Passing the string is faster than passing a cdecl because we
        # have the string directly in bytecode without a lookup
        if node is None:
            node = ffi_new('RSRingNode*')
        self.cffi_ring_node = node

        # Directly setting attributes is faster than the initializer
        node.user_data = self.cffi_ring_handle = ffi_new_handle(self)
        entry = self.cffi_entry = node.u.entry
        entry.frequency = 1
        # We denormalize len to avoid accessing through CFFI (but it is needed
        # by the C code)
        self.len = entry.weight = len(key) + len(value)

    def reset(self, key='', value=b''):
        self.key = key
        self.value = value
        entry = self.cffi_entry
        entry.frequency = 1
        self.len = entry.weight = len(key) + len(value)

    def reset_for_free_list(self):
        """
        Put this node into an invalid state, representing that it
        should not be in a ring, but just the free list.

        You must call `reset` to use this node again.
        """
        self.key = self.value = self.len = None
        self.cffi_entry.r_parent = 0 # make sure we can't dereference a generation

    frequency = property(lambda self: self.cffi_entry.frequency,
                         lambda self, nv: setattr(self.cffi_entry, 'frequency', nv))

    def set_value(self, value):
        if value == self.value:
            return
        self.value = value
        self.len = self.cffi_entry.weight = len(self.key) + len(value)

    # We don't implement __len__---we want people to access .len
    # directly to avoid the function call as it showed up in benchmarks

    def __repr__(self):
        return ("<%s key=%r f=%d size=%d>" %
                (type(self).__name__, self.key, self.frequency, self.len))

def _mutates_free_list(func):
    @functools.wraps(func)
    def mutates(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        finally:
            self._mutated_free_list = True
            # Now replace ourself with a "bound function" on the instance
            # so our overhead somewhat goes away
            setattr(self, func.__name__, lambda *args: func(self, *args))

    return mutates

class CacheRing(object):

    # For the bulk insertion method add_MRUs in the eden generation, we need
    # to know whether or not the node_free_list we have is still the original
    # contiguous array that can be passed to C.
    _mutated_free_list = False

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
        node = self.ring_home = ffi.new("RSRing")
        node.r_next = node
        node.r_prev = node
        node.u.head.max_weight = limit
        node.u.head.generation = self.PARENT_CONST

        self.node_free_list = []

    def _preallocate_entries(self, ordered_keys_and_values, count=None):
        """
        Create and return *count* CacheRingNode values.

        The underlying RSRingNode structs will be allocated in a single contiguous
        C array.

        Return the RSRingNode pointer and the CacheRingNodes.
        """
        count = len(ordered_keys_and_values) if count is None else count
        nodes = ffi.new('RSRingNode[]', count)
        entries = []
        for i, (k, v) in enumerate(ordered_keys_and_values):
            node = nodes + i # pointer arithmetic gets RSRingNode*; nodes[i] returns the struct
            entry = CacheRingNode(k, v, node)
            entry._cffi_owning_node = nodes
            entries.append(entry)
        return nodes, entries

    def iteritems(self):
        head = self.ring_home
        here = head.r_next
        while here != head:
            yield here
            here = here.r_next

    def __iter__(self):
        for node in self.iteritems():
            yield ffi_from_handle(node.user_data)


    def __bool__(self):
        return bool(self.ring_home.u.head.len)

    __nonzero__ = __bool__ # Python 2

    def __len__(self):
        return self.ring_home.u.head.len

    @property
    def size(self):
        return self.ring_home.u.head.sum_weights

    @_mutates_free_list
    def add_MRU(self, key, value):
        node_free_list = self.node_free_list
        if node_free_list:
            new_entry = node_free_list.pop()
            new_entry.reset(key, value)
        else:
            new_entry = CacheRingNode(key, value)

        _ring_add(self.ring_home, new_entry.cffi_ring_node)
        return new_entry

    def get_LRU(self):
        # Only for testing
        return ffi_from_handle(self.ring_home.r_next.user_data)

    def make_MRU(self, entry):
        # Only for testing
        _ring_move_to_head(self.ring_home, entry.cffi_ring_node)

    @_mutates_free_list
    def update_MRU(self, entry, value):
        old_size = entry.len
        entry.set_value(value)
        new_size = entry.len
        if old_size == new_size:
            # Treat it as a simple hit
            return self.on_hit(entry)

        rejected_items = _lru_update_mru(self.cffi_cache, self.ring_home, entry.cffi_ring_node, old_size, new_size)

        if not rejected_items.r_next:
            # Nothing rejected.
            return

        dct = self.data
        node = rejected_items.r_next
        node_free_list = self.node_free_list
        while node:
            old_entry = dct.pop(ffi_from_handle(node.user_data).key)
            old_entry.reset_for_free_list()
            node_free_list.append(old_entry)

            node = node.r_next

    def on_hit(self, entry):
        _lru_on_hit(self.ring_home, entry.cffi_ring_node)

    def delete(self, entry):
        its_node = entry.cffi_ring_node
        return _ring_del(self.ring_home, its_node)

    remove = delete

    def stats(self):
        return {
            'limit': self.limit,
            'size': self.size,
            'count': len(self),
            'free_list': len(self.node_free_list),
        }


class EdenRing(CacheRing):

    PARENT_CONST = 1

    @_mutates_free_list
    def add_MRUs(self, ordered_keys_and_values):
        number_nodes = len(ordered_keys_and_values)
        if not number_nodes:
            return ()
        # Start by using existing entries *if* we haven't mutated the free list
        # (Because the C code needs contiguous data)
        if not self._mutated_free_list and self.node_free_list:
            self._mutated_free_list = True
            # Take the number of entries out of the free list and
            # pair them up with keys/values
            entries = self.node_free_list[:number_nodes]
            nodes = entries[0]._cffi_owning_node
            del self.node_free_list[:number_nodes]
            for entry, (k, v) in izip(entries, ordered_keys_and_values):
                entry.reset(k, v)

            # Move the freelist nodes into the ring. Anything that
            # doesn't fit is moved back onto the freelist
            added_entries = self.__add_MRUs(nodes, entries)

            # Did we have any more incoming key/values than we had freelist nodes?
            remaining_keys_and_values = ordered_keys_and_values[len(entries):]
            # If we did have mare, and we successfully added all the entries from the freelist,
            # then go ahead and add as many of the leftovers as we can
            if remaining_keys_and_values and (len(added_entries) == len(entries)):
                added_entries.extend(self.add_MRUs(remaining_keys_and_values))

            return added_entries

        nodes, entries = self._preallocate_entries(ordered_keys_and_values, number_nodes)
        return self.__add_MRUs(nodes, entries)



    def __add_MRUs(self, nodes, entries):
        number_nodes = len(entries)
        # Only return the objects we added, allowing the rest to become garbage.

        added_count = _eden_add_many(self.cffi_cache,
                                     nodes,
                                     number_nodes)
        if not added_count:
            # Allow any nodes we preallocated to get GC'd now
            return ()

        if added_count == number_nodes:
            # Yay, they all fit!
            return entries

        # Ok, some few did not fit, so we have to separate them out.
        # Because we went to the trouble of pre-allocating them, we might
        # as well put them on the free list if we didn't use them. The
        # whole array will stay around around as long as any one
        # object does
        node_free_list = self.node_free_list
        added_entries = []
        for e in entries:
            if e.cffi_entry.r_parent == -1:
                # -1 is the sentinel meaning this node wasn't added,
                # but we don't want to leave that around because that's a
                # valid index in Python, so convert back to 0, which is not.
                # Also free whatever python memory it was holding on to.
                e.reset_for_free_list()
                node_free_list.append(e)
            else:
                added_entries.append(e)
        return added_entries

    @_mutates_free_list
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
            old_entry.reset_for_free_list()
            node_free_list.append(old_entry)
            node = node.r_next
        return new_entry

class ProtectedRing(CacheRing):
    PARENT_CONST = 2


class ProbationRing(CacheRing):

    PARENT_CONST = 3

    def on_hit(self, entry):
        # Move the entry to the protected LRU on its very first hit, where
        # it becomes the MRU.
        _lru_probation_on_hit(self.cffi_cache, entry.cffi_ring_node)
