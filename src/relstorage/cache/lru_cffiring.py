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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import itertools

from zope import interface

from relstorage.cache.interfaces import IGenerationalLRUCache
from relstorage.cache.interfaces import IGeneration
from relstorage.cache.interfaces import ILRUEntry
from relstorage.cache.interfaces import GenerationalCacheBase
from . import _cache_ring

try:
    izip = itertools.izip
except AttributeError:
    # Python 3
    izip = zip

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



@interface.implementer(IGenerationalLRUCache)
class CFFICache(GenerationalCacheBase):

    # Should we allocate some nodes in a contiguous block on startup?
    # NOTE: For large cache sizes, this can be slow. It actually makes
    # the zodbshootout 'cold' tests look bad (for small object counts
    # especially, or large cache sizes) because when zodbshootout clears caches,
    # our implementation throws this object all away, and then allocates again.
    # Meanwhile, all the old objects have to be GC'd.
    _preallocate_entries = True
    # If so, how many? Try to get enough to fill the cache assuming objects are
    # this size on average
    _preallocate_avg_size = 512
    # But no more than this number.
    _preallocate_max_count = 150000 # 8 MB array

    _dict_type = dict

    @classmethod
    def create_generations(cls,
                           eden_limit=0,
                           protected_limit=0,
                           probation_limit=0,
                           key_weight=len, value_weight=len):
        cffi_cache = ffi_new("RSCache*")

        generations = {}
        for klass, limit in ((Eden, eden_limit),
                             (Protected, protected_limit),
                             (Probation, probation_limit)):
            generation = klass(limit, cffi_cache, key_weight, value_weight)
            setattr(cffi_cache, generation.__name__, generation.ring_home)
            generations[generation.__name__] = generation
        return generations

    def __init__(self, byte_limit, key_weight=len, value_weight=len):
        # This holds all the ring entries, no matter which ring they are in.

        # We experimented with using OOBTree and LOBTree for the type
        # of self.data. The OOBTree has a similar but slightly slower
        # performance profile (as would be expected given the big-O
        # complexity) as a dict, but very large ones can't be pickled
        # in a single shot! The LOBTree works faster and uses less
        # memory than the OOBTree or the dict *if* all the keys are
        # integers; which they currently are not. Plus the LOBTrees
        # are slower on PyPy than its own dict specializations. We
        # were hoping to be able to write faster pickles with large
        # BTrees, but since that's not the case, we abandoned the
        # idea.
        #
        # Maybe a two-level index, like fsIndex?

        self.data = self._dict_type()
        self.get = self.data.get

        generations = self.create_generations(
            eden_limit=int(byte_limit * self._gen_eden_pct),
            protected_limit=int(byte_limit * self._gen_protected_pct),
            probation_limit=int(byte_limit * self._gen_probation_pct),
            key_weight=key_weight,
            value_weight=value_weight
        )

        super(CFFICache, self).__init__(byte_limit,
                                        generations['eden'],
                                        generations['protected'],
                                        generations['probation'])

        self.cffi_cache = self.eden.cffi_cache

        # Setup the shared data structures for the generations
        node_free_list = self._make_node_free_list()
        for ring in self.generations[1:]:
            setattr(ring, 'node_free_list', node_free_list)

    def _make_node_free_list(self):
        "Create the node free list and preallocate any desired entries"
        node_free_list = []
        if self._preallocate_entries:
            needed_entries = self.limit // self._preallocate_avg_size
            entry_count = min(self._preallocate_max_count, needed_entries)
            node_free_list = self.eden.init_node_free_list(entry_count)
        return node_free_list


    # mapping operations, operating on user-level key/value pairs.

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, key):
        return key in self.data

    def __setitem__(self, key, value):
        entry = self.get(key)
        if entry is not None:
            # This bumps its frequency, and potentially ejects other items.
            self.update_MRU(entry, value)
        else:
            # New values have a frequency of 1 and might evict other
            # items.
            self.add_MRU(key, value)

        assert key in self

    def __delitem__(self, key):
        entry = self.data[key]
        del self.data[key]
        self.generations[entry.cffi_entry.r_parent].remove(entry)

    def __getitem__(self, key):
        entry = self.get(key)
        if entry is not None:
            self.on_hit(entry)
            return entry.value

    # Cache-specific operations.

    def get_from_key_or_backup_key(self, pref_key, backup_key):
        entry = self.get(pref_key)
        if entry is None:
            entry = self.get(backup_key)
            if entry is not None:
                # Swap the key (which we assume has the same weight).
                entry.key = pref_key
                del self.data[backup_key]
                self.data[pref_key] = entry
        if entry is not None:
            self.on_hit(entry)
            return entry.value

    def peek(self, key):
        entry = self.get(key)
        if entry is not None:
            return entry.value

    def age_frequencies(self):
        _lru_age_lists(self.cffi_cache)

    age_lists = age_frequencies # BWC

    def add_MRU(self, key, value):
        item, evicted_items = self.eden.add_MRU(key, value)
        for k, _ in evicted_items:
            del self.data[k]
        self.data[key] = item
        return item

    def add_MRUs(self, ordered_keys, return_count_only=False):
        added_entries = self.eden.add_MRUs(ordered_keys)
        for entry in added_entries:
            self.data[entry.key] = entry
        return added_entries if not return_count_only else len(added_entries)

    def update_MRU(self, entry, value):
        evicted_items = self.generations[entry.cffi_entry.r_parent].update_MRU(entry, value)
        for k, _ in evicted_items:
            del self.data[k]

    def on_hit(self, entry):
        self.generations[entry.cffi_entry.r_parent].on_hit(entry)

    @property
    def size(self):
        return self.eden.size + self.protected.size + self.probation.size

    @property
    def weight(self):
        return self.size

    def __len__(self):
        return len(self.eden) + len(self.protected) + len(self.probation)

    def stats(self):
        return {
            'eden_stats': self.eden.stats(),
            'prot_stats': self.protected.stats(),
            'prob_stats': self.probation.stats(),
        }

    def entries(self):
        return getattr(self.data, 'itervalues', self.data.values)()


@interface.implementer(ILRUEntry)
class CacheRingEntry(object):
    """
    The Python-level objects holding the Python-level key and value.
    """

    __slots__ = (
        'key', 'value', 'weight',
        'cffi_ring_node', 'cffi_ring_handle',
        'cffi_entry',
        # This is an owning pointer that is allocated when we
        # are imported from a persistent file. It keeps a whole array alive
        '_cffi_owning_node'
    )

    def __init__(self, key, value, weight, node=None):
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
        # by the C code).
        self.weight = entry.weight = weight

    def reset(self, key, value, weight):
        self.key = key
        self.value = value
        entry = self.cffi_entry
        entry.frequency = 1
        self.weight = entry.weight = weight

    def reset_for_free_list(self):
        """
        Put this node into an invalid state, representing that it
        should not be in a ring, but just the free list.

        You must call `reset` to use this node again.
        """
        self.key = self.value = self.weight = None
        self.cffi_entry.r_parent = 0 # make sure we can't dereference a generation

    frequency = property(lambda self: self.cffi_entry.frequency,
                         lambda self, nv: setattr(self.cffi_entry, 'frequency', nv))

    def set_value(self, value, weight):
        if value == self.value:
            return
        self.value = value
        self.weight = self.cffi_entry.weight = weight

    # We don't implement __len__---we want people to access .len
    # directly to avoid the function call as it showed up in benchmarks

    def __repr__(self):
        return ("<%s key=%r f=%d size=%d>" %
                (type(self).__name__, self.key, self.frequency, self.weight))

def _mutates_free_list(func):
    @functools.wraps(func)
    def mutates(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        finally:
            self._mutated_free_list = True
            # Now replace ourself with a "bound function" on the instance
            # so our overhead somewhat goes away
            setattr(self, func.__name__, lambda *args, **kwargs: func(self, *args, **kwargs))

    return mutates

@interface.implementer(IGeneration)
class Generation(object):

    # For the bulk insertion method add_MRUs in the eden generation, we need
    # to know whether or not the node_free_list we have is still the original
    # contiguous array that can be passed to C.
    _mutated_free_list = False

    # The CFFI pointer to the RSCache structure. It should be shared
    # among all the rings of the cache.
    cffi_cache = None

    # The list of free CacheRingNode objects. It should be shared
    # among all the rings of a cache.
    node_free_list = ()

    PARENT_CONST = 0

    def __init__(self, limit,
                 cffi_cache,
                 key_weight=len, value_weight=len):

        self.limit = limit
        self.key_weight = key_weight
        self.value_weight = value_weight
        self.cffi_cache = cffi_cache
        node = self.ring_home = ffi.new("RSRing")
        node.r_next = node
        node.r_prev = node
        node.u.head.max_weight = limit
        node.u.head.generation = self.PARENT_CONST
        self.node_free_list = []

    def init_node_free_list(self, entry_count):
        assert not self.node_free_list
        assert not self._mutated_free_list
        keys_and_values = itertools.repeat(('', (b'', 0)), entry_count)
        _, nodes = self._preallocate_entries(keys_and_values, entry_count)
        self.node_free_list.extend(nodes)
        return self.node_free_list

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
        key_weight = self.key_weight
        value_weight = self.value_weight
        for i, (k, v) in enumerate(ordered_keys_and_values):
            node = nodes + i # pointer arithmetic gets RSRingNode*; nodes[i] returns the struct
            weight = key_weight(k) + value_weight(v)
            entry = CacheRingEntry(k, v, weight, node)
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
        weight = self.key_weight(key) + self.value_weight(value)
        if node_free_list:
            new_entry = node_free_list.pop()
            new_entry.reset(key, value, weight)
        else:
            new_entry = CacheRingEntry(key, value, weight)

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
        old_size = entry.weight
        new_size = self.key_weight(entry.key) + self.value_weight(value)
        entry.set_value(value, new_size)

        if old_size == new_size:
            # Treat it as a simple hit; nothing could get evicted.
            self.on_hit(entry)
            return ()

        evicted_ring = _lru_update_mru(self.cffi_cache,
                                       self.ring_home,
                                       entry.cffi_ring_node,
                                       old_size, new_size)

        if not evicted_ring.r_next:
            # Nothing rejected.
            return ()

        node = evicted_ring.r_next
        evicted_items = []
        node_free_list = self.node_free_list
        while node:
            old_entry = ffi_from_handle(node.user_data)
            evicted_items.append((old_entry.key, old_entry.value))
            old_entry.reset_for_free_list()
            node_free_list.append(old_entry)

            node = node.r_next
        return evicted_items

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


class Eden(Generation):
    __name__ = 'eden'
    PARENT_CONST = generation_number = 1

    @_mutates_free_list
    def add_MRUs(self, ordered_keys_and_values, total_count=None):
        """
        Returns a sequence of added entries.

        You *must* keep the objects in the sequence alive while they remain in
        the ring, until they are explicitly removed or evicted.
        """
        # ordered_keys_and_values may be a generator, in which case you
        # must provide total_count. Beware, though: if you provide many, many
        # more values than can fit, you can find up allocating a large
        # ring array that's mostly unused.
        # TODO: Stop pre-allocating at creation time, only do so now.
        if total_count is None:
            total_count = len(ordered_keys_and_values)

        if not total_count:
            return ()
        # Start by using existing entries *if* we haven't mutated the free list
        # (Because the C code needs contiguous data)
        if not self._mutated_free_list and self.node_free_list:
            self._mutated_free_list = True
            # Take the number of entries out of the free list and
            # pair them up with keys/values
            entries = self.node_free_list[:total_count]
            nodes = entries[0]._cffi_owning_node
            del self.node_free_list[:total_count]
            key_weight = self.key_weight
            value_weight = self.value_weight

            ordered_keys_and_values_iter = iter(ordered_keys_and_values)
            for entry, (k, v) in izip(entries, ordered_keys_and_values_iter):
                weight = key_weight(k) + value_weight(v)
                entry.reset(k, v, weight)

            # Move the freelist nodes into the ring. Anything that
            # doesn't fit is moved back onto the freelist
            added_entries = self.__add_MRUs(nodes, entries)
            if len(added_entries) < len(entries):
                # We had no room, stop looking at the data,
                # which could be a generator.
                # XXX: If we didn't actually consume all the
                # entries we took off the list, and we had more entries
                # than we needed in the free list, which was contiguous,
                # then we wind up with a gap of unused memory in the array.
                # The `entry` objects are now going to get GC'd
                return added_entries


            # Anything left over couldn't fit on the freelist. But we did
            # fit in the cache, so keep trying.
            added_entries.extend(self.add_MRUs(ordered_keys_and_values_iter,
                                               total_count=total_count - len(added_entries)))

            return added_entries

        nodes, entries = self._preallocate_entries(ordered_keys_and_values, total_count)
        return self.__add_MRUs(nodes, entries)

    def __add_MRUs(self, nodes, entries):
        number_nodes = len(entries)
        # Only return the objects we added, allowing the rest to become garbage.
        # Bulk addition like this will never evict existing items.

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
        """
        Returns ``(added_entry, (evicted_key, evicted_value))``

        You *must* keep the ``added_entry`` object alive while it
        remains in the ring, until it is explicitly removed or
        it is evicted.
        """
        node_free_list = self.node_free_list
        weight = self.key_weight(key) + self.value_weight(value)
        if node_free_list:
            new_entry = node_free_list.pop()
            new_entry.reset(key, value, weight)
        else:
            new_entry = CacheRingEntry(key, value, weight)

        evicted_ring = _eden_add(self.cffi_cache,
                                 new_entry.cffi_ring_node)

        if not evicted_ring.r_next:
            # Nothing rejected.
            return new_entry, ()

        node = evicted_ring.r_next
        evicted_items = []
        while node:
            old_entry = ffi_from_handle(node.user_data)

            evicted_items.append((old_entry.key, old_entry.value))
            # TODO: Should we avoid this if _cffi_owning_node is set?
            # To allow that big array to get GC'd sooner
            old_entry.reset_for_free_list()
            node_free_list.append(old_entry)
            node = node.r_next
        return new_entry, evicted_items

class Protected(Generation):
    __name__ = 'protected'
    PARENT_CONST = generation_number = 2


class Probation(Generation):
    __name__ = 'probation'
    PARENT_CONST = generation_number = 3

    def on_hit(self, entry):
        # Move the entry to the protected LRU on its very first hit, where
        # it becomes the MRU.
        _lru_probation_on_hit(self.cffi_cache, entry.cffi_ring_node)
