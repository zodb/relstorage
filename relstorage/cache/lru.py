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
_ring_move_to_head_from_foreign = _FFI_RING.ring_move_to_head_from_foreign
_lru_probation_on_hit = _FFI_RING.lru_probation_on_hit


class SizedLRURingEntry(object):

    __slots__ = ('key', 'value',
                 'cffi_ring_node', 'cffi_ring_handle')

    def __init__(self, key, value, parent):
        self.key = key
        self.value = value
        #self.__parent__ = parent
        self.cffi_ring_handle = ffi_new_handle(self)
        self.cffi_ring_node = ffi_new('CPersistentRing*',
                                      {'len': len(key) + len(value),
                                       'user_data': self.cffi_ring_handle,
                                       'frequency': 1,
                                       'r_parent': parent.cffi_handle})
    @property
    def __parent__(self):
        return ffi_from_handle(self.cffi_ring_node.r_parent)

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
        self.cffi_handle = ffi_new_handle(self)
        self._ring = Ring()
        self._ring.ring_home.max_len = limit
        self._ring.ring_home.r_parent = self.cffi_handle

        self.get_LRU = self._ring.lru
        self.make_MRU = self._ring.move_to_head
        self.remove = self.delete
        self.over_size = False

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

    def add_MRU(self, key, value):
        entry = SizedLRURingEntry(key, value, self)
        self.over_size = self._ring.add(entry)
        #self.size += entry.len
        #entry.frequency += 1
        return entry

    def take_ownership_of_entry_MRU(self, entry):
        #assert entry.__parent__ is None
        old_parent = entry.__parent__

        # But don't increment here, we're just moving
        # from one ring to another
        #entry.__parent__ = self
        self.over_size = _ring_move_to_head_from_foreign(old_parent._ring.ring_home,
                                                         self._ring.ring_home,
                                                         entry.cffi_ring_node)

        old_parent.over_size = old_parent.size > old_parent.limit


    def update_MRU(self, entry, value):
        #assert entry.__parent__ is self
        old_size = entry.len
        entry.set_value(value)
        new_size = entry.len
        self.over_size = _lru_update_mru(self._ring.ring_home, entry.cffi_ring_node, old_size, new_size)

    def delete(self, entry):
        self._ring.delete(entry)
        self.over_size = self.size > self.limit

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


class EdenLRU(SizedLRU):

    def __init__(self, limit, probation_lru, protected_lru, entry_dict):
        SizedLRU.__init__(self, limit)
        self.probation_lru = probation_lru
        self.protected_lru = protected_lru
        self.entry_dict = entry_dict

    def add_MRU(self, key, value):
        new_entry = SizedLRU.add_MRU(self, key, value)

        if not self.over_size:
            return new_entry

        dct = self.entry_dict
        probation_lru = self.probation_lru
        protected_lru = self.protected_lru

        if not probation_lru and not protected_lru.over_size:
            # This is a modification of the algorithm. When we start out
            # go ahead and populate the protected_lru directly
            # from eden; only when its full do we start doing the probationary
            # dance. This helps mitigate any issues with choosing segment sizes;
            # we're going to occupy all the memory anyway, why not, it's reserved for us,
            # so go ahead and fill it.

            while self.over_size:
                eden_oldest = self.get_LRU()
                if eden_oldest.key is key:
                    break

                if eden_oldest.len + protected_lru.size > protected_lru.limit:
                    # This would oversize protected. Move it to probation instead,
                    # which is currently empty, so there's no need to choose a victim.
                    # This may temporarily oversize us.
                    probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                    break
                else:
                    protected_lru.take_ownership_of_entry_MRU(eden_oldest)

            return new_entry

        while self.over_size:
            eden_oldest = self.get_LRU()
            if eden_oldest.key is key:
                break

            #assert eden_oldest.__parent__ is None
            #assert eden_oldest._Persistent__ring is None

            if probation_lru.size + eden_oldest.len < probation_lru.limit:
                # Cool, we can keep it.
                probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                #assert eden_oldest.__parent__ is probation_lru
            else:
                # Snap, somebody has to go.
                try:
                    oldest_main_ring = probation_lru.get_LRU()
                except (StopIteration, KeyError):
                    # probation ring is empty, nothing to eject. This must be a large
                    # item. Well, just accept it then to match what we used to do.
                    probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                    continue

                if oldest_main_ring.frequency > eden_oldest.frequency:
                    # Discard this entry, it loses
                    # print("Completely evicting item", oldest.key,
                    #       "because main ring item", oldest_main_ring.key,
                    #       "has better frequency", oldest_main_ring.frequency, oldest.frequency)
                    self._ring.delete(eden_oldest)
                    del dct[eden_oldest.key]
                else:
                    # eden item is more popular, keep it
                    probation_lru.remove(oldest_main_ring)
                    del dct[oldest_main_ring.key]

                    probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                    #assert eden_oldest.__parent__ is probation_lru
            self.over_size = self.size > self.limit
        return new_entry

class ProtectedLRU(SizedLRU):
    pass


class ProbationLRU(SizedLRU):

    promote_count = 0
    demote_count = 0
    remove_count = 0

    def __init__(self, limit, protected_lru, entry_dict):
        SizedLRU.__init__(self, limit)
        self.protected_lru = protected_lru
        self.entry_dict = entry_dict

    def on_hit(self, entry):
        # Move the entry to the Protected LRU on its very first hit;
        # It will become the MRU there.
        protected_lru = self.protected_lru
        protected_lru.over_size = _lru_probation_on_hit(self._ring.ring_home,
                                                        protected_lru._ring.ring_home,
                                                        entry.cffi_ring_node)
