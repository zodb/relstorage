# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2015 Zope Foundation and Contributors.
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

#pylint: disable=W0212,E0211,W0622,E0213,W0221,E0239

from zope.interface import Interface
from zope.interface import implementer

class IRing(Interface):
    """Conceptually, a doubly-linked list for efficiently keeping track of least-
    and most-recently used :class:`persistent.interfaces.IPersistent` objects.

    This is meant to be used by the :class:`persistent.picklecache.PickleCache`
    and should not be considered a public API. This interface documentation exists
    to assist development of the picklecache and alternate implementations by
    explaining assumptions and performance requirements.
    """

    def __len__():
        """Return the number of persistent objects stored in the ring.

        Should be constant time.
        """

    def __contains__(object):
        """Answer whether the given persistent object is found in the ring.

        Must not rely on object equality or object hashing, but only
        identity or the `_p_oid`. Should be constant time.
        """

    def add(object):
        """Add the persistent object to the ring as most-recently used.

        When an object is in the ring, the ring holds a strong
        reference to it so it can be deactivated later by the pickle
        cache. Should be constant time.

        The object should not already be in the ring, but this is not necessarily
        enforced.
		"""

    def delete(object):
        """Remove the object from the ring if it is present.

        Returns a true value if it was present and a false value
        otherwise. An ideal implementation should be constant time,
        but linear time is allowed.
        """

    def move_to_head(object):
        """Place the object as the most recently used object in the ring.

        The object should already be in the ring, but this is not
        necessarily enforced, and attempting to move an object that is
        not in the ring has undefined consequences. An ideal
        implementation should be constant time, but linear time is
        allowed.
        """

    def delete_all(indexes_and_values):
        """Given a sequence of pairs (index, object), remove all of them from
        the ring.

        This should be equivalent to calling :meth:`delete` for each
        value, but allows for a more efficient bulk deletion process.

        If the index and object pairs do not match with the actual state of the
        ring, this operation is undefined.

        Should be at least linear time (not quadratic).
        """

    def __iter__():
        """Iterate over each persistent object in the ring, in the order of least
        recently used to most recently used.

        Mutating the ring while an iteration is in progress has
        undefined consequences.
        """

from collections import deque

@implementer(IRing)
class _DequeRing(object):
    """A ring backed by the :class:`collections.deque` class.

    Operations are a mix of constant and linear time.

    It is available on all platforms.
    """

    __slots__ = ('ring', 'ring_oids')

    def __init__(self):

        self.ring = deque()
        self.ring_oids = set()

    def __len__(self):
        return len(self.ring)

    def __contains__(self, pobj):
        return pobj.key in self.ring_oids

    def add(self, pobj):
        self.ring.append(pobj)
        self.ring_oids.add(pobj.key)

    def delete(self, entry):
        try:
            self.ring.remove(entry)
        except ValueError:
            raise KeyError("%r not in the ring" % entry)
        self.ring_oids.discard(entry.key)
        return 1

    def move_to_head(self, pobj):
        self.delete(pobj)
        self.add(pobj)

    def delete_all(self, indexes_and_values):
        for ix, value in reversed(indexes_and_values):
            del self.ring[ix]
            self.ring_oids.discard(value.key)

    def __iter__(self):
        return iter(self.ring)


try:
    from cffi import FFI
except ImportError: # pragma: no cover
    _CFFIRing = None
else:

    import os
    this_dir = os.path.dirname(os.path.abspath(__file__))

    ffi = FFI()
    with open(os.path.join(this_dir, 'ring.h')) as f:
        ffi.cdef(f.read())

    _FFI_RING = ffi.verify("""
    #include "ring.c"
    """, include_dirs=[this_dir])

    _ring_move_to_head = _FFI_RING.ring_move_to_head
    _ring_del = _FFI_RING.ring_del
    _ring_add = _FFI_RING.ring_add
    ffi_new = ffi.new
    ffi_new_handle = ffi.new_handle
    ffi_from_handle = ffi.from_handle

    #pylint: disable=E1101
    @implementer(IRing)
    class _CFFIRing(object):
        """
        A ring backed by a C implementation. All operations are constant time.

        It is only available on platforms with ``cffi`` installed.

        You must keep the entries alive! Otherwise memory will be freed.
        """

        def __init__(self, ring_type='RSRing', ffi=ffi):
            self.ring_type = ring_type
            node = self.ring_home = ffi.new(self.ring_type)
            node.r_next = node
            node.r_prev = node

        def __len__(self):
            return self.ring_home.len

        def __contains__(self, k):
            import warnings
            warnings.warn("Contains is linear", stacklevel=2)
            return k in list(self)

        def add(self, entry):
            node = entry.cffi_ring_node
            if node is None:
                handle = ffi_new_handle(entry)
                entry.cffi_ring_handle = handle
                node = ffi_new(self.ring_type,
                               {'user_data': handle})
                entry.cffi_ring_node = node
            assert node.user_data
            return _ring_add(self.ring_home, node)
            #self.ring_home.len += 1

        def delete(self, pobj):
            if not self.ring_home.len:
                raise KeyError("No items in ring %r" % self)
            its_node = pobj.cffi_ring_node
            #if its_node.r_next: # Don't do if null
            return _ring_del(self.ring_home, its_node)

        def move_to_head(self, entry):
            _ring_move_to_head(self.ring_home, entry.cffi_ring_node)

        def delete_all(self, indexes_and_values):
            for _, value in indexes_and_values:
                self.delete(value)

        def iteritems(self):
            head = self.ring_home
            here = head.r_next
            while here != head:
                yield here
                here = here.r_next

        def __iter__(self):
            for node in self.iteritems():
                yield ffi_from_handle(node.user_data)

        def lru(self):
            return ffi_from_handle(self.ring_home.r_next.user_data)




# Export the best available implementation
Ring = _CFFIRing if _CFFIRing else _DequeRing
