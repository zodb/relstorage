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

class LRURingEntry(object):

    # _p_oid is what the persistent.ring.Ring implementations use as their
    # key.
    # _Persistent__ring is a private implementation detail---the CFFI
    # version has to maintain a reference to the C Node structure and it uses
    # this.
    # value is self-explanatory.
    __slots__ = ('key', 'cffi_ring_node',
                 'value', '__parent__', 'frequency',
                 'len')

    def __init__(self, key, value, parent):
        self.key = key
        self.value = value
        self.len = len(key) + len(value)
        self.frequency = 0
        self.__parent__ = parent
        self.cffi_ring_node = None

    def set_value(self, value):
        self.value = value
        self.len = len(self.key) + len(value)

    def __len__(self):
        return self.len

    def __repr__(self):
        return ("<%s key=%r f=%d size=%d>" %
                (type(self).__name__, self._p_oid, self.frequency, self.len))

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
    _ring_move_to_head_from_foreign = _FFI_RING.ring_move_to_head_from_foreign
    _ring_del = _FFI_RING.ring_del
    _ring_add = _FFI_RING.ring_add
    ffi_new = ffi.new

    #pylint: disable=E1101
    @implementer(IRing)
    class _CFFIRing(object):
        """
        A ring backed by a C implementation. All operations are constant time.

        It is only available on platforms with ``cffi`` installed.
        """

        __slots__ = ('ring_home', 'ring_to_obj')

        def __init__(self):
            node = self.ring_home = ffi.new("CPersistentRing*")
            node.r_next = node
            node.r_prev = node

            # In order for the CFFI objects to stay alive, we must keep
            # a strong reference to them, otherwise they get freed. We must
            # also keep strong references to the objects so they can be deactivated
            self.ring_to_obj = dict()

        def __len__(self):
            return len(self.ring_to_obj)

        def __contains__(self, pobj):
            return getattr(pobj, 'cffi_ring_node', self) in self.ring_to_obj

        def add(self, pobj):
            node = ffi_new("CPersistentRing*")
            _ring_add(self.ring_home, node)
            self.ring_to_obj[node] = pobj
            pobj.cffi_ring_node = node

        def delete(self, pobj):
            its_node = pobj.cffi_ring_node
            del self.ring_to_obj[its_node]
            _ring_del(its_node)
            return 1

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
            ring_to_obj = self.ring_to_obj
            for node in self.iteritems():
                yield ring_to_obj[node]

        def lru(self):
            return self.ring_to_obj[self.ring_home.r_next]

        def next_mru_to(self, entry):
            """
            Return the object that is the *next* most recently used, compared
            to the given entry.
            """
            return self.ring_to_obj[entry.cffi_ring_node.r_prev]

        def next_lru_to(self, entry):
            """
            Return the object that is the *next* least recently used, compared
            to the given entry.
            """
            return self.ring_to_obj[entry.cffi_ring_node.r_next]

        def move_entry_from_other_ring(self, entry, other_ring):
            node = entry.cffi_ring_node
            assert node is not None

            #other_ring.delete(entry)

            _ring_move_to_head_from_foreign(self.ring_home, node)
            self.ring_to_obj[node] = entry


# Export the best available implementation
Ring = _CFFIRing if _CFFIRing else _DequeRing
