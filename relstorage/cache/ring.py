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
        return pobj._p_oid in self.ring_oids

    def add(self, pobj):
        self.ring.append(pobj)
        self.ring_oids.add(pobj._p_oid)

    def delete(self, pobj):
        # Note that we do not use self.ring.remove() because that
        # uses equality semantics and we don't want to call the persistent
        # object's __eq__ method (which might wake it up just after we
        # tried to ghost it)
        for i, o in enumerate(self.ring):
            if o is pobj:
                del self.ring[i]
                self.ring_oids.discard(pobj._p_oid)
                return 1

    def move_to_head(self, pobj):
        self.delete(pobj)
        self.add(pobj)

    def delete_all(self, indexes_and_values):
        for ix, value in reversed(indexes_and_values):
            del self.ring[ix]
            self.ring_oids.discard(value._p_oid)

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

    _OGA = object.__getattribute__
    _OSA = object.__setattr__

    #pylint: disable=E1101
    @implementer(IRing)
    class _CFFIRing(object):
        """A ring backed by a C implementation. All operations are constant time.

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
            return getattr(pobj, '_Persistent__ring', self) in self.ring_to_obj

        def add(self, pobj):
            node = ffi.new("CPersistentRing*")
            _FFI_RING.ring_add(self.ring_home, node)
            self.ring_to_obj[node] = pobj
            _OSA(pobj, '_Persistent__ring', node)

        def delete(self, pobj):
            its_node = getattr(pobj, '_Persistent__ring', None)
            our_obj = self.ring_to_obj.pop(its_node, None)
            if its_node is not None and our_obj is not None and its_node.r_next:
                _FFI_RING.ring_del(its_node)
                return 1

        def move_to_head(self, pobj):
            node = _OGA(pobj, '_Persistent__ring')
            _FFI_RING.ring_move_to_head(self.ring_home, node)

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

# Export the best available implementation
Ring = _CFFIRing if _CFFIRing else _DequeRing
