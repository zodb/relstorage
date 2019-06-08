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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import Attribute
from zope.interface import Interface

import BTrees

from relstorage._compat import PYPY

# pylint: disable=inherit-non-class,no-method-argument,no-self-argument
# pylint:disable=unexpected-special-method-signature
# pylint:disable=signature-differs

# An LLBTree uses much less memory than a dict, and is still plenty fast on CPython;
# it's just as big and slower on PyPy, though.
OID_TID_MAP_TYPE = BTrees.family64.II.BTree if not PYPY else dict
OID_OBJECT_MAP_TYPE = BTrees.family64.IO.BTree if not PYPY else dict
MAX_TID = BTrees.family64.maxint

class IStateCache(Interface):
    """
    The methods we use to store state information.

    This interface is defined in terms of OID and TID *integers*;
    implementations (such as memcache) that only support string
    keys will need to convert.

    All return values for states return (state_bytes, tid_int).

    We use special methods where possible because those are slightly
    faster to invoke.
    """

    def __getitem__(oid_tid):
        """
        Given an (oid, tid) pair, return the cache data (state_bytes,
        tid_int) for that object.

        The returned *tid_int* must match the tid in the key.

        If the (oid, tid) pair isn't in the cache, return None.
        """

    def __call__(oid, tid1, tid2):
        """
        The same as invoking `__getitem__((oid, tid1))` followed by
        `__getitem__((oid, tid2))` if no result was found for the first one.

        If no result is found for *tid1*, but a result is found for *tid2*,
        then this method should cache the result at (oid, tid1) before returning.
        """

    def __setitem__(oid_tid, state_bytes_tid):
        """
        Store the *state_bytes* for the (oid, tid) pair.

        Note that it does not necessarily mean that the key tid
        matches the value tid.
        """

    def set_multi(keys_and_values):
        """
        Given a mapping from keys to values, set them all.
        """

    def store_checkpoints(cp0_tid, cp1_tid):
        """
        Store the suggested pair of checkpoints.
        """

    def get_checkpoints():
        """
        Return the current checkpoints as (cp0_tid, cp1_tid).

        If not found, return None.
        """

    def close():
        """
        Release external resources held by this object.
        """

    def flush_all():
        """
        Clear cached data.
        """

class IPersistentCache(Interface):
    """
    A cache that can be persisted to a location on disk
    and later re-populated from that same location.
    """

    size = Attribute("The byte-size of the entries in the cache.")
    limit = Attribute("The upper bound of the byte-size that this cache should hold.")

    def save():
        """
        Save the cache to disk.
        """

    def restore():
        """
        Restore the cache from disk.
        """

class ILRUItem(Interface):
    """
    An entry in an `ILRUCache`.

    Keys and values must not be changed; the frequency
    can be increased.
    """

    key = Attribute("The key for the entry")
    value = Attribute("The value for the entry")
    frequency = Attribute("The frequency of accesses to the entry.")
    weight = Attribute("The weight of the entry.")

class ILRUCache(Interface):
    """
    A container of cached keys and values and associated metadata,
    limited to containing a total weight less than some limit.

    Values may be evicted when new ones are added.
    """

    # TODO: This is a mix of user-level (key/value) and implementation
    # level (ILRUItem); make the separation much more clear.

    limit = Attribute("The maximim weight allowed.")
    weight = Attribute("The weight of the entries in the cache.")

    def __len__():
        """
        Count how many entries are in the cache.
        """

    def stats():
        """
        Return info about the cache.
        """

    def update_MRU(entry, value):
        """
        Given an entry that is known to be in the cache, update its
        ``value`` to the new *value* and mark it as the most recently used.

        Because the value's weight may have changed, this may evict other items.
        If so, they are returned as ``[(key, value)]``.
        """

    def add_MRU(key, value):
        """
        Insert a new item in the cache, as the most recently used.

        Returns the entry, and any items that had to be evicted to make room.
        """

    def add_MRUs(ordered_keys_and_values):
        """
        Add as many of the key/value pairs in *ordered_keys_and_values* as possible,
        without evicting any existing items.

        Returns the entries that were added.
        """

    def age_frequencies():
        """Call to periodically adjust the frequencies of items."""

    def on_hit(entry):
        """
        Notice that the entry is being accessed and adjust its frequency
        and move any items around in the cache as necessary.
        """

    def itervalues():
        """
        Iterate all the ILRUItem values.
        """

    def get(key): # pylint:disable=arguments-differ
        """
        Get an item by key.

        This should not be considered a hit.        """

    def __contains__(key):
        "Is the key in the cache?"

    def __iter__():
        "Iterate the keys"

    def __setitem__(key, value):
        """
        Either set or update an entry.
        This is like either ``update_MRU`` or ``add_MRU``,
        depending on whether an entry exists.

        This may evict items.
        """

    def __delitem__(key):
        """
        Remove the entry from the cache.
        """

class CacheCorruptedError(AssertionError):
    """
    Raised when we detect cache corruption.
    """
