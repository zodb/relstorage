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


from transaction.interfaces import TransientError
from ZODB.POSException import StorageError

# Export
from relstorage._compat import MAX_TID # pylint:disable=unused-import

# pylint: disable=inherit-non-class,no-method-argument,no-self-argument
# pylint:disable=unexpected-special-method-signature
# pylint:disable=signature-differs


class IStateCache(Interface):
    """
    The methods we use to store state information.

    This interface is defined in terms of OID and TID *integers*;
    implementations (such as memcache) that only support string
    keys will need to convert.

    All return values for states return ``(state_bytes, tid_int)``.

    We use special methods where possible because those are slightly
    faster to invoke.
    """

    def __getitem__(oid_tid):
        """
        Given an (oid, tid) pair, return the cache data (state_bytes,
        tid_int) for that object.

        The returned *tid_int* must match the requested tid.

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
        Store the *state_bytes_tid* (``(state_bytes, tid_int)``) for
        the ``(oid, tid)`` pair.

        Note that it does not necessarily mean that the key tid
        matches the value tid.

        Also note that if the object has been deleted, ``state_bytes``
        may be `None`.
        """

    def __delitem__(oid_tid):
        """
        Remove the data cached for the oid/tid pair.

        If no data is cached, this should do nothing.
        """

    def set_all_for_tid(tid_int, state_oid_iter):
        """
        Store the states for the ``(state, oid_int)`` pairs under
        ``(oid_int, tid_int)`` keys.

        The *state_oid_iter* is an **iteable** of ``(state, oid_int,
        prev_tid_int)`` pairs; this method may choose to buffer a
        limited amount of those pairs before passing them on to the
        underlying storage in a bulk group.

        As an **iterable**, *state_oid_iter* may be consumed multiple
        times.
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

    ##
    # Methods that are here to assist with tracing.
    ##

    def updating_delta_map(oid_tid_map):
        """
        Return a new map that can (temporarily) observe set actions.
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

    def zap_all():
        """
        Remove the cache from disk.
        """

class ILRUEntry(Interface):
    """
    An entry in an `ILRUCache`.

    This is a read-only object. The containing cache
    is in charge of all the attributes, and they must not
    be changed behind its back.

    """

    key = Attribute("The key for the entry")
    value = Attribute("The value for the entry")
    frequency = Attribute("The frequency of accesses to the entry.")
    weight = Attribute("The weight of the entry.")

class ILRUCache(Interface):
    """
    A container of cached keys and values and associated metadata,
    limited to containing a total weight less than some limit.

    The interface is mapping-like, and specified in terms of keys and
    values. For access to the additional stored metadata, different
    methods are defined.

    Values may be evicted when new ones are added.

    The cache may not store None values.

    The cache may have specific restrictions on the type and format
    of keys and values it accepts.
    """

    limit = Attribute("The maximim weight allowed.")
    weight = Attribute("The weight of the entries in the cache.")

    # Mapping-like methods.

    def __getitem__(key):
        """
        Get a value by key. If there is no value
        for the key, or it has been evicted, return None.

        This should be considered a hit on the key.

        This never results in raising a `KeyError`.
        """

    def __len__():
        """
        Count how many entries are in the cache.
        """
    def __contains__(key):
        "Is the key in the cache?"

    def __iter__():
        "Iterate the keys"

    def __setitem__(key, value):
        """
        Either set or update an entry.

        If the key already existed in the cache, then update its
        ``value`` to the new *value* and mark it as the most recently
        used.

        Otherwise, create a new entry for the key, setting it to the
        most recently used.

        This may evict other items.
        """

    def __delitem__(key):
        """
        Remove the entry from the cache.

        If it does not exist, it is an error.
        """

    ###
    # Cache-specific operations.
    ###

    def get_from_key_or_backup_key(pref_key, backup_key):
        """
        Get a value stored at either *pref_key* or, failing that,
        *backup_key*.

        *backup_key* may be None if there is no second key.

        If a value is found at *backup_key*, then it is
        moved to be stored at *pref_key*, while retaining its
        frequency information.

        Counts as  a hit on whichever key matches.

        This is used to implement ``IStateCache.__call__``.
        """

    def peek(key):
        """
        Similar to ``__getitem__``, but *does not* count
        as a hit on the key, merely returns a value if its present.
        """

    def entries():
        """
        Iterate all the `ILRUEntry` values.
        """

    def stats():
        """
        Return info about the cache.
        """

    def add_MRU(key, value):
        """
        Insert a new item in the cache, as the most recently used.

        Returns the new entry, and any item pairs that had to be evicted
        to make room.
        """

    def add_MRUs(ordered_keys_and_values, return_count_only=False):
        """
        Add as many of the key/value pairs in *ordered_keys_and_values* as possible,
        without evicting any existing items.

        Returns the entries that were added, unless *return_count_only* is given,
        in which case it returns the count added instead. (This can save memory
        if the entries are not actually needed.)
        """

    def age_frequencies():
        """Call to periodically adjust the frequencies of items."""

class IGeneration(Interface):
    """
    A generation in a cache.
    """
    limit = Attribute("The maximim weight allowed.")
    __name__ = Attribute("The name of the generation")
    generation_number = Attribute("The number of the generation.")

    def __iter__():
        """
        Iterate the ILRUEntry objects in this generation,
        from most recent to least recently used.
        """

class IGenerationalLRUCache(ILRUCache):
    """
    The cache moves items between three generations, as determined by
    an admittance policy.

    * Items begin in *eden*, where they stay until eden grows too
      large.

    * When eden grows too large, the least recently used item is then
      (conceptually) moved to the *probation* ring. If this would make
      the probation ring too large, the *frequency* of the least
      recently used item from the probation ring is compared to the
      frequency of the incoming item. Only if the incoming item is more
      popular than the item it would force off the probation ring is it
      kept (and the probation item removed). Otherwise the eden item is
      removed.

    * When an item in probation is accessed, it is moved to the
      *protected* ring. The protected ring is the largest ring. When
      adding an item to it would make it too large, the least recently
      used item is demoted to probation, following the same rules as for
      eden.

    This cache only approximately follows its size limit. It may
    temporarily become larger.
    """

    eden = Attribute("The youngest generation.")
    protected = Attribute("The protected generation.")
    probation = Attribute("The probation generation.")
    generations = Attribute("Ordered list of generations, with 0 being NoSuchGeneration.")


class CacheCorruptedError(StorageError):
    """
    Raised when we detect internal cache corruption,
    outside of any particular transaction.
    """

class CacheConsistencyError(StorageError, TransientError):
    """
    Raised when we detect internal cache corruption, having to do with
    transactional consistency.

    This is probably a transient error. By the time it is raised, the
    faulty cache will have been cleared, and we can try again.
    """

class NoSuchGeneration(object):
    # For more specific error messages; if we get an AttributeError
    # on this object, it means we have corrupted the cache. We only
    # expect to wind up with this in generation 0.
    __name__ = 'NoSuchGeneration'
    def __init__(self, generation_number):
        self.__gen_num = generation_number

    def __getattr__(self, name):
        msg = "Generation %s has no attribute %r" % (self.__gen_num, name)
        raise AttributeError(msg)

class GenerationalCacheBase(object):
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
    #: ring that holds it. (Indexing by ints is faster than a dictionary lookup
    #: especially on PyPy.) Initialize to an object that will report a bad lookup
    #: for the right generation. (The generator expression keeps us from leaking the
    #: indexing variable on Py2.)
    generations = tuple((NoSuchGeneration(i) for i in range(4)))

    def __init__(self, limit, eden, protected, probation):
        self.limit = limit
        self.eden = eden
        self.protected = protected
        self.probation = probation
        # Preserve the NoSuchGeneration initializers
        generations = list(GenerationalCacheBase.generations)
        for gen in (self.protected, self.probation, self.eden):
            generations[gen.generation_number] = gen
        self.generations = tuple(generations)
