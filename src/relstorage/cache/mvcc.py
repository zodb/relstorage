# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
MVCC tracking for cache objects.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading

from zope.interface import implementer

from relstorage._compat import iterkeys
from relstorage._compat import OID_TID_MAP_TYPE as OidTMap
from relstorage._compat import OidTMap_difference
from relstorage._compat import OidTMap_multiunion
from relstorage._compat import OidTMap_intersection
from relstorage._util import log_timed
from relstorage.options import Options

from .interfaces import IStorageCacheMVCCDatabaseCoordinator
from ._util import InvalidationMixin

logger = __import__('logging').getLogger(__name__)

def _debug(*_args):
    "Does nothing"
if 0: # pylint:disable=using-constant-test
    debug = print
else:
    debug = _debug

###
# Notes on in-process concurrency:
#
# Where possible, we rely on atomic primitive operations of fundamental types.
# For example, we rely on ``dict[key] = value`` being atomic.
# On CPython we use BTrees which are implemented in C and don't release the GIL
# because they're always resident, so this holds true. On PyPy we use
# native dicts and this also holds true. On CPython if we used PURE_PYTHON
# BTrees, this would *not* hold true, so we also use dicts.
###

class _TransactionRangeObjectIndex(OidTMap):
    """
    Holds the portion of the object index visible to transactions <=
    ``highest_visible_tid``.

    Initialized to be either empty, or with the *complete* index of
    objects <= ``highest_visible_tid`` and greater than
    ``complete_since_tid``. (That is, you cannot pass an 'ignore_txn'
    value to the poller. TODO: Workon that. Maybe there's a way.)

    ``highest_visible_tid`` must always be given, but
    ``complete_since_tid`` may be null.

    These attribute, once set, will never change. We may add data
    prior to and including ``complete_since_tid`` as we access it, but we have no
    guarantee that it is complete.

    When ``complete_since_tid`` and ``highest_visible_tid`` are the same
    """
    __slots__ = (
        'highest_visible_tid',
        'complete_since_tid',
        'accepts_writes',
    )

    # When the root node of a BTree splits (outgrows ``max_internal_size``),
    # it creates a new BTree object to be its child by calling ``type(self)()``
    # That doesn't work if you have required arguments.

    def __init__(self, highest_visible_tid=0, complete_since_tid=None, data=()):
        assert complete_since_tid is None or highest_visible_tid >= complete_since_tid
        self.highest_visible_tid = highest_visible_tid
        self.complete_since_tid = complete_since_tid
        self.accepts_writes = True

        OidTMap.__init__(self, data)

        if self:
            # Verify the data matches what they told us.
            # If we were constructed with data, we must be complete.
            # Otherwise we get built up bit by bit.
            assert self.complete_since_tid
            self.verify()
        else:
            # If we had no changes, then either we polled for the same tid
            # as we got, or we didn't try to poll for changes at all.
            assert complete_since_tid is None or complete_since_tid == highest_visible_tid

    def verify(self, initial=True):
        # Check that our constraints are met
        if not self or not __debug__:
            return

        max_stored_tid = self.max_stored_tid()
        min_stored_tid = self.min_stored_tid()
        hvt = self.highest_visible_tid
        assert max_stored_tid <= hvt, (max_stored_tid, hvt, self)
        assert min_stored_tid > 0, min_stored_tid
        if initial:
            # This is only true at startup. Over time we can add older entries.
            assert self.complete_since_tid is None or min_stored_tid > self.complete_since_tid, (
                min_stored_tid, self.complete_since_tid)

    def complete_to(self, newer_bucket):
        """
        Given an incomplete bucket (this object) and a possibly-complete bucket for the
        same or a later TID, merge this one to hold the same data and be complete
        for the same transaction range.

        This bucket will be complete for the given bucket's completion, *if* the
        given bucket actually had a different tid than this one. If the given
        bucket was the same tid, then nothing changed and we can't presume
        to be complete.
        """
        assert not self.complete_since_tid
        assert newer_bucket.highest_visible_tid >= self.highest_visible_tid
        self.update(newer_bucket)
        if newer_bucket.highest_visible_tid > self.highest_visible_tid:
            self.highest_visible_tid = newer_bucket.highest_visible_tid
            self.complete_since_tid = newer_bucket.complete_since_tid

    def merge_same_tid(self, bucket):
        """
        Given an incoming complete bucket for the same highest tid as this bucket,
        merge the two into this object.
        """
        assert bucket.highest_visible_tid == self.highest_visible_tid
        self.update(bucket)
        if bucket.complete_since_tid < self.complete_since_tid:
            self.complete_since_tid = bucket.complete_since_tid

    def merge_older_tid(self, bucket):
        """
        Given an incoming complete bucket for an older tid than this bucket,
        merge the two into this object.

        Because we're newer, entries in this bucket supercede objects
        in the incoming data.

        If the *bucket* was complete to a transaction earlier than the transaction
        we're complete to, we become complete to that earlier transaction.

        Does not modify the *bucket*.
        """
        assert bucket.highest_visible_tid <= self.highest_visible_tid
        #debug('Diff between self', dict(self), "and", dict(bucket), items_not_in_self)
        # bring missing data into ourself, being careful not to overwrite
        # things we do have.
        self.update(bucket.items_not_in(self))
        if bucket.complete_since_tid and bucket.complete_since_tid < self.complete_since_tid:
            self.complete_since_tid = bucket.complete_since_tid

    # These raise ValueError if the map is empty
    if not hasattr(OidTMap, 'maxKey'):
        maxKey = lambda self: max(iterkeys(self))

    if hasattr(OidTMap, 'itervalues'): # BTrees, or Python 2 dict
        def maxValue(self):
            return max(self.itervalues())
        def minValue(self):
            return min(self.itervalues())
    else:
        def maxValue(self):
            return max(self.values())
        def minValue(self):
            return min(self.values())

    if not hasattr(OidTMap, 'iteritems'):
        iteritems = OidTMap.items

    def items_not_in(self, other):
        """
        Return the ``(k, v)`` pairs in self whose ``k`` is not found in *other*
        """
        return OidTMap_difference(self, other)

    max_stored_tid = maxValue
    min_stored_tid = minValue

    def __repr__(self):
        return '<%s at 0x%x hvt=%s complete_after=%s len=%s readonly=%s>' % (
            self.__class__.__name__,
            id(self),
            self.highest_visible_tid,
            self.complete_since_tid,
            len(self),
            not self.accepts_writes,
        )


class _ObjectIndex(object):
    """
    Collectively holds all the object index visible to transactions <=
    ``highest_visible_tid``.

    The entries in ``maps`` attribute are instances of
    :class:`_TransactionRangeObjectIndex`, and our
    ``highest_visible_tid`` is exactly that of the frontmost map. For
    convenience, we also define this to be our
    ``maximum_highest_visible_tid``. The ``highest_visible_tid`` of
    the final map defines our ``minimum_highest_visible_tid``.

    The following describes the general evolution of this index and
    its MVCC strategy. Some of this is implemented here, but much of
    it is implemented in the :class:`MVCCDatabaseCoordinator`.

    .. rubric:: Initial State

    In the beginning, the frontmost map will be empty and incomplete;
    it will have a ``highest_visible_tid`` but no
    ``complete_since_tid``. (Alternately, if we restored from cache,
    we may have data, but we are definitely still incomplete.) We may
    load new object indices into this map, but only if they're less
    than what our ``highest_visible_tid`` is (this is just a special
    version of the general rule for updates laid out below).

    When a new poll takes place and produces a new change list, in the
    form of a :class:`_TransactionRangeObjectIndex`, it is merged with
    the previous map and replaced into this object. This map does have
    a ``complete_since_tid.``

    After this first map is added, moving forward there should be no
    gaps in ``complete_since_tid`` coverage. There may be overlaps,
    but there must be no gaps. Thus, we have complete coverage from
    ``highest_visible_tid`` in the first map all the way back to
    ``complete_since_tid`` in the second-to-last map.

    If the front of this instance already has data matching that same
    ``highest_visible_tid``, the two maps are merged together
    preserving the lowest ``complete_since_tid`` value. In this case,
    a new instance is *not* returned; the same instance is returned so
    that this map can be shared.

    If the front of this instance has a ``highest_visible_tid`` *less*
    than the new polled tid (that is, there's a new TID in the
    database) all data in the new map with a TID less than the current
    ``highest_visible_tid`` could be discarded (but since we walk the
    maps from front to back to find entries, the more data up front
    the better). In fact, we could limit the poll to just that range
    (use ``highest_visible_tid`` as our ``prev_polled_tid``
    parameter), if we're willing to walk through the current map and
    find all objects whose TID is greater than what we would have used
    for ``prev_polled_tid`` (assuming it's >= the current
    ``complete_since_tid`` value) --- but it's probably better to just
    let the database do that.

    On the chance that the value we wish to poll from
    (``prev_polled_tid``) is <= our current ``complete_since_tid``,
    the new map can completely replace the current front of the map
    (merging in any extra data).

    Instead of updating the frontmost map when new a new entry is
    presented, we update the oldest map that can see the new entry.
    The older (farther to the right, or towards the end of the
    ``maps`` list), the more likely it is to be shared by the most
    transactions. (That does have the unfortunate effect of making it
    take longer to get a hit since we have to probe further back in
    the map; maybe we want to update both frontmost and backmost?)

    When a transaction ends and polls, such that the
    ``MVCCDatabaseCoordinator.minimum_highest_visible_tid`` increments
    (which also corresponds to the last reference to that
    ``TransactionRangeObjectIndex`` going away), that map is removed
    from the list (where it was the end), and any unique data in it is
    added to the map at the new back of the list. (The object that was
    removed is *cleared* and marked as *inactive*: other transactions
    will still have that same object on their list, but they shouldn't
    use it anymore to store new index entries.)

    This last map will keep growing. We shrink it in one way:

        - When we merge the back two maps, we examine the map being
          removed. This is a snapshot of the database no longer in
          use. We first remove any entries that have newer data in a
          more recent snapshot. Then, any remaining unique entries
          (which by definition are the current state of the affected
          objects) are simply removed (note that we don't search for
          duplicates). Crucially, to obtain decent hit rates, those
          dropped index entries are re-cached with a special key
          indicating that they haven't changed in a long time;
          operations that update the cache with new data must
          invalidate this special key. If an object is not found in
          this index, it can then be looked for under this special
          key. Even though the index data is not complete, this
          "frozen" object can still be found.

    When merging this last map, we have an excellent time to purge
    entries from the cache: Any OIDs stored in the last map (the one
    being removed) who have entries in other maps has their stored
    OID/TID from the last map removed. These have changed, and by
    definition their old state is not visible to any other connection.

    TODO: The merging and recaching might be expensive? Perhaps we
    want to defer it until some threshold is reached?
    """

    __slots__ = (
        'maps',
        # Set this when we know that the index is invalid
        # for some external reason.
        'invalid',
    )

    def __init__(self, highest_visible_tid, complete_since_tid=None, data=()):
        """
        An instance is created with the first poll, giving us our
        initial TID. It may optionally have data retrieved from
        previously saving the map.
        """
        self.invalid = False
        initial_bucket = _TransactionRangeObjectIndex(highest_visible_tid, None, ())
        initial_bucket.update(data)
        initial_bucket.complete_since_tid = complete_since_tid
        initial_bucket.verify(initial=False)
        # Maps are read from 0...N, so newest bucket must be first.
        self.maps = [initial_bucket]

    def __repr__(self):
        return '<%s at 0x%x maxhvt=%s minhvt=%s cst=%s depth=%s invalid=%s>' % (
            self.__class__.__name__,
            id(self),
            self.maximum_highest_visible_tid,
            self.minimum_highest_visible_tid,
            self.complete_since_tid,
            self.depth,
            self.invalid,
        )

    def stats(self):
        return {
            'depth': self.depth,
            'invalid': self.invalid,
            'hvt': self.maximum_highest_visible_tid,
            'total OIDS': self.total_size,
            'unique OIDs': len(self.keys()),
            'transactions': [
                {
                    'hvt': tx.highest_visible_tid,
                    'total OIDS': len(tx),
                    'cst': tx.complete_since_tid,
                }
                for tx in self.maps
            ]
        }

    def keys(self):
        return OidTMap_multiunion(self.maps)

    def __getitem__(self, oid):
        for mapping in self.maps:
            try:
                # TODO: Microbenchmark this. Which is faster,
                # try/catch or .get()?
                return mapping[oid]
            except KeyError:
                pass
        # No data. Could it be frozen? We'll let the caller decide.
        return None

    def __contains__(self, oid):
        return any(oid in m for m in self.maps)

    def __setitem__(self, oid, tid):
        # Silently discard things that are too new.
        # Something like loadSerial for conflict resolution might do this;
        # we'll see the object from the future when we poll for changes.

        # We need to store this as far forward as we can reasonably go
        # so that it has the most complete information for as long as possible.

        # TODO: Maybe this should always store into the master object?

        for mapping in reversed(self.maps):
            mapping_hvt = mapping.highest_visible_tid
            accepts_writes = mapping.accepts_writes
            if not accepts_writes:
                # Closed, but still in our list because this is a shared object.
                continue
            if tid > mapping_hvt:
                # Not visible; everything still to come is newer yet,
                # so we're done.
                break
            if mapping.complete_since_tid and tid > mapping.complete_since_tid:
                assert oid in mapping, (oid, tid, mapping)
                assert mapping[oid] == tid, mapping
                continue

            assert mapping.complete_since_tid is None or tid <= mapping.complete_since_tid
            mapping[oid] = tid


    @property
    def total_size(self):
        """
        The total size of this object is the combined length of all maps;
        this is not the same thing as the length of unique keys.
        """
        return sum(len(m) for m in self.maps)

    @property
    def depth(self):
        return len(self.maps)

    @property
    def highest_visible_tid(self):
        return self.maps[0].highest_visible_tid

    # The maximum_highest_visible_tid of this object, and indeed, of any
    # given map, must never change. It must always match what's
    # visible to the clients it has been handed to.
    maximum_highest_visible_tid = highest_visible_tid

    @property
    def minimum_highest_visible_tid(self):
        return self.maps[-1].highest_visible_tid

    @property
    def complete_since_tid(self):
        # We are complete since the oldest map we have
        # that thinks *it* is complete. This may not necessarily be the
        # last map (but it should be the second to last map!)
        for mapping in reversed(self.maps):
            cst = mapping.complete_since_tid
            if cst is not None:
                return cst

    # We can't actually remove things from the maps;
    # if we think we have complete information for a range
    # of transactions, then we better have the complete info.
    def invalidate(self, oid_int, tid_int): # pylint:disable=unused-argument
        return
        # bound = tid_int + 1
        # for mapping in self.maps:
        #     if mapping.get(oid_int, bound) <= tid_int:
        #         print("Invalidating", oid_int, tid_int, mapping)
        #         del mapping[oid_int]

    def invalidate_all(self, oids): # pylint:disable=unused-argument
        return
        # for mapping in self.maps:
        #     for oid in oids:
        #         mapping.pop(oid, None)

    def verify(self):
        # Each component has values in range.
        for ix in self.maps:
            ix.verify(initial=False)
        # No gaps in completion
        map_iter = iter(self.maps)
        newest_map = next(map_iter)
        for mapping in map_iter:
            __traceback_info__ = newest_map, mapping
            assert newest_map.complete_since_tid <= mapping.highest_visible_tid
            newest_map = mapping

    def with_polled_changes(self, highest_visible_tid, complete_since_tid, changes):

        # Never call this when the poller has specifically said
        # that there are no changes; either the very first poll, or
        # we went backwards due to reverting to a stale state. That's
        # handled at a higher layer.
        assert changes is not None
        assert highest_visible_tid >= self.highest_visible_tid
        assert complete_since_tid is not None

        # First, create the transaction map.
        assert highest_visible_tid and complete_since_tid
        incoming_bucket = _TransactionRangeObjectIndex(highest_visible_tid,
                                                       complete_since_tid,
                                                       changes)
        newest_bucket = self.maps[0]
        oldest_bucket = self.maps[-1]

        # Was this our first poll?
        if newest_bucket is oldest_bucket and oldest_bucket.complete_since_tid is None:
            #debug("First poll. Oldest bucket:", oldest_bucket, dict(oldest_bucket),
            #      "Incoming:", incoming_bucket, dict(incoming_bucket))
            # Merge the two together and replace ourself.
            assert highest_visible_tid >= oldest_bucket.highest_visible_tid
            if highest_visible_tid == oldest_bucket.highest_visible_tid:
                # Overwrite the incomplete data with the new data, which *may be* complete
                # back to a point. The make the incomplete data become the complete data
                #debug("Oldest bucket", oldest_bucket, "completing to", incoming_bucket)
                oldest_bucket.complete_to(incoming_bucket)
                #debug("Oldest bucket is now", oldest_bucket, dict(oldest_bucket))
                oldest_bucket.verify()
                return self
            # We need to move forward. Therefore we need a new index.
            # Copy forward any old data we've got and maintain our completion status.
            # But we don't want to lose our connection to the older
            # bucket, because that represents the oldest thing we have indexed.
            incoming_bucket.merge_older_tid(oldest_bucket)
            other = _ObjectIndex.__new__(_ObjectIndex)
            other.maps = [incoming_bucket, oldest_bucket]
            other.invalid = False
            #debug("New index is", other)
            other.verify()
            return other

        # Special cases:
        #
        # - len(new_bucket) == 0: No changes. Therefore, if this was
        #   not our first poll, it must have returned the exact same
        #   highest_visible_tid as our current highest visible tid. We
        #   just need to set our frontmost map's
        #   ``complete_since_tid`` to the lowest of the two values.
        #   This is just a degenerate case of polling to a matching highest_visible_tid.

        # We can't get a higher transaction id unless something changed.
        # put the other way, if nothing changed, it must be for our existing tid.
        __traceback_info__ = incoming_bucket, newest_bucket
        assert incoming_bucket or (
            not incoming_bucket and highest_visible_tid == newest_bucket.highest_visible_tid
        )

        if highest_visible_tid == newest_bucket.highest_visible_tid:
            #debug("Merging for same tid", newest_bucket, "incoming", incoming_bucket)
            newest_bucket.merge_same_tid(incoming_bucket)
            return self

        # all that's left is to put the new bucket on front of a new object.
        other = _ObjectIndex.__new__(_ObjectIndex)
        other.maps = [incoming_bucket]
        other.invalid = False
        other.maps.extend(self.maps)
        other.verify() # XXX: Remove before release.
        return other

class _AlreadyClosedLock(object):

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        "Does nothing"

@implementer(IStorageCacheMVCCDatabaseCoordinator)
class MVCCDatabaseCoordinator(InvalidationMixin):
    """
    Keeps track of the most recent polling data so that
    instances don't make unnecessary polls.

    Shared between all instances of a StorageCache
    in a tree, from the master down.
    """

    # Essentially how many transactions any one viewer is allowed to
    # get behind the leading edge before we cut it off. The next time it polls,
    # it will drop its ZODB object cache.
    max_allowed_index_depth = 100
    # The total number of entries in the object index we allow
    # before we start cutting off old viewers. This gets set from
    # Options.cache_delta_size_limit
    max_allowed_index_size = 100000
    object_index = None

    def __init__(self, options=None):
        # Use this lock when we're doing normal poll updates
        # or need to read consistent metadata.
        self._da0_lock = threading.RLock()
        # This holds strong references, creating a reference cycle.
        # calling 'unregister' is crucial.
        self.registered_viewers = []
        options = options or Options()
        # We used to keep two of these...in every connection.
        # If we have many concurrent commits going on, and/or lots of old idle connections,
        # we can surge above this by a substantial amount; as we continue processing transactions,
        # each new one will drop more old viewers, though, and it will start to be reclaimed.
        # Also, lots of it is shared across the connections.
        self.max_allowed_index_size = options.cache_delta_size_limit * 2

    def stats(self):
        return {
            'registered_viewers': len(self.registered_viewers),
            'oldest viewer': self.minimum_highest_visible_tid,
            'hvt': self.maximum_highest_visible_tid,
            'index': self.object_index.stats() if self.object_index else None,
        }

    def register(self, cache):
        with self._da0_lock:
            self.registered_viewers.append(cache)

    def unregister(self, cache):
        with self._da0_lock:
            if self.is_registered(cache):
                self.registered_viewers.remove(cache)

    def is_registered(self, cache):
        return cache in self.registered_viewers

    @property
    def maximum_highest_visible_tid(self):
        # Visible to *any* connection.
        return None if self.object_index is None else self.object_index.maximum_highest_visible_tid

    def __viewers_with_older_tids(self, tid):
        """
        Return all the viewers with tids at least as old as the
        given tid.

        Passing the value from ``minumum_highest_visible_tid`` is always safe,
        even if that value is None. If that value is None, it's because we
        have no viewers, or the viewers we do have haven't looked at the
        database; they'll be ignored.
        """
        with self._da0_lock:
            return [
                viewer for viewer in self.registered_viewers
                if viewer.object_index
                and not viewer.object_index.invalid
                and viewer.object_index.highest_visible_tid
                and viewer.object_index.highest_visible_tid <= tid
            ]

    @property
    def minimum_highest_visible_tid(self):
        # TODO: Track this in real time instead of computing.
        with self._da0_lock:
            min_tid = None
            for viewer in self.registered_viewers:
                ix = viewer.object_index
                if ix is None:
                    continue
                if min_tid is None and not ix.invalid:
                    min_tid = ix.highest_visible_tid # Could still be None
                ix_tid = ix.highest_visible_tid if not ix.invalid else None
                if ix_tid is not None and min_tid is not None and ix_tid < min_tid:
                    min_tid = ix_tid

        return min_tid

    def __invalidate_viewers_locked(self):
        # mark all existing object indexes as needing a rebuild.
        for viewer in self.registered_viewers:
            ix = viewer.object_index
            if ix is not None:
                ix.invalid = True

    @property
    def complete_since_tid(self):
        return -1 if self.object_index is None else self.object_index.complete_since_tid

    def debug_info(self):
        return {
            'object_index': self.object_index,
        }

    def reset_viewer(self, cache):
        with self._da0_lock:
            # Take the lock so that our MVCC state stays consistent.
            cache.object_index = None

    def after_tpc_finish(self, cache, tid_int, temp_objects):
        with self._da0_lock:
            # TODO: The cache is supposed to be calling
            # ``after_tpc_finish`` on us so that we know when it can
            # release its snapshot between transactions; but that
            # isn't great for the ZODB connection cache: We'll miss
            # some invalidations, because we'll be polling from the
            # global most recent polled instead of something local to
            # its ZODB Connection cache. We need a separate queue of
            # invalidations, or the ability to enter an 'inactive'
            # state for just this one object so that we know not to
            # consider its snapshot active. Now, this only happens on
            # connections that do a write; read-only connections never
            # call ``after_tpc_finish`` (which is its own problem) so in practice
            # this probably doesn't matter too much?
            self.reset_viewer(cache)

        # Note that unless we've polled, this data is not yet
        # going to be visible to anyone.
        if tid_int is not None and temp_objects is not None:
            cache.cache.set_all_for_tid(tid_int, temp_objects)

    def poll(self, cache, conn, cursor):
        # For now, we'll do it all while locked, ensuring a linear history
        with self._da0_lock:
            return self._poll_locked(cache, conn, cursor)

    def __set_viewer_state_locked(self, cache):
        cache.object_index = self.object_index
        return self.object_index

    def _poll_locked(self, cache, conn, cursor):
        # Note that poll_invalidations can raise StaleConnectionError,
        # or it can return (None, old_tid) where old_tid is less than
        # its third parameter (``prev_polled_tid``)
        #debug("Entering poll")
        assert self.is_registered(cache)
        if self.object_index is None:
            #debug("Initial poll for", cache)
            # Initial poll for the world. Do this locked to be sure we
            # have a consistent state.
            change_iter, tid = cache.adapter.poller.poll_invalidations(conn, cursor, None, None)
            assert change_iter is None
            if tid > 0:
                #debug("Found data; can begin caching", cache, tid)
                # tid 0 is empty database, no data.
                self.object_index = _ObjectIndex(tid)
            #debug("Initial got tid", tid, self.object_index)
            self.__set_viewer_state_locked(cache)
            return change_iter


        # We have begun keeping an object index. But the cache
        # may not yet. (See comments in _ObjectIndex docstring about
        # possible optimizations for that case, or the case when it can use
        # a different poll range.)
        cache_was_invalid = False
        if cache.object_index and cache.object_index.invalid:
            # Snarf. Old state. Whelp, it needs to invalidate all its
            # cached objects (so we must return None), but it can still use our index
            # and poll state; we don't need to go backwards.
            cache_was_invalid = True
            polling_since = self.maximum_highest_visible_tid
        else:
            polling_since = cache.highest_visible_tid or self.maximum_highest_visible_tid

        change_iter, tid = cache.adapter.poller.poll_invalidations(
            conn, cursor,
            polling_since,
            None)

        if tid == 0 or tid < polling_since:
            assert change_iter is None
            #debug("Freshly zapped or empty db", tid, polling_since, change_iter)
            # Freshly zapped or empty database (tid==0)
            # or stale and asked to revert. Mark not just this one,
            # but all other extent indexes as needing a full rebuild.
            self.flush_all()
            self.__set_viewer_state_locked(cache)
            return change_iter

        # Ok cool, we got data to move us forward.
        # Let's do it.
        # Will need to be able to iterate this more than once.
        change_iter = list(change_iter)
        #debug("Changes since", polling_since, "now at", tid, "are", change_iter)
        prev_index = self.object_index
        assert prev_index is not None
        self.object_index = prev_index.with_polled_changes(
            tid,
            polling_since,
            change_iter
        )
        __traceback_info__ = prev_index, self.object_index
        #debug("Object index now", self.object_index)
        self.__set_viewer_state_locked(cache)

        self._vacuum(cache.local_client)

        return None if cache_was_invalid else change_iter

    @log_timed
    def _vacuum(self, local_cache):
        """
        Handle object index and cache entries for which we no longer
        have a requirement.

        Named for the ``VACUUM`` command and process in PostgreSQL,
        this notices when we are keeping index data for transactions
        that are no longer needed.

        When this function is called, the cache doing the polling
        should have been updated with the new object index (and thus
        ``highest_visible_tid``), and thus released its claim to its
        former TID. Our ``object_index`` must also have been updated.

        If our ``object_index`` has a ``minimum_highest_visible_tid``
        (i.e., the oldest polled transaction) that is now less than
        the oldest polled transaction needed by any extent cache
        registered to this coordinator, we are free to vacuum that
        oldest state.
        """
        # This is called for every transaction. It needs to be fast, and mindful
        # of what it logs.

        stats_before = self.stats()
        # TODO: This strategy can easily develop "gaps", where
        # one lone reader is at the back and all the other readers are
        # up front somewhere, with that chain of maps in between doing
        # no one any good. We should try to squash those gaps.

        # We partly deal with that by deciding to cut off the oldest viewers
        # (probably idle connections sitting in the pool) if the total depth
        # gets too deep, or the total size gets too large. Do that first.
        if (
                self.object_index.depth > self.max_allowed_index_depth
                or self.object_index.total_size > self.max_allowed_index_size
        ):
            for viewer in self.__viewers_with_older_tids(self.minimum_highest_visible_tid):
                viewer.object_index.invalid = True

        object_index = self.object_index
        while object_index.depth > 1 and not self._is_oldest_tid_still_required():
            # all remaining valid viewers have highest_visible_tid > this one
            # So any OIDs that exist in both this bucket and any newer bucket with a newer
            # TID can be purged from the local cache because they've been changed.
            obsolete_bucket = object_index.maps.pop()
            # Immediately also mark it as closed before we start mutating its
            # contents. No more storing to this one!
            obsolete_bucket.accepts_writes = False
            newer_oids = object_index.keys()
            in_both = OidTMap_intersection(newer_oids, obsolete_bucket)
            logger.debug("Examining %d old OIDs to see if they've been replaced",
                         len(in_both))
            for oid in in_both:
                old_tid = obsolete_bucket[oid]
                newer_tid = object_index[oid]
                # We intersected, we're sure that they're both not None.
                if newer_tid != old_tid:
                    # Note that even though we're removing data from
                    # this bucket that might be in the range that it
                    # claims to have complete index data for, that's
                    # fine: The end result when we put everything back
                    # together is still going to be complete index
                    # data, because the object changed in the future.
                    # This particular transaction chunk won't be complete, but
                    # it's inaccessible.
                    del obsolete_bucket[oid]
                    del local_cache[(oid, old_tid)]

            # Now at this point, the obsolete_bucket contains data that we know is
            # either not present in a future map, or is present with exactly the
            # same value. Therefore, at least until someone changes it again,
            # we can consider this data to be frozen. We'll make available each
            # cached value at a special key. There's nothing
            # useful to have in this last bucket and we can throw it away. Note that
            # we do *not* remove the index entries; they're needed to keep
            # the CST in sync for newer transactions that might still be open.
            logger.debug("Vacuum: Freezing %s old OIDs", obsolete_bucket)
            if obsolete_bucket:
                local_cache.freeze(obsolete_bucket)

        stats_after = self.stats()
        if stats_before != stats_after:
            logger.debug("Vacuum: Stats Before: %s After: %s", stats_before, self.stats())
        else:
            logger.debug("Vacuum: Stats: No change: %s", stats_before)

    def _is_oldest_tid_still_required(self):
        oldest_indexed = self.object_index.minimum_highest_visible_tid
        oldest_required = self.minimum_highest_visible_tid
        if oldest_required:
            # If everyone is unregistered, we may not have any requirements.
            # Vacuum freely!
            assert oldest_required >= oldest_indexed, (oldest_required, oldest_indexed)
            return oldest_required <= oldest_indexed

    def flush_all(self):
        with self._da0_lock:
            self.object_index = None
            self.__invalidate_viewers_locked()

    def close(self):
        with self._da0_lock:
            self._da0_lock = _AlreadyClosedLock()
            self.object_index = None
            self.registered_viewers = ()

    def invalidate_all(self, oids):
        if not self.object_index:
            return

        self.object_index.invalidate_all(oids)

    def invalidate(self, oid_int, tid_int):
        if not self.object_index:
            return
        self.object_index.invalidate(oid_int, tid_int)

    def save(self, local_client, save_args):
        if not self.object_index or not self.maximum_highest_visible_tid:
            # We have never polled or verified anything, don't
            # try to save what we can't validated.
            logger.debug("No index or HVT; nothing to save %s", self.stats())
            return
        # Vacuum, disposing of uninteresting and duplicate data.
       # We should have no viewers, so we eliminated all except the final map
        for viewer in self.registered_viewers:
            if viewer.object_index:
                viewer.object_index.invalid = True
        self._vacuum(local_client)
        # At this point, we now have processed all the extent invalidations

        assert self.object_index.depth == 1
        # We giv that last map to the local client so it knows to write only
        # known-valid data and to dispose of anything invalid.

        checkpoints = None
        max_hvt = self.maximum_highest_visible_tid
        checkpoints = (
            max_hvt,
            self.complete_since_tid or max_hvt
        )
        return local_client.save(object_index=self.object_index.maps[0],
                                 checkpoints=checkpoints, **save_args)

    def restore(self, adapter, local_client):
        # This method is not thread safe

        # Note that there may have been an arbitrary amount of data in
        # the file that we didn't get to actually store but that still
        # comes back in the object_index; that's ok. If we pick up some
        # database changes we will start dropping this off pretty quickly.
        # TODO: maybe we want to split the incoming data up by transaction,
        # just like we built it that way in memory.
        row_filter = _PersistentRowFilter(adapter,
                                          OidTMap,
                                          self.max_allowed_index_size)
        local_client.restore(row_filter)

        if row_filter.complete_range:
            # We will thus begin polling at the last poll location
            # stored in the data.
            self.object_index = _ObjectIndex(
                row_filter.highest_visible_tid,
                # But we can't truly claim to be complete before that,
                # based on the present schema. Our data is based on what's in the
                # cache, not what our object_index has. The index may be complete,
                # but the data still in the cache memory may not be.
                None, # row_filter.complete_since_tid,
                row_filter.complete_range)
        else:
            self.object_index = None


class _PersistentRowFilter(object):

    def __init__(self, adapter, delta_type, delta_size_limit):
        self.adapter = adapter
        self.complete_range = delta_type()
        self.highest_visible_tid = 0
        self.complete_since_tid = 0
        self.delta_size_limit = delta_size_limit

    def __str__(self):
        return "<PersistentRowFilter>"

    def __call__(self, checkpoints, row_iter):
        # The 'checkpoints' are actually
        # (max_highest_visible_tid, complete_since_tid)
        # where complete_since_tid could actually be == max_hvt if
        # we didn't have that information.
        if not checkpoints:
            #debug("No checkpoints")
            checkpoints = (0, 0)

        #debug("Using checkpoints", checkpoints)
        complete_range = self.complete_range
        highest_visible_tid, complete_since_tid = checkpoints
        self.highest_visible_tid = highest_visible_tid
        self.complete_since_tid = complete_since_tid

        # Right now, we only keep things in the complete range.
        # The 'frozen' data is still to come.

        for row in row_iter:
            # Rows are (oid, key tid, state, tid),
            # The key tid will be -1 if the row was frozen.
            oid = row[0]
            key_tid = row[1]
            value = row[2:]
            actual_tid = value[1]

            # If we had a transaction where we added/updated items, and then
            # closed down before polling, we could have things in the cache file
            # that have an actual_tid > highest_visible_tid.

            # We'll begin polling at the highest_visible_tid
            # which we will set such that we do a single fairly large poll
            # up front by only storing items into our map until we hit the delta size limit,
            if len(complete_range) < self.delta_size_limit:
                #debug('Restoring', oid, "at", actual_tid)
                complete_range[oid] = actual_tid
                key_tid = actual_tid
            elif key_tid > 0:
                continue
                # No point storing things with their actual tid if we don't have them
                # in the map...

            yield (oid, key_tid), value

        if complete_range:
            self.highest_visible_tid = max(complete_range.values())
