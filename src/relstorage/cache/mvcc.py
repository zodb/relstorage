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

import os

from zope.interface import implementer

from relstorage._compat import iterkeys
from relstorage._compat import OID_TID_MAP_TYPE as OidTMap
from relstorage._compat import OidTMap_difference
from relstorage._compat import OidTMap_multiunion
from relstorage._compat import OidTMap_intersection
from relstorage._compat import OID_SET_TYPE as OidSet
from relstorage._compat import iteroiditems
from relstorage._compat import IN_TESTRUNNER
from relstorage._util import log_timed
from relstorage._util import positive_integer
from relstorage._util import TRACE
from relstorage._mvcc import DetachableMVCCDatabaseCoordinator
from relstorage.options import Options
from relstorage.interfaces import IMVCCDatabaseViewer

from .interfaces import IStorageCacheMVCCDatabaseCoordinator

logger = __import__('logging').getLogger(__name__)

DEBUG = __debug__ and IN_TESTRUNNER

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
            if DEBUG:
                assert self.complete_since_tid
                self.verify()
        else:
            # If we had no changes, then either we polled for the same tid
            # as we got, or we didn't try to poll for changes at all.
            assert complete_since_tid is None or complete_since_tid == highest_visible_tid, (
                complete_since_tid, highest_visible_tid
            )

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

@implementer(IMVCCDatabaseViewer)
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
    )

    def __init__(self, highest_visible_tid, complete_since_tid=None, data=()):
        """
        An instance is created with the first poll, giving us our
        initial TID. It may optionally have data retrieved from
        previously saving the map.
        """
        initial_bucket = _TransactionRangeObjectIndex(highest_visible_tid, None, ())
        initial_bucket.update(data)
        initial_bucket.complete_since_tid = complete_since_tid
        initial_bucket.verify(initial=False)
        # Maps are read from 0...N, so newest bucket must be first.
        self.maps = [initial_bucket]

    def __repr__(self):
        return '<%s at 0x%x maxhvt=%s minhvt=%s cst=%s depth=%s>' % (
            self.__class__.__name__,
            id(self),
            self.maximum_highest_visible_tid,
            self.minimum_highest_visible_tid,
            self.complete_since_tid,
            self.depth,
        )

    def stats(self):
        return {
            'depth': self.depth,
            'hvt': self.maximum_highest_visible_tid,
            'min hvt': self.minimum_highest_visible_tid,
            'total OIDS': self.total_size,
            'unique OIDs': len(self.keys()),
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

        # Because each successive map is supposed to be a delta, we only
        # store this in the first possible map.

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
            break


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
            # Merge the two together and replace ourself.
            assert highest_visible_tid >= oldest_bucket.highest_visible_tid
            if highest_visible_tid == oldest_bucket.highest_visible_tid:
                # Overwrite the incomplete data with the new data, which *may be* complete
                # back to a point. The make the incomplete data become the complete data
                oldest_bucket.complete_to(incoming_bucket)
                #debug("Oldest bucket is now", oldest_bucket, dict(oldest_bucket))
                oldest_bucket.verify()
                return self
            # We need to move forward. Therefore we need a new index.
            # Copy forward any old data we've got and maintain our completion status.
            # But we don't want to lose our connection to the older
            # bucket, because that represents the oldest thing we have indexed.
            # Why merge? That doesn't make much sense. We're no longer a delta.
            # incoming_bucket.merge_older_tid(oldest_bucket)
            other = _ObjectIndex.__new__(_ObjectIndex)
            other.maps = [incoming_bucket, oldest_bucket]
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
        other.maps.extend(self.maps)
        if DEBUG:
            other.verify()
        return other


class _AlreadyClosedLock(object):

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        "Does nothing"

@implementer(IStorageCacheMVCCDatabaseCoordinator)
class MVCCDatabaseCoordinator(DetachableMVCCDatabaseCoordinator):
    """
    Keeps track of the most recent polling data so that
    instances don't make unnecessary polls.

    Shared between all instances of a StorageCache
    in a tree, from the master down.
    """

    # Essentially how many transactions any one viewer is allowed to
    # get behind the leading edge before we cut it off. The next time it polls,
    # it will drop its ZODB object cache. This probably indicates an idle connection
    # without a connection pool idle timeout configured, or a very long-duration
    # read transaction.
    max_allowed_index_depth = positive_integer(
        os.environ.get('RS_CACHE_MVCC_MAX_DEPTH', '100')
    )

    # The total number of entries in the object index we allow
    # before we start cutting off old viewers. This gets set from
    # Options.cache_delta_size_limit
    max_allowed_index_size = 100000
    object_index = None

    def __init__(self, options=None):
        super(MVCCDatabaseCoordinator, self).__init__()
        # There's a tension between blocking as little as possible
        # and making as few polling queries as possible. Polling is when
        # the GIL is released or gevent switches can occur, and potentially
        # allow other threads/greenlets to do useful work. OTOH the more
        # polling queries we do the more (probably overlapping) data we have to read
        # and process.
        #
        # We content ourselves with only locking enough to keep our constraints
        # consistent and allow for potentially overlapped polls. Since we use the previous
        # global poll as our starting point, they should be small.

        options = options or Options()
        # We used to keep two of these...in every connection.
        # If we have many concurrent commits going on, and/or lots of old idle connections,
        # we can surge above this by a substantial amount; as we continue processing transactions,
        # each new one will drop more old viewers, though, and it will start to be reclaimed.
        # Also, lots of it is shared across the connections.
        self.max_allowed_index_size = options.cache_delta_size_limit * 2
        self.log = logger.log

    def stats(self):
        return {
            'registered_viewers': len(self._registered_viewers),
            'oldest viewer': self.minimum_highest_visible_tid,
            'hvt': self.object_index.maximum_highest_visible_tid if self.object_index else None,
            'index': self.object_index.stats() if self.object_index else None,
        }

    @property
    def complete_since_tid(self):
        return -1 if self.object_index is None else self.object_index.complete_since_tid

    def reset_viewer(self, cache):
        cache.object_index = None
        self.change(cache, None)

    def poll(self, cache, conn, cursor):
        with self._lock:
            cur_ix = self.object_index
            # this can mutate without changing the object identity!
            cur_ix_hvt = cur_ix.highest_visible_tid if cur_ix else None

        return self._poll(cache, conn, cursor, cur_ix, cur_ix_hvt)

    def __set_viewer_state_locked(self, cache, index):
        cache.object_index = index
        # attaches the viewer if it was detached.
        self.change(cache,
                    index.highest_visible_tid
                    if index is not None
                    else None)

    def __initial_poll(self, cache, cursor):
        # Initial poll for the world.
        tid = cache.adapter.poller.get_current_tid(cursor)

        new_index = None
        if tid > 0:
            # tid 0 is empty database, no data.
            new_index = _ObjectIndex(tid)

        with self._lock:
            if new_index is not None and self.object_index is None:
                # If we're still none, we got here first, yay us.
                self.object_index = new_index

            self.__set_viewer_state_locked(cache, self.object_index)
        # Regardless whether we or someone else did or did not
        # get an index, the viewer gets our current state, and also
        # gets told to invalidate everything (implicit return None)

    def _poll(self, cache, conn, cursor,
              current_index,
              current_index_hvt):
        # Note that poll_invalidations can raise StaleConnectionError,
        # or it can return (None, old_tid) where old_tid is less than
        # its third parameter (``prev_polled_tid``)
        if current_index is None:
            return self.__initial_poll(cache, cursor)

        # We have begun keeping an object index. But the cache
        # may not yet. (See comments in _ObjectIndex docstring about
        # possible optimizations for that case, or the case when it can use
        # a different poll range.)

        polling_since = current_index_hvt

        # Do a small poll.
        # NOTE: See comment in __init__ about tensions about locking
        # and overlapping polls.
        change_iter, polled_tid = cache.adapter.poller.poll_invalidations(
            conn, cursor,
            polling_since,
        )

        if polled_tid == 0 or polled_tid < polling_since:
            assert change_iter is None
            # Freshly zapped or empty database (tid==0) or stale and
            # asked to revert (as opposed to raising
            # ReadConflictError). Mark not just this one, but all
            # other extent indexes as needing a full rebuild.
            with self._lock:
                self.flush_all()
                self.__set_viewer_state_locked(cache, None)
            return None

        # Ok cool, we got data to move us forward.
        # We must be careful to always consume the iterator, even if we exit early.
        # So we do that now.
        change_iter = list(change_iter)
        self.log(
            TRACE,
            "Polled new tid %s since %s with %s changes",
            polled_tid, polling_since, len(change_iter)
        )

        # The *change_index* is the index we'll provide to the viewer; it
        # *must* be built from the data we polled, or at least, from an
        # index with an HVT equal to what we polled. It cannot go into the future
        # of what we polled.
        change_index = None
        # Set *should_vacuum* to true when we're the ones that moved the
        # state forward.
        should_vacuum = False
        installed_index = None
        with self._lock:
            installed_index = self.object_index
            if installed_index is None:
                # Whoops, somebody flushed things while we were working. Alrighty then.
                self.__set_viewer_state_locked(cache, None)
                return None

            if installed_index.highest_visible_tid < polled_tid:
                # it's older than our poll, we take control;
                # but the history of the index must always move forward,
                # so we build it starting from what's currently installed.
                # There could be some overlap.
                change_index = self.object_index = installed_index.with_polled_changes(
                    polled_tid,
                    polling_since,
                    change_iter
                )
                change_iter = ()
                should_vacuum = True
            elif installed_index.highest_visible_tid == polled_tid:
                # Cool, no work for us!
                # we can simply discard our poll data.
                change_iter = ()
                change_index = installed_index
            # Otherwise, the data they have is in the future. Whoops!
            # We must build our own change data.
            del installed_index

        # Build changes without the lock.
        if change_index is None:
            # Note that this is going to create a gap in the
            # master object_index. The master object_index
            # may have transaction ranges like:
            # (26, 31]
            # (31, 37]
            # and we have now created an alternate index with
            # (26, 31]
            # (32, 35]
            # and can thus produce a self.minimum_highest_visible_tid
            # of 35, which is not directly answerable with any
            # transaction range in the index.
            # The vacuum process has to be careful to check that
            # the min HVT is answerable *before* it removes a map.
            change_index = current_index.with_polled_changes(
                polled_tid,
                polling_since,
                change_iter
            )
        change_iter = self._find_changes_for_viewer(cache, change_index)

        # Move our MVCC state forward and vacuum while locked.
        with self._lock:
            if self.object_index is None:
                self.__set_viewer_state_locked(cache, None)
                return None

            self.__set_viewer_state_locked(cache, change_index)
            if should_vacuum:
                # Must be sure to vacuum using the state we installed.
                # If it's been replaced, it will only have moved forward
                # and all the rest of the maps are still shared.
                if self.object_index.highest_visible_tid >= polled_tid:
                    self._vacuum(cache, change_index)

        return change_iter

    @staticmethod
    def _find_changes_for_viewer(viewer, object_index):
        """
        Given a freshly polled *object_index*, and the *viewer* that polled
        for it, build a changes iterator.

        Call this **before** updating the viewer's MVCC state, so that
        we know how far back we need to build the changes.

        Does not need to hold the lock, except that the index cannot be
        vacuumed until this process is complete (since we may need that for
        building changes).
        """
        if viewer.highest_visible_tid is None or viewer.detached:
            # Snarf. Old state, and we probably lost track of changes.
            # Whelp, it needs to invalidate all its cached objects (so
            # we must return None), but it can still use our index and
            # poll state going forward; we don't need to go backwards.
            return None

        # Somewhere in the index is a map with the highest visible tid
        # matching the last time this viewer polled. Everything from there
        # forward is a change
        # Note there could be no changes.
        last_poll_time = viewer.highest_visible_tid
        changes = OidTMap()
        change_dicts = []
        for m in object_index.maps:
            if m.highest_visible_tid == last_poll_time:
                break
            change_dicts.append(m)

        while change_dicts:
            # In reverse order, capturing only the most recent change.
            # TODO: Except for that 'ignore_tid' passed to the viewer's
            # poll method, we could very efficiently do this with
            # OidTMap_multiunion with one call to C.
            changes.update(change_dicts.pop())

        return iteroiditems(changes)

    @log_timed
    def _vacuum(self, cache, object_index):
        """
        Handle object index and cache entries for which we no longer
        have a requirement.

        Named for the ``VACUUM`` command and process in PostgreSQL,
        this notices when we are keeping index data for transactions
        that are no longer needed.

        When this function is called, the cache doing the polling
        should have been updated with the new object index (and thus
        ``highest_visible_tid``), and thus released its claim to its
        former TID.

        Usually, ``object_index`` will be the same as ``self.object_index``
        but that's not required to enable vacuuming of divergent indices
        in concurrent situations.

        If the given ``object_index`` has a ``minimum_highest_visible_tid``
        (i.e., the oldest polled transaction) that is now less than
        the oldest polled transaction needed by any extent cache
        registered to this coordinator, we are free to vacuum that
        oldest state.

        Usually, the next state's ``minimum_highest_visible_tid`` will
        match exactly our new required minimum, but in case of divergent
        indices, there may be a gap. We do not want to vacuum in that case
        because, even though the objects with the partially diverged index
        will still be able to read just fine, we may prematurely remove
        cached object states that they need.
        """
        # This is called for every transaction. It needs to be fast, and mindful
        # of what it logs.
        #
        # MVCC can easily develop "gaps", where one lone reader is at
        # the back and all the other readers are up front somewhere,
        # with that chain of maps in between doing no one any good. We
        # should try to squash those gaps.

        # We partly deal with that by deciding to cut off the oldest viewers
        # (probably idle connections sitting in the pool) if the total depth
        # gets too deep, or the total size gets too large. Do that first.
        if (
                object_index.depth > self.max_allowed_index_depth
                or object_index.total_size > self.max_allowed_index_size
        ):
            self.detach_viewers_at_minimum()

        required_tid = self.minimum_highest_visible_tid # This won't change during our invocation
        local_client = cache.local_client
        self.log(
            TRACE,
            "Attempting vacuum from %s up to %s",
            object_index.minimum_highest_visible_tid,
            required_tid,
        )
        oids_tids_to_del = OidTMap()
        while 1:
            if object_index.depth == 1:
                # Nothing left to vacuum
                break
            if object_index.minimum_highest_visible_tid == required_tid:
                # Still need this history.
                break
            next_state = object_index.maps[-2]
            if required_tid and next_state.highest_visible_tid > required_tid:
                # The last state isn't quite obsolete, others are still looking
                # at that range. Don't remove it.
                break

            # all remaining valid viewers have highest_visible_tid > this one
            # So any OIDs that exist in both this bucket and any newer bucket with a newer
            # TID can be purged from the local cache because they've been changed.
            obsolete_bucket = object_index.maps.pop()
            # Immediately also mark it as closed before we start mutating its
            # contents. No more storing to this one!
            obsolete_bucket.accepts_writes = False
            newer_oids = object_index.keys()
            in_both = OidTMap_intersection(newer_oids, obsolete_bucket)

            self.log(
                TRACE,
                "Examining %d old OIDs to see if they've been replaced",
                len(in_both)
            )

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
                    # This is where we should hook in the 'invalidation' tracing.
                    del obsolete_bucket[oid]
                    oids_tids_to_del[oid] = old_tid # These will just keep going up
                    # If we have a shared memcache, we can't be sure everyone
                    # else is done with this key, so we just leave it alone.

            # Now at this point, the obsolete_bucket contains data that we know is
            # either not present in a future map, or is present with exactly the
            # same value. Therefore, at least until someone changes it again,
            # we can consider this data to be frozen. We'll make available each
            # cached value at a special key. There's nothing
            # useful to have in this last bucket and we can throw it away. Note that
            # we do *not* remove the index entries; they're needed to keep
            # the CST in sync for newer transactions that might still be open.
            if obsolete_bucket:
                self.log(TRACE, "Vacuum: Freezing %s old OIDs", len(obsolete_bucket))
                local_client.freeze(obsolete_bucket)

        if oids_tids_to_del:
            local_client.delitems(oids_tids_to_del)


    def flush_all(self):
        with self._lock:
            self.object_index = None
            self.detach_all()

    def close(self):
        self.clear()
        with self._lock:
            self.object_index = None

    def save(self, cache, save_args):
        if not self.object_index or not self.object_index.maximum_highest_visible_tid:
            # We have never polled or verified anything, don't
            # try to save what we can't validated.
            logger.debug("No index or HVT; nothing to save %s", self.stats())
            return
        # Vacuum, disposing of uninteresting and duplicate data.
        # We should have no viewers, so we eliminated all except the final map.
        self.detach_all()
        self._vacuum(cache, self.object_index)
        # At this point, we now have processed all the extent invalidations.
        # Note that if there was previously saved data that we invalidated,
        # and have vacuumed away from our index now, we won't know to remove it from
        # the cache file. We do that at load time.

        assert self.object_index.depth == 1, (self.object_index, self._registered_viewers)
        # We give that last map to the local client so it knows to write only
        # known-valid data and to dispose of anything invalid.

        checkpoints = None
        max_hvt = self.object_index.maximum_highest_visible_tid
        checkpoints = (
            max_hvt,
            self.complete_since_tid or max_hvt
        )
        local_client = cache.local_client
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

        checkpoints = local_client.restore()
        highest_visible_tid = checkpoints[0] if checkpoints else None

        if highest_visible_tid:
            # We will thus begin polling at the last poll location
            # stored in the data. All loaded rows are treated as frozen.
            # We won't write them back out.

            self.object_index = _ObjectIndex(highest_visible_tid)
            self.__poll_old_oids_and_remove(adapter, local_client)

    @log_timed
    def __poll_old_oids_and_remove(self, adapter, local_client):
        from relstorage.adapters.connmanager import connection_callback

        oids = OidSet(local_client.keys())
        # In local tests, this function executes against PostgreSQL 11 in .78s
        # for 133,002 older OIDs; or, .35s for 57,002 OIDs against MySQL 5.7.
        # In one production environment of 800,000 OIDs with a 98% survival rate,
        # using MySQL 5.7 takes an average of about 11s.
        logger.debug("Polling %d oids stored in cache", len(oids))

        @connection_callback(isolation_level=adapter.connmanager.isolation_load, read_only=True)
        def poll_old_oids(_conn, cursor):
            return adapter.mover.current_object_tids(cursor, oids)

        current_tids_for_oids = adapter.connmanager.open_and_call(poll_old_oids).get
        polled_invalid_oids = OidSet()
        peek = local_client._peek

        for oid in oids:
            current_tid = current_tids_for_oids(oid)
            if (current_tid is None
                    or peek(oid)[1] != current_tid):
                polled_invalid_oids.add(oid)

        logger.debug("Polled %d older oids stored in cache; %d survived",
                     len(oids), len(oids) - len(polled_invalid_oids))
        local_client.remove_invalid_persistent_oids(polled_invalid_oids)
