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

from logging import DEBUG as LDEBUG

from perfmetrics import statsd_client

from zope.interface import implementer

from relstorage._inthashmap import OidTidMap as OidTMap # pylint:disable=no-name-in-module
from relstorage._inthashmap import OidSet # pylint:disable=no-name-in-module
from relstorage._compat import iteroiditems

from relstorage._util import log_timed
from relstorage._util import get_positive_integer_from_environ
from relstorage._util import TRACE as LTRACE
from relstorage._util import get_duration_from_environ
from relstorage._mvcc import DetachableMVCCDatabaseCoordinator
from relstorage.options import Options


from .interfaces import IStorageCacheMVCCDatabaseCoordinator

from ._objectindex import _ObjectIndex # pylint:disable=no-name-in-module,import-error



logger = __import__('logging').getLogger(__name__)


#: The maximum amount of time, in seconds, we will spend polling the
#: database for OIDS when restoring the cache. If this time is exceeded,
#: we will only partially use the cache. This is not set by default because
#: doing so introduces a slight speed penalty to the polling process.
POLL_TIMEOUT = get_duration_from_environ('RS_CACHE_POLL_TIMEOUT', None, logger=logger)

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

# pylint:disable=too-many-lines



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
    # get behind the leading edge before we cut it off. The next time
    # it polls, it will drop its ZODB object cache. When configured
    # properly, this may indicate an idle connection without a
    # connection pool idle timeout configured, or a very long-duration
    # read transaction. In sites with high transaction turnover, but
    # few concurrent requests to any given process, it may also
    # indicate low concurrency, e.g., the first connection in the pool
    # is used most of the time, and by the time there's two concurrent
    # connections in use, the second one has been detached.
    #
    # Observations in a production site with a one-hour ZODB
    # connection pool timeout show that the old default of 100 was
    # detaching a connection at anywhere from 1 minute to 20 minutes
    # since it had been used. But 88% of detached connections (2546
    # detached, 2249 polled again) were used again after flushing their
    # entire object cache.
    #
    # So we bump the default to 1000.
    max_allowed_index_depth = get_positive_integer_from_environ(
        'RS_CACHE_MVCC_MAX_DEPTH', 1000, logger=logger
    )

    # The total number of entries in the object index we allow
    # before we start cutting off old viewers. This gets set from
    # Options.cache_delta_size_limit. A full structure with the default
    # size (200,000) occupies 6.6MB of memory with C BTrees.
    max_allowed_index_size = 100000
    object_index = None

    def __init__(self, options=None):
        super().__init__()
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
            # Freshly zapped or empty database (tid==0) or stale replica and
            # asked to revert (as opposed to raising
            # ReadConflictError). Mark not just this one, but all
            # other extent indexes as needing a full rebuild.
            with self._lock:
                self.flush_all()
                self.__set_viewer_state_locked(cache, None)
            return None

        # Ok cool, we got data to move us forward.
        # We must be careful to always consume the iterator, even if we exit early
        # (because it could be a server-side cursor holding connection state).
        # So we do that now.
        change_iter = list(change_iter)
        self.log(
            LTRACE,
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
                # There could be some overlap. Since we moved the index forward,
                # we can vacuum.
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
            logger.debug(
                "Invalidating all persistent objects for viewer %r (detached? %s)",
                viewer, viewer.detached
            )
            if viewer.detached:
                client = statsd_client()
                if client is not None:
                    client.incr('relstorage.cache.mvcc.invalidate_all_detached',
                                1,
                                1) # Always send, not a sample. Should be rare.
            return None

        # Somewhere in the index is a map with the highest visible tid
        # matching the last time this viewer polled. Everything from there
        # forward is a change that this viewer needs to see.
        # Note there could be no changes.
        changes = object_index.collect_changes_after(viewer.highest_visible_tid)

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
            self.log(
                LDEBUG,
                "Detaching oldest viewers because depth (%s) > max depth (%s) "
                "or total size (%s) > max size (%s)",
                object_index.depth, self.max_allowed_index_depth,
                object_index.total_size, self.max_allowed_index_size,
            )
            self.detach_viewers_at_minimum()

        required_tid = self.minimum_highest_visible_tid # This won't change during our invocation
        local_client = cache.local_client
        self.log(
            LTRACE,
            "Attempting vacuum from %s up to %s",
            object_index.minimum_highest_visible_tid,
            required_tid,
        )
        oids_tids_to_del = OidTMap()
        while 1: # pylint:disable=consider-refactoring-into-while-condition
            if object_index.depth == 1:
                # Nothing left to vacuum
                break
            if object_index.minimum_highest_visible_tid == required_tid:
                # Still need this history.
                break
            next_state = object_index.get_second_oldest_transaction()
            if required_tid and next_state.highest_visible_tid > required_tid:
                # The last state isn't quite obsolete, others are still looking
                # at that range. Don't remove it.
                break

            # all remaining valid viewers have highest_visible_tid > this one
            # So any OIDs that exist in both this bucket and any newer bucket with a newer
            # TID can be purged from the local cache because they've been changed.
            obsolete_bucket = object_index.remove_oldest_transaction_and_collect_invalidations(
                oids_tids_to_del)

            # Now at this point, the obsolete_bucket contains data that we know is
            # either not present in a future map, or is present with exactly the
            # same value. Therefore, at least until someone changes it again,
            # we can consider this data to be frozen. We'll make available each
            # cached value at a special key. There's nothing
            # useful to have in this last bucket and we can throw it away. Note that
            # we do *not* remove the index entries; they're needed to keep
            # the CST in sync for newer transactions that might still be open.
            if obsolete_bucket:
                self.log(LTRACE, "Vacuum: Freezing %s old OIDs", len(obsolete_bucket))
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
            return None
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
        return local_client.save(object_index=self.object_index.get_newest_transaction(),
                                 checkpoints=checkpoints, **save_args)

    def restore(self, adapter, local_client, timeout=None):
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
            self.__poll_old_oids_and_remove(adapter, local_client, timeout or POLL_TIMEOUT)

    @log_timed
    def __poll_old_oids_and_remove(self, adapter, local_client, timeout):
        from relstorage.adapters.connmanager import connection_callback
        from relstorage.adapters.interfaces import AggregateOperationTimeoutError

        cached_oids = OidSet(local_client.keys())
        # In local tests, this function executes against PostgreSQL 11 in .78s
        # for 133,002 older OIDs; or, .35s for 57,002 OIDs against MySQL 5.7.
        # In one production environment of 800,000 OIDs with a 98% survival rate,
        # using MySQL 5.7 takes an average of about 11s.
        # However, it has been observed that in some cases, presumably when the database
        # is under intense IO stress, this can take 400s for 500,000 OIDS:
        # since the ``current_object_tids`` batches in groups of 1024, that works out to
        # .75s per SQL query. Not good. Hence the ability to set a timeout.
        logger.info("Polling %d oids stored in cache with SQL timeout %r",
                    len(cached_oids), timeout)

        @connection_callback(isolation_level=adapter.connmanager.isolation_load,
                             read_only=True)
        def poll_cached_oids(_conn, cursor):
            # type: (Any, Any) -> Dict[Int, Int]
            """Return mapping of {oid_int: tid_int}"""
            try:
                return adapter.mover.current_object_tids(cursor, cached_oids,
                                                         timeout=timeout)
            except AggregateOperationTimeoutError as ex:
                # If we time out, we can at least validate the results we have
                # so far.
                logger.info(
                    "Timed out polling the database for %s oids; will use %s partial results",
                    len(cached_oids), len(ex.partial_result)
                )
                return ex.partial_result

        current_tids = adapter.connmanager.open_and_call(poll_cached_oids)
        current_tid = current_tids.get
        polled_invalid_oids = OidSet()
        # pylint:disable-next=protected-access
        cache_is_correct = local_client._cache.contains_oid_with_tid

        for oid_int in cached_oids:
            if not cache_is_correct(oid_int, current_tid(oid_int)):
                polled_invalid_oids.add(oid_int)

        logger.info("Polled %d older oids stored in cache (%d found in database); %d survived",
                    len(cached_oids), len(current_tids),
                    len(cached_oids) - len(polled_invalid_oids))
        local_client.remove_invalid_persistent_oids(polled_invalid_oids)
