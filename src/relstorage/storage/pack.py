# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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
Implementation of the high-level packing algorithm.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
from contextlib import contextmanager

from persistent.timestamp import TimeStamp

from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage._compat import OID_SET_TYPE
from relstorage._util import metricmethod
from .util import writable_storage_method

logger = __import__('logging').getLogger(__name__)

class Pack(object):

    __slots__ = (
        'options',
        'locker',
        'connmanager',
        'blobhelper',
        'cache',
        'packundo',
        'stats',
    )

    def __init__(self, options, adapter, blobhelper, cache):
        self.options = options
        self.locker = adapter.locker
        self.connmanager = adapter.connmanager
        self.packundo = adapter.packundo.with_options(options)
        self.stats = adapter.stats
        self.blobhelper = blobhelper
        self.cache = cache

    def __pre_pack(self, t, referencesf):
        logger.info("pack: beginning pre-pack")

        # In 2019, Unix timestamps look like
        #            1564006806.0
        # While 64-bit integer TIDs for the same timestamp look like
        #    275085010696509852
        #
        # Multiple TIDs can map to a single Unix timestamp.
        # For example, the 9 integers between 275085010624927044 and
        # 275085010624927035 all map to 1564006804.9999998.
        #
        # Therefore, Unix timestamps are ambiguous, especially if we're committing
        # multiple transactions rapidly (within the resolution of the underlying TID
        # clock).
        # This ambiguity mostly matters for unit tests, where we do commit rapidly.
        #
        # To help them out, we accept 64-bit integer TIDs to specify an exact
        # transaction to pack to.

        # We also allow None or a negative number to mean "current committed transaction".
        if t is None:
            t = -1

        if t > 275085010696509852:
            # Must be a TID.

            # This magic number generates 'Value too large to be stored in data type' when
            # given to time.gmtime() as used to originally generate TimeStamp values,
            # though it can be converted back. It also happens to specify July 2019
            # when this feature was implemented.

            # Turn it back into a time.time() for later logging
            ts = TimeStamp(int64_to_8bytes(t))
            logger.debug(
                "Treating requested pack time %s as TID meaning %s",
                t, ts
            )
            best_pack_tid_int = t
            t = ts.timeTime()
        elif t < 0 or t >= time.time():
            # Packing for the current time or in the future means to pack
            # to the lastest commit in the database. This matters if not all
            # machine clocks are synchronized.
            best_pack_tid_int = self.packundo.MAX_TID - 1
        else:
            # Find the latest commit before or at the pack time.
            # Note that several TIDs will fit in the resolution of a time.time(),
            # so this is slightly ambiguous.
            requested_pack_ts = TimeStamp(*time.gmtime(t)[:5] + (t % 60,))
            requested_pack_tid = requested_pack_ts.raw()
            requested_pack_tid_int = bytes8_to_int64(requested_pack_tid)

            best_pack_tid_int = requested_pack_tid_int

        tid_int = self.packundo.choose_pack_transaction(best_pack_tid_int)

        if tid_int is None:
            logger.debug("all transactions before %s have already "
                         "been packed", time.ctime(t))
            return

        s = time.ctime(TimeStamp(int64_to_8bytes(tid_int)).timeTime())
        logger.info("Analyzing transactions committed %s or before (TID %d)",
                    s, tid_int)

        # In pre_pack, the adapter fills tables with
        # information about what to pack.  The adapter
        # must not actually pack anything yet.
        def get_references(state):
            """Return an iterable of the set of OIDs the given state refers to."""
            if not state:
                return ()

            return {bytes8_to_int64(oid) for oid in referencesf(state)}

        self.packundo.pre_pack(tid_int, get_references)
        logger.info("pack: pre-pack complete")
        return tid_int

    def __pack_to(self, tid_int):
        # Now pack. We'll get a callback for every oid/tid removed,
        # and we'll use that to keep caches consistent.
        # In the common case of using zodbpack, this will rewrite the
        # persistent cache on the machine running zodbpack.
        oids_removed = OID_SET_TYPE()
        def invalidate_cached_data(
                oid_int, tid_int,
                cache=self.cache,
                blob_invalidate=self.blobhelper.after_pack,
                keep_history=self.options.keep_history,
                oids=oids_removed
        ):
            # pylint:disable=dangerous-default-value
            # Flush the data from the local/global cache. It's quite likely that
            # a fair amount if it is now useless. We almost certainly want to
            # establish new checkpoints. The alternative is to clear out
            # data that we know we removed in the pack; we *do* keep track
            # of that, it's what's passed to the `packed_func`, so we could probably
            # piggyback on that in some fashion.
            #
            # Having consistent cache data became especially important
            # when loadSerial() began using the cache: Since that function
            # is allowed to return non-transactional data, it wouldn't be
            # great for it to return data that is no longer in the DB,
            # only the cache. I *think* this is only an issue in the tests,
            # that use the storage in a non-conventional way after packing,
            # by directly verifying that loadSerial() doesn't return data,
            # when in a real use we could only get there through detecting a conflict
            # in the database at commit time, with locks involved.
            cache.remove_cached_data(oid_int, tid_int)
            # Clean up blob files. This currently does nothing
            # if we're a blob cache, but it could.
            blob_invalidate(oid_int, tid_int)
            # If we're not keeping history, we need to remove all the cached
            # data for a particular OID, no matter what key it was under:
            # there was only one way to access it.
            if not keep_history:
                oids.add(oid_int)

        self.packundo.pack(tid_int,
                           packed_func=invalidate_cached_data)
        self.cache.remove_all_cached_data_for_oids(oids_removed)

    @contextmanager
    def _holding_pack_lock(self):
        lock_conn, lock_cursor = self.connmanager.open_for_pack_lock()
        try:
            self.locker.hold_pack_lock(lock_cursor)
            try:
                yield
            finally:
                self.locker.release_pack_lock(lock_cursor)
        finally:
            self.connmanager.rollback_and_close(lock_conn, lock_cursor)

    @writable_storage_method
    @metricmethod
    def pack(self, t, referencesf, prepack_only=False, skip_prepack=False):
        """Pack the storage. Holds the pack lock for the duration."""

        prepack_only = prepack_only or self.options.pack_prepack_only
        skip_prepack = skip_prepack or self.options.pack_skip_prepack

        if prepack_only and skip_prepack:
            raise ValueError('Pick either prepack_only or skip_prepack.')

        with self._holding_pack_lock():
            if not skip_prepack:
                tid_int = self.__pre_pack(t, referencesf)
            else:
                # Need to determine the tid_int from the pack_object table
                tid_int = self.packundo._find_pack_tid()

            if not prepack_only:
                self.__pack_to(tid_int)

        self.stats.large_database_change()

    @metricmethod
    def check_refs(self, referencesf):
        logger.info("pack: Beginning reference check.")
        with self._holding_pack_lock():
            tid_int = self.__pre_pack(None, referencesf)
            return self.packundo.check_refs(tid_int)
