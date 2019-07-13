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

from perfmetrics import metricmethod
from persistent.timestamp import TimeStamp

from ZODB.POSException import ReadOnlyError

from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage._compat import OID_SET_TYPE
from .util import storage_method

logger = __import__('logging').getLogger(__name__)

class Pack(object):

    __slots__ = (
        'options',
        'adapter',
        'blobhelper',
        'cache',
    )

    def __init__(self, options, adapter, blobhelper, cache):
        self.options = options
        self.adapter = adapter
        self.blobhelper = blobhelper
        self.cache = cache

    @storage_method
    @metricmethod
    def pack(self, t, referencesf, prepack_only=False, skip_prepack=False):
        """Pack the storage. Holds the pack lock for the duration."""
        # pylint:disable=too-many-branches,unused-argument
        # 'sleep' is a legacy argument, no longer used.
        if self.options.read_only:
            raise ReadOnlyError()

        prepack_only = prepack_only or self.options.pack_prepack_only
        skip_prepack = skip_prepack or self.options.pack_skip_prepack

        if prepack_only and skip_prepack:
            raise ValueError('Pick either prepack_only or skip_prepack.')

        def get_references(state):
            """Return an iterable of the set of OIDs the given state refers to."""
            if not state:
                return ()

            return {bytes8_to_int64(oid) for oid in referencesf(state)}

        # Use a private connection (lock_conn and lock_cursor) to
        # hold the pack lock.  Have the adapter open temporary
        # connections to do the actual work, allowing the adapter
        # to use special transaction modes for packing.
        adapter = self.adapter
        lock_conn, lock_cursor = adapter.connmanager.open()
        try:
            adapter.locker.hold_pack_lock(lock_cursor)
            try:
                if not skip_prepack:
                    # Find the latest commit before or at the pack time.
                    pack_point = TimeStamp(*time.gmtime(t)[:5] + (t % 60,)).raw()
                    tid_int = adapter.packundo.choose_pack_transaction(
                        bytes8_to_int64(pack_point))
                    if tid_int is None:
                        logger.debug("all transactions before %s have already "
                                     "been packed", time.ctime(t))
                        return

                    if prepack_only:
                        logger.info("pack: beginning pre-pack")

                    s = time.ctime(TimeStamp(int64_to_8bytes(tid_int)).timeTime())
                    logger.info("pack: analyzing transactions committed "
                                "%s or before", s)

                    # In pre_pack, the adapter fills tables with
                    # information about what to pack.  The adapter
                    # must not actually pack anything yet.
                    adapter.packundo.pre_pack(tid_int, get_references)
                else:
                    # Need to determine the tid_int from the pack_object table
                    tid_int = adapter.packundo._find_pack_tid()

                if prepack_only:
                    logger.info("pack: pre-pack complete")
                else:
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
                        cache.invalidate(oid_int, tid_int)
                        # Clean up blob files. This currently does nothing
                        # if we're a blob cache, but it could.
                        blob_invalidate(oid_int, tid_int)
                        # If we're not keeping history, we need to remove all the cached
                        # data for a particular OID, no matter what key it was under:
                        # there was only one way to access it.
                        if not keep_history:
                            oids.add(oid_int)

                    adapter.packundo.pack(tid_int,
                                          packed_func=invalidate_cached_data)
                    self.cache.invalidate_all(oids_removed)
            finally:
                adapter.locker.release_pack_lock(lock_cursor)
        finally:
            adapter.connmanager.rollback_and_close(lock_conn, lock_cursor)
