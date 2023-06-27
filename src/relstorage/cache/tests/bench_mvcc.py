##############################################################################
#
# Copyright (c) 2021 Zope Foundation and Contributors.
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

# pragma: no cover
import sys

from relstorage.cache.mvcc import MVCCDatabaseCoordinator
from relstorage._mvcc import DetachableMVCCDatabaseViewer
from relstorage._compat import perf_counter

class BenchConnection(object):
    pass


class BenchCursor(object):

    current_tid = 0
    current_oid = 0


class BenchPoller(object):

    OBJ_PER_TXN = 250
    SAME_TID = True
    REPLACE_OBJECTS = False

    def get_current_tid(self, cursor):
        cursor.current_tid += 1
        return cursor.current_tid

    def poll_invalidations(self, conn, cursor, polling_since):
        # pylint:disable=unused-argument
        cursor.current_tid += 1
        tid = cursor.current_tid
        # [(oid, tid)]
        change_iter = []
        for oid in range(cursor.current_oid, cursor.current_oid + self.OBJ_PER_TXN):
            change_iter.append((oid, tid))

        if not self.REPLACE_OBJECTS:
            cursor.current_oid = oid
        return change_iter, tid


class BenchAdapter(object):

    def __init__(self):
        self.poller = BenchPoller()

class BenchLocalClient(object):

    def delitems(self, oids_tids_to_del):
        # XXX: This is called, we should try using the real thing
        pass

    def freeze(self, obsolete_bucket):
        # XXX: This is called, we should try using the real thing
        pass

class BenchCache(DetachableMVCCDatabaseViewer):

    object_index = None
    local_client = None

    def __init__(self):
        super().__init__()
        self.adapter = BenchAdapter()
        self.local_client = BenchLocalClient()

def populate():

    mvcc = MVCCDatabaseCoordinator()
    viewer = BenchCache()
    old_viewers = [BenchCache() for _ in range(20)]
    conn = BenchConnection()
    cursor = BenchCursor()

    mvcc.register(viewer)
    for v in old_viewers:
        mvcc.register(v)
        mvcc.poll(v, conn, cursor)

    for i in range(10000):
        mvcc.poll(viewer, conn, cursor)
        for ix, viewer in enumerate(old_viewers):
            if i % ((ix + 1) * 50) == 0:
                mvcc.poll(viewer, conn, cursor)

    print(mvcc.stats())
    print()

def bench_multiunion_no_overlap(loops):
    from relstorage._inthashmap import OidTidMap # pylint:disable=no-name-in-module
    # 2000 maps of 250 unique oids
    # 29.3ms with the BTree sorting
    # 25.2ms with the stdlib sort/unique/erase approach, but copying into a new result
    #   vector.
    # 24.7ms when returning the vector in place.
    # Most of the time here is probably in the final C++->Python conversion.
    i = 0
    maps = []
    for _map_num in range(2000):
        x = OidTidMap()
        maps.append(x)
        for _ in range(250):
            i += 1
            x[i] = i

    duration = 0
    for _ in range(loops):
        begin = perf_counter()
        # pylint:disable=protected-access
        OidTidMap._multiunion(maps)
        duration += perf_counter() - begin

    return duration

def run_populate():
    import os
    import logging
    logging.basicConfig(level=logging.DEBUG)
    print("PID", os.getpid())
    begin = perf_counter()
    populate()
    end = perf_counter()
    print("Duration", end - begin)

    # Using UU BTrees for the OidTMap type:
    # 54.97s with 250 objs per txn, non-overlapping (172,558 oids)
    #  In this scenario, the keys() function takes the most time
    #  at 85%.
    #  There are 1 intersecting OIDs in the index and the oldest bucket.
    # 46.06s with 250 objs per txn, overlapping (250 oids)
    #  Keys() is still the biggest, also at 85%.
    #  All 250 keys intersect in the index and the oldest bucket.
    #
    # depth -- number of maps being multiunioned to get keys --- was 693
    # IN BOTH CASES
    #
    # Switching to Python dicts took the latter scenario to 21s, and
    # the former to 159s! Switching from a BTree to a simple Bucket
    # made the former scenario take 162s (essentially no change).
    #
    # In the BTrees case, we were taking a slow path through
    # multiunion() and iterating the input instead of directly
    # memcpy() it. This is because the fast path only works for
    # *exactly* the Bucket object. If we tweak that,
    # and make our Objectindex be built on Bucket, using the fast
    # path gets the big map down to 28.6s and the small map down to
    # 22s. (Sorting/reverse sorting/shuffling the change_iter fed
    # to the bucket apparently had no significant impact in overall
    # timing; it should, but keys() is still so dominant that it
    # didn't.)
    #
    # Reverting those changes and dropping the use of multiunion()
    # altogether in favor of converting to a Python set(),
    # the small case drops to 33s, while the large case
    # clocks in at a massive 209s.

def main():
    if len(sys.argv) == 2 and sys.argv[-1] == 'populate':
        run_populate()
    else:
        import pyperf
        runner = pyperf.Runner()
        runner.bench_time_func(
            'OidTidMap._multiunion distinct', bench_multiunion_no_overlap,
        )

if __name__ == '__main__':
    main()
