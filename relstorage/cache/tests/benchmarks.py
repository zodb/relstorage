##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
from __future__ import print_function, absolute_import, division

from relstorage.options import Options

class MockOptions(Options):
    cache_module_name = ''
    cache_servers = ''
    cache_local_mb = 1
    cache_local_object_max = 16384
    cache_local_compression = 'zlib'
    cache_delta_size_limit = 10000
    cache_local_dir = None
    cache_local_dir_compress = False
    cache_local_dir_count = 1


import timeit
import statistics
try:
    import sys
    import cProfile, pstats
    if '--profile' not in sys.argv:
        raise ImportError
except ImportError:
    class cProfile(object):
        class Profile(object):
            def enable(self):
                pass
            def disable(self):
                pass
    class pstats(object):
        class Stats(object):
            def __init__(self, *args):
                pass
            def sort_stats(self, *args):
                return self
            def print_stats(self, *args):
                pass

NUMBER = 4
REPEAT_COUNT = 3

def run_func(func, number=NUMBER, repeat_count=REPEAT_COUNT):
    print("Timing func", func)
    pop_timer = timeit.Timer(func)
    pr = cProfile.Profile()
    pr.enable()
    pop_times = pop_timer.repeat(number=repeat_count)
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative')
    ps.print_stats(.4)

    return pop_times

def run_and_report_funcs(named_funcs, **kwargs):
    times = {}
    for name, func in named_funcs:
        times[name] = run_func(func, **kwargs)

    for name, time in sorted(times.items()):
        print(name, "average", statistics.mean(time), "stddev", statistics.stdev(time))


def local_benchmark():
    from relstorage.cache import LocalClient
    options = MockOptions()
    options.cache_local_mb = 100
    options.cache_local_compression = 'none'


    KEY_GROUP_SIZE = 400
    DATA_SIZE = 1024

    # With 1000 in a key group, and 1024 bytes of data, we produce
    # 909100 keys, and 930918400 = 887MB of data, which will overflow
    # a cache of 500 MB.

    # A group size of 100 produces 9100 keys with 9318400 = 8.8MB of data.
    # Likewise, group of 200 produces 36380 keys with 35.5MB of data.

    # Group size of 400 produces 145480 keys with 142MB of data.

    # Most of our time is spent in compression, it seems.
    # In the 8.8mb case, populating all the data with default compression
    # takes about 2.5-2.8s. Using no compression, it takes 0.38 to 0.42s.
    # Reading is the same at about 0.2s.


    with open('/dev/urandom', 'rb') as f:
        random_data = f.read(DATA_SIZE)

    key_groups = []
    key_groups.append([str(i) for i in range(KEY_GROUP_SIZE)])
    for i in range(1, KEY_GROUP_SIZE):
        keys = [str(i) + str(j) for j in range(KEY_GROUP_SIZE)]
        assert len(set(keys)) == len(keys)
        key_groups.append(keys)


    # Recent PyPy and Python 3.6 preserves iteration order of a dict
    # to match insertion order. If we use a dict for ALL_DATA, this
    # gives slightly different results due to the key lengths being
    # different and so things being ejected at slightly different
    # times (on PyPy, 8 key groups have *no* matches in read() using a dict,
    # while that doesn't occur in cPython 2.7/3.4). To
    # make this all line up the same, we preserve order everywhere by using
    # a list of tuples (sure enough, that change makes 8 groups go missing)
    # Alternately, if we sort by the hash of the key, we get the iteration order that
    # CPython used for a dict, making all groups of keys be found in read(). This
    # keeps the benchmark consistent

    ALL_DATA = {}
    for group in key_groups:
        for key in group:
            ALL_DATA[key] = random_data
    assert all(isinstance(k, str) for k in ALL_DATA)
    ALL_DATA = list(ALL_DATA.items())
    ALL_DATA.sort(key=lambda x: hash(x[0]))
    print(len(ALL_DATA), sum((len(v[1]) for v in ALL_DATA))/1024/1024)


    def do_times(client_type=LocalClient):
        client = client_type(options)
        print("Testing", type(client._bucket0._dict))

        def populate():
            for k, v in ALL_DATA:
                client.set(k, v)


        def populate_empty():
            c = LocalClient(options)
            for k, v in ALL_DATA:
                c.set(k, v)

        def read():
            # This is basically the worst-case scenario for a basic
            # segmented LRU: A repeating sequential scan, where no new
            # keys are added and all existing keys fit in the two parts of the
            # cache. Thus, entries just keep bouncing back and forth between
            # probation and protected. It so happens that this is our slowest
            # case.
            miss_count = 0
            for keys in key_groups:
                res = client.get_multi(keys)
                #assert len(res) == len(keys)
                if not res:
                    miss_count += 1
                    continue
                assert res.popitem()[1] == random_data

            if miss_count:
                print("Failed to get any keys in %d of %d groups"
                      % (miss_count, len(key_groups)))

            # import pprint
            # pprint.pprint(client._bucket0.stats())
            # print("Probation promotes", client._bucket0._probation.promote_count)
            # print("Probation demotes", client._bucket0._probation.demote_count)
            # print("Probation removes", client._bucket0._probation.remove_count)

        def mixed():
            hot_keys = key_groups[0]
            i = 0
            for k, v in ALL_DATA:
                i += 1
                client.set(k, v)
                if i == len(hot_keys):
                    client.get_multi(hot_keys)
                    i = 0


        run_and_report_funcs((('pop ', populate),
                              ('epop', populate_empty),
                              ('read', read),
                              ('mix ', mixed),))

    do_times()

def save_load_benchmark():
    from relstorage.cache.mapping import SizedLRUMapping as LocalClientBucket
    from relstorage.cache import persistence as _Loader

    import os
    import itertools

    import sys
    sys.setrecursionlimit(500000)
    bucket = LocalClientBucket(500*1024*1024)
    print("Testing", type(bucket._dict))


    size_dists = [100] * 800 + [300] * 500 + [1024] * 300 + [2048] * 200 + [4096] * 150

    with open('/dev/urandom', 'rb') as rnd:
        data = [rnd.read(x) for x in size_dists]
    data_iter = itertools.cycle(data)

    for j, datum in enumerate(data_iter):
        if len(datum) > bucket.limit or bucket.size + len(datum) > bucket.limit:
            break
        # To ensure the pickle memo cache doesn't just write out "use object X",
        # but distinct copies of the strings, we need to copy them
        bucket[str(j)] = datum[:-1] + b'x'
        # We need to get the item so its frequency goes up enough to be written
        # (this is while we're doing an aging at write time, which may go away).
        # Using an assert statement causes us to write nothing if -O is used.
        if bucket[str(j)] is datum:
            raise AssertionError()

    print("Len", len(bucket), "size", bucket.size)


    cache_pfx = "pfx"
    cache_options = MockOptions()
    cache_options.cache_local_dir = '/tmp'
    cache_options.cache_local_dir_compress = False

    fnames = set()

    def write():
        fname = _Loader.save_local_cache(cache_options, cache_pfx, bucket)
        fnames.add(fname)


    def load():
        b2 = LocalClientBucket(bucket.limit)
        _Loader.load_local_cache(cache_options, cache_pfx, b2)

    run_and_report_funcs( (('write', write),
                           ('read ', load)))
    for fname in fnames:
        os.remove(fname)

if __name__ == '__main__':
    import sys
    if '--localbench' in sys.argv:
        local_benchmark()
    elif '--iobench' in sys.argv:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        save_load_benchmark()
