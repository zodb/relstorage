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

# pylint:disable=unused-argument,redefined-variable-type

class MockOptions(Options):
    cache_module_name = ''
    cache_servers = ''
    cache_local_object_max = 16384
    cache_local_compression = 'none'
    cache_local_dir_count = 1


import timeit
import statistics # pylint:disable=import-error
try:
    import sys
    import cProfile
    import pstats
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

NUMBER = 3
REPEAT_COUNT = 4

def run_func(func, number=NUMBER, repeat_count=REPEAT_COUNT):
    print("Timing func", func)
    pop_timer = timeit.Timer(func)
    pr = cProfile.Profile()
    pr.enable()
    pop_times = pop_timer.repeat(number=number, repeat=repeat_count)
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative')
    ps.print_stats(.4)

    return pop_times

def run_and_report_funcs(named_funcs, **kwargs):
    times = {}
    for name, func in named_funcs:
        times[name] = run_func(func, **kwargs)

    for name, _time in sorted(times.items()):
        print(name, "average", statistics.mean(_time), "stddev", statistics.stdev(_time))


def local_benchmark():
    from relstorage.cache.local_client import LocalClient
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

        def mixed_for_stats():
            # This is a trivial function that simulates the way
            # new keys can come in over time as we reset our checkpoints.
            # (Actually, it mostly shows our superiority over the plain LRU;
            # that one scored a 0.0 hit ratio, where our segmented LRU scores 1.0)
            client.reset_stats()
            hot_keys = key_groups[0]
            i = 0
            for _k, v in ALL_DATA:
                i += 1
                client._bucket0[str(i)] = v


            client.get_multi(hot_keys)

            print("Hit ratio", client.stats()['ratio'])

        run_and_report_funcs((('pop ', populate),
                              ('epop', populate_empty),
                              ('read', read),
                              ('mix ', mixed),))
        mixed_for_stats()
    do_times()


import os
import os.path
import time
import random
from collections import namedtuple
StorageRecord = namedtuple('Record', ['asu', 'lba', 'size', 'opcode', 'ts'])

class StorageTraceSimulator(object):
    # Text trace files can be obtained from http://traces.cs.umass.edu/index.php/Storage/Storage
    # Binary trace files can be obtained from https://github.com/cache2k/cache2k-benchmark

    def _open_file(self, filename, mode='r'):
        if filename.endswith('.bz2'):
            import bz2
            f = bz2.BZ2File(filename, mode)
        else:
            f = open(filename, mode)
        return f

    def _read_binary_records(self, filename, num_clients=8, write_pct=.30,
                             mean_size=10000, stddev_size=512):
        # pylint:disable=too-many-locals
        import struct
        keys = []
        i = 0
        with self._open_file(filename, 'rb') as f:
            while True:
                key = f.read(4)
                if not key:
                    break
                key = struct.unpack(">I", key)[0]
                key = str(key)
                keys.append((i, key))
                i += 1

        random.seed("read_binary_records")
        write_keys = set(random.sample(keys, int(len(keys) * write_pct)))

        records = []
        for key in keys:
            size = int(random.normalvariate(mean_size, stddev_size))
            opcode = 'r'
            if key in write_keys:
                opcode = 'w'
            asu = 1 if num_clients == 1 else random.randrange(num_clients)

            records.append(StorageRecord(asu, key[1], size, opcode, 0.0))
        return records

    def _read_text_records(self, filename):
        from relstorage._compat import intern as _intern

        records = []
        with self._open_file(filename) as f:
            for line in f:
                line = line.decode('ascii') if isinstance(line, bytes) and str is not bytes else line
                fields = [x.strip() for x in line.split(",")]
                fields[0] = int(fields[0]) # asu
                try:
                    fields[1] = _intern(fields[1]) # lba
                    fields[2] = int(fields[2]) # size
                    fields[3] = _intern(fields[3].lower()) # opcode
                    fields[4] = float(fields[4]) # ts
                except IndexError:
                    print("Invalid line", line)
                    continue

                records.append(StorageRecord(*fields[:5]))

        return records

    def read_records(self, filename):
        if filename.endswith(".trace"):
            return self._read_binary_records(filename)
        return self._read_text_records(filename)

    def _report_one(self, stats, f, cache_local_mb, begin_time, end_time):
        stats['time'] = end_time - begin_time
        print("{:15s} {:>5s} {:>7s} {:>7s} {:>5s}".format("File", "Limit", "Size", "Time", "Hits"))
        print("{:15s} {:5d} {:7.2f} {:7.2f} {:.3f}".format(os.path.basename(f), cache_local_mb,
                                                           stats['bytes'] / 1024 / 1024, stats['time'],
                                                           stats['ratio']))

    def _simulate_local(self, records, cache_local_mb, f):
        from relstorage.cache.local_client import LocalClient
        options = MockOptions()
        options.cache_local_mb = cache_local_mb
        options.cache_local_compression = 'none'
        client = LocalClient(options)

        now = time.time()
        for record in records:
            key = record.lba

            if record.opcode == 'r':
                data = client.get(key)
                if data is None:
                    # Fill it in from the backend
                    client.set(key, b'r' * record.size)
            else:
                assert record.opcode == 'w'
                client.set(key, b'x' * record.size)

        done = time.time()
        stats = client.stats()
        self._report_one(stats, f, cache_local_mb, now, done)

        return stats

    def _simulate_storage(self, records, cache_local_mb, f):
        # pylint:disable=too-many-locals
        from relstorage.cache.storage_cache import StorageCache
        from relstorage.cache.tests.test_cache import MockAdapter
        from ZODB.utils import p64

        TRANSACTION_SIZE = 10

        options = MockOptions()
        options.cache_local_mb = cache_local_mb
        options.cache_local_compression = 'none'
        #options.cache_delta_size_limit = 30000
        adapter = MockAdapter()

        # Populate the backend with data, all as of tid 1 Use the size
        # for the first time we see the data, just like
        # _simulate_local does. If we choose a small fixed size, we
        # get much better hit rates than _simulate_local If we use the
        # actual size of the first time we see each record, we use an
        # insane amount of memory even interning the strings
        # (WebSearch3 requires 12GB of memory), so we create just the biggest value
        # and then take memoryviews of it to avoid any copies.

        max_size = 0
        first_sizes = {}
        for record in records:
            max_size = max(record.size, max_size)
            if record.lba in first_sizes:
                continue
            first_sizes[record.lba] = record.size

        # Create one very big value, and then use subviews of a memoryview to reference
        # the same memory

        max_size = max(first_sizes.values())
        biggest_value = b'i' * max_size
        biggest_value = memoryview(biggest_value)

        for lba, size in first_sizes.items():
            oid = int(lba)
            adapter.mover.data[oid] = (biggest_value[:size], 1)
            assert len(adapter.mover.data[oid][0]) == size
        assert len(adapter.mover.data) == len(first_sizes)
        root_cache = StorageCache(adapter, options, None)

        if '--store-trace' in sys.argv:
            class RecordingCache(object):

                def __init__(self, cache):
                    self._cache = cache
                    self.operations = []

                def set_multi(self, data):
                    for k, v in data.items():
                        self.operations.append(('w', k, len(v)))
                    return self._cache.set_multi(data)

                def set(self, k, v):
                    self.operations.append(('w', k, len(v)))
                    return self._cache.set(k, v)

                def get(self, k):
                    self.operations.append(('r', k, -1))
                    return self._cache.get(k)

                def get_multi(self, keys):
                    for k in keys:
                        self.operations.append(('r', k, -1))
                    return self._cache.get_multi(keys)

                def stats(self):
                    return self._cache.stats()

            local_client = root_cache.clients_local_first[0]
            local_client = RecordingCache(local_client)
            root_cache.clients_local_first[0] = local_client
            root_cache.clients_global_first[0] = local_client

        # Initialize to the current TID
        current_tid_int = 2
        root_cache.after_poll(None, 1, current_tid_int, [])

        # Each ASU is a connection, so it has its own storage cache instance.
        asu_caches = {asu: root_cache.new_instance()
                      for asu
                      in set((x.asu for x in records))}

        for cache in asu_caches.values():
            cache.after_poll(None, 0, current_tid_int, [])
            cache.bm_current_tid = current_tid_int
            cache.bm_changes = {}

        now = time.time()

        for record in records:
            oid_int = int(record.lba)
            cache = asu_caches[record.asu]

            # Poll after a certain number of operations, or of we know we would get a
            # conflict.
            if current_tid_int - cache.bm_current_tid >= TRANSACTION_SIZE or oid_int in cache.bm_changes:
                cache.after_poll(None, cache.bm_current_tid, current_tid_int,
                                 cache.bm_changes.items())
                cache.bm_current_tid = current_tid_int
                cache.bm_changes.clear()

            if record.opcode == 'r':
                cache.load(None, oid_int)
            else:
                assert record.opcode == 'w'
                current_tid_int += 1
                cache.tpc_begin()
                new_state = biggest_value[:record.size]
                cache.store_temp(oid_int, new_state)
                adapter.mover.data[oid_int] = (new_state, current_tid_int)
                cache.after_tpc_finish(p64(current_tid_int))

                for cache in asu_caches.values():
                    cache.bm_changes[oid_int] = current_tid_int

        done = time.time()
        stats = root_cache.clients_local_first[0].stats()
        self._report_one(stats, f, cache_local_mb, now, done)

        if hasattr(root_cache.clients_local_first[0], 'operations'):
            with open(f + '.' + str(options.cache_local_mb) + '.ctrace', 'w') as f:
                for o in root_cache.clients_local_first[0].operations:
                    f.write("%s,%s,%d\n" % o)
        return stats

    def simulate(self, s_type='local'):
        meth = getattr(self, '_simulate_' + s_type)

        def _print(size, records):
            print("Simulating", len(records),
                  "operations (reads:", (len([x for x in records if x.opcode == 'r'])),
                  "writes:", (len([x for x in records if x.opcode == 'w'])), ")",
                  "to", len(set(x.lba for x in records)), "distinct keys",
                  "from", len(set((x.asu for x in records))), "connections",
                  "with cache limit", size)

        filename = sys.argv[2]
        filename = os.path.abspath(os.path.expanduser(filename))
        if os.path.isdir(filename):
            all_stats = []
            for f in sorted(os.listdir(filename)):
                records = self.read_records(os.path.join(filename, f))
                for size in (100, 512, 1024):
                    _print(size, records)
                    stats = meth(records, size, f)
                    all_stats.append((f, size, stats))

            print("{:15s} {:>5s} {:>7s} {:>7s} {:>5s}".format("File", "Limit", "Size", "Time", "Hits"))
            for f, size, stats in all_stats:
                print("{:15s} {:5d} {:7.2f} {:7.2f} {:.3f}".format(os.path.basename(f), size, stats['bytes'] / 1024 / 1024, stats['time'], stats['ratio']))

        else:
            size = int(sys.argv[3])
            records = self.read_records(filename)
            _print(size, records)
            pr = cProfile.Profile()
            pr.enable()
            meth(records, size, filename)
            pr.disable()
            ps = pstats.Stats(pr).sort_stats('cumulative')
            ps.print_stats(.4)


def save_load_benchmark():
    # pylint:disable=too-many-locals
    from relstorage.cache.mapping import SizedLRUMapping as LocalClientBucket
    from relstorage.cache import persistence as _Loader

    import itertools

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

    run_and_report_funcs((('write', write),
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
    elif '--simulatelocal' in sys.argv:
        StorageTraceSimulator().simulate('local')
    elif '--simulatestorage' in sys.argv:
        #import logging
        #logging.basicConfig(level=logging.DEBUG)

        StorageTraceSimulator().simulate('storage')
