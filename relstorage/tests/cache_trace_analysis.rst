Check to make sure the cache analysis scripts work.

    >>> import time
    >>> timetime = time.time
    >>> now = 1278864701.5
    >>> time.time = lambda : now
    >>> import os
    >>> from ZODB.utils import p64, z64, u64
    >>> import relstorage.cache, sys
    >>> from relstorage.tests.test_cache import MockOptions, MockAdapter

    >>> os.environ["ZEO_CACHE_TRACE"] = 'single'
    >>> import random2 as random
    >>> random = random.Random(42)
    >>> history = []
    >>> serial = 1
    >>> for i in range(1000):
    ...     serial += 1
    ...     oid = random.randint(i+1000, i+6000)
    ...     history.append((b's', oid, serial,
    ...                     b'x'*random.randint(200,2000)))
    ...     for j in range(10):
    ...         oid = random.randint(i+1000, i+6000)
    ...         history.append((b'l', oid, serial,
    ...                        b'x'*random.randint(200,2000)))

    >>> def cache_run(name, size):
    ...     serial = 1
    ...     random.seed(42)
    ...     global now
    ...     now = 1278864701.5
    ...     options = MockOptions()
    ...     options.cache_local_mb = size*(1<<20)
    ...     options.cache_local_dir = '.'
    ...     options.cache_local_compression = 'zlib'
    ...     cache = relstorage.cache.StorageCache(MockAdapter(), options, name)
    ...     cache.checkpoints = (0, 0)
    ...     cache.tpc_begin()
    ...     data_tot = 0
    ...     for action, oid, serial, data in history:
    ...         data_tot += len(data)
    ...         now += 1
    ...         if action == b's':
    ...             cache.after_poll(None, cache.current_tid, serial, [(oid, serial)])
    ...             assert cache.current_tid == serial
    ...             cache.store_temp(oid, data)
    ...             cache.send_queue(p64(serial))
    ...             cache.adapter.mover.data[oid] = (data, serial)
    ...         else:
    ...             v = cache.load(None, oid)
    ...             if v[0] is None:
    ...                 cache.store_temp(oid, data)
    ...                 cache.adapter.mover.data[oid] = (data, serial)
    ...     cache.close()

    >>> cache_run('cache', 2)

    >>> import ZEO.scripts.cache_stats, ZEO.scripts.cache_simul

    >>> def ctime(t):
    ...     return time.asctime(time.gmtime(t-3600*4))
    >>> ZEO.scripts.cache_stats.ctime = ctime
    >>> ZEO.scripts.cache_simul.ctime = ctime

=======
 Stats
=======

    >>> ZEO.scripts.cache_stats.main(['relstorage-trace-cache.0.trace'])
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0      16     186    0.0%
    Jul 11 12:15-29     818      16      82     796    2.0%
    Jul 11 12:30-44     818      53      82     689    6.5%
    Jul 11 12:45-59     818      93      82     582   11.4%
    Jul 11 13:00-14     818     120      82     519   14.7%
    Jul 11 13:15-29     818     207      82     451   25.3%
    Jul 11 13:30-44     819     235      81     394   28.7%
    Jul 11 13:45-59     818     258      82     387   31.5%
    Jul 11 14:00-14     818     318      82     321   38.9%
    Jul 11 14:15-29     818     383      82     275   46.8%
    Jul 11 14:30-44     818     427      82     264   52.2%
    Jul 11 14:45-59     819     453      81     230   55.3%
    Jul 11 15:00-14     818     463      82     229   56.6%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 16,322 trace records (554,940 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  8,351 (51.2%), average size 1105 bytes
    Hit rate:   30.3% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              998  1c  invalidate (hit, saving non-current)
            6,972  20  load (miss)
            3,028  22  load (hit)
            5,323  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 2 -i 5 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 2,097,152 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      1    186    0.6%       0    10.1
    Jul 11 12:15   4:59     272      19      2    290    7.0%       0    26.3
    Jul 11 12:20   4:59     273      34      5    261   12.5%       0    40.0
    Jul 11 12:25   4:59     273      53      2    245   19.4%       0    54.3
    Jul 11 12:30   4:59     272      60      8    247   22.1%       0    67.0
    Jul 11 12:35   4:59     273      68      6    230   24.9%       0    79.7
    Jul 11 12:40   4:59     273      85      8    212   31.1%       0    91.0
    Jul 11 12:45   4:59     273      85      7    214   31.1%      66    99.2
    Jul 11 12:50   4:59     272     104      9    201   38.2%     204    98.9
    Jul 11 12:55   4:59     273     104      4    195   38.1%     187    99.1
    Jul 11 13:00   4:59     273      92     12    204   33.7%     210    99.3
    Jul 11 13:05   4:59     273     103      8    198   37.7%     192    98.9
    Jul 11 13:10   4:59     272      99     16    203   36.4%     205    99.2
    Jul 11 13:15   4:59     273      91     11    207   33.3%     219    98.7
    Jul 11 13:20   4:59     273      96      9    201   35.2%     209    99.2
    Jul 11 13:25   4:59     272      89     11    217   32.7%     216    99.1
    Jul 11 13:30   4:59     273      82     14    216   30.0%     217    99.1
    Jul 11 13:35   4:59     273     101      9    198   37.0%     192    99.5
    Jul 11 13:40   4:59     273      92      6    208   33.7%     213    99.4
    Jul 11 13:45   4:59     272      80      6    223   29.4%     220    99.3
    Jul 11 13:50   4:59     273      81      8    217   29.7%     212    99.2
    Jul 11 13:55   4:59     273      86     11    215   31.5%     209    98.8
    Jul 11 14:00   4:59     273      95     11    204   34.8%     186    99.3
    Jul 11 14:05   4:59     272      93     11    209   34.2%     211    99.2
    Jul 11 14:10   4:59     273     110      6    188   40.3%     195    98.8
    Jul 11 14:15   4:59     273      91      9    206   33.3%     206    99.2
    Jul 11 14:20   4:59     272      85     16    219   31.2%     214    99.3
    Jul 11 14:25   4:59     273      89      8    209   32.6%     223    99.3
    Jul 11 14:30   4:59     273      96     12    205   35.2%     217    99.3
    Jul 11 14:35   4:59     273      90     10    209   33.0%     209    99.3
    Jul 11 14:40   4:59     272     106     10    196   39.0%     200    98.8
    Jul 11 14:45   4:59     273      80      8    219   29.3%     230    99.0
    Jul 11 14:50   4:59     273      99      8    201   36.3%     200    99.0
    Jul 11 14:55   4:59     273      87      8    211   31.9%     204    99.4
    Jul 11 15:00   4:59     272      98      8    205   36.0%     213    99.4
    Jul 11 15:05   4:59     273      93     11    206   34.1%     198    99.2
    Jul 11 15:10   4:59     273      96     11    204   35.2%     185    99.1
    Jul 11 15:15      1       2       1      0      1   50.0%       0    99.2
    --------------------------------------------------------------------------
    Jul 11 12:45 2:30:01    8184    2794    288   6209   34.1%    6062    99.2

    >>> cache_run('cache4', 4)

    >>> ZEO.scripts.cache_stats.main('relstorage-trace-cache4.0.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0      16     186    0.0%
    Jul 11 12:15-29     818      16      82     796    2.0%
    Jul 11 12:30-44     818      53      82     689    6.5%
    Jul 11 12:45-59     818      93      82     582   11.4%
    Jul 11 13:00-14     818     120      82     519   14.7%
    Jul 11 13:15-29     818     207      82     451   25.3%
    Jul 11 13:30-44     819     235      81     394   28.7%
    Jul 11 13:45-59     818     258      82     387   31.5%
    Jul 11 14:00-14     818     318      82     321   38.9%
    Jul 11 14:15-29     818     383      82     275   46.8%
    Jul 11 14:30-44     818     427      82     264   52.2%
    Jul 11 14:45-59     819     453      81     230   55.3%
    Jul 11 15:00-14     818     463      82     229   56.6%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 16,322 trace records (554,940 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  8,351 (51.2%), average size 1105 bytes
    Hit rate:   30.3% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              998  1c  invalidate (hit, saving non-current)
            6,972  20  load (miss)
            3,028  22  load (hit)
            5,323  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 4 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 4,194,304 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      1    186    0.6%       0     5.1
    Jul 11 12:15  14:59     818     106      9    796   13.0%       0    27.1
    Jul 11 12:30  14:59     818     213     22    689   26.0%       0    45.5
    Jul 11 12:45  14:59     818     321     23    582   39.2%       0    61.4
    Jul 11 13:00  14:59     818     380     43    519   46.5%       0    75.7
    Jul 11 13:15  14:59     818     450     44    451   55.0%       0    88.1
    Jul 11 13:30  14:59     819     503     47    394   61.4%      31    98.2
    Jul 11 13:45  14:59     818     496     49    406   60.6%     389    98.5
    Jul 11 14:00  14:59     818     515     48    384   63.0%     377    98.3
    Jul 11 14:15  14:59     818     528     58    371   64.5%     390    98.1
    Jul 11 14:30  14:59     818     511     51    391   62.5%     379    98.5
    Jul 11 14:45  14:59     819     529     53    368   64.6%     407    97.9
    Jul 11 15:00  14:59     818     512     49    390   62.6%     382    97.7
    Jul 11 15:15      1       2       2      0      0  100.0%       0    97.7
    --------------------------------------------------------------------------
    Jul 11 13:30 1:45:01    5730    3596    355   2704   62.8%    2355    97.7

    >>> cache_run('cache1', 1)

    >>> ZEO.scripts.cache_stats.main('relstorage-trace-cache1.0.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0      16     186    0.0%
    Jul 11 12:15-29     818      16      82     796    2.0%
    Jul 11 12:30-44     818      53      82     689    6.5%
    Jul 11 12:45-59     818      93      82     582   11.4%
    Jul 11 13:00-14     818     120      82     519   14.7%
    Jul 11 13:15-29     818     207      82     451   25.3%
    Jul 11 13:30-44     819     235      81     394   28.7%
    Jul 11 13:45-59     818     258      82     387   31.5%
    Jul 11 14:00-14     818     318      82     321   38.9%
    Jul 11 14:15-29     818     383      82     275   46.8%
    Jul 11 14:30-44     818     427      82     264   52.2%
    Jul 11 14:45-59     819     453      81     230   55.3%
    Jul 11 15:00-14     818     463      82     229   56.6%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 16,322 trace records (554,940 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  8,351 (51.2%), average size 1105 bytes
    Hit rate:   30.3% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              998  1c  invalidate (hit, saving non-current)
            6,972  20  load (miss)
            3,028  22  load (hit)
            5,323  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 1 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 1,048,576 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      1    186    0.6%       0    20.2
    Jul 11 12:15  14:59     818     106      9    796   13.0%      85    99.6
    Jul 11 12:30  14:59     818     161     16    741   19.7%     729    99.6
    Jul 11 12:45  14:59     818     157      8    746   19.2%     746    99.1
    Jul 11 13:00  14:59     818     139     21    760   17.0%     770    99.4
    Jul 11 13:15  14:59     818     125     17    776   15.3%     782    99.4
    Jul 11 13:30  14:59     819     147     13    750   17.9%     745    99.4
    Jul 11 13:45  14:59     818     120     17    782   14.7%     764    99.5
    Jul 11 14:00  14:59     818     159     17    740   19.4%     726    99.6
    Jul 11 14:15  14:59     818     139     13    760   17.0%     790    99.5
    Jul 11 14:30  14:59     818     151     15    751   18.5%     756    99.1
    Jul 11 14:45  14:59     819     132     13    765   16.1%     768    99.5
    Jul 11 15:00  14:59     818     154     10    748   18.8%     723    99.4
    Jul 11 15:15      1       2       1      0      1   50.0%       1    99.3
    --------------------------------------------------------------------------
    Jul 11 12:15 3:00:01    9820    1691    169   9116   17.2%    8385    99.3

Cleanup:

    >>> del os.environ["ZEO_CACHE_TRACE"]
    >>> time.time = timetime
    >>> ZEO.scripts.cache_stats.ctime = time.ctime
    >>> ZEO.scripts.cache_simul.ctime = time.ctime
