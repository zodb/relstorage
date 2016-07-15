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
    ...     options.share_local_cache = True
    ...     # We can interleave between instances
    ...     cache = relstorage.cache.StorageCache(MockAdapter(), options, name)
    ...     new_cache = cache.new_instance()
    ...     cache.checkpoints = new_cache.checkpoints = (0, 0)
    ...     new_cache.tpc_begin()
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
    ...             new_cache.current_tid = serial
    ...             v = new_cache.load(None, oid)
    ...             if v[0] is None:
    ...                 new_cache.store_temp(oid, data)
    ...                 new_cache.send_queue(p64(serial))
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
    Jul 11 12:11-14     180       0      16     198    0.0%
    Jul 11 12:15-29     818       5      82     895    0.6%
    Jul 11 12:30-44     818      28      82     872    3.4%
    Jul 11 12:45-59     818      72      82     828    8.8%
    Jul 11 13:00-14     818      85      82     815   10.4%
    Jul 11 13:15-29     818     169      82     731   20.7%
    Jul 11 13:30-44     819     194      81     706   23.7%
    Jul 11 13:45-59     818     223      82     677   27.3%
    Jul 11 14:00-14     818     278      82     622   34.0%
    Jul 11 14:15-29     818     337      82     563   41.2%
    Jul 11 14:30-44     818     388      82     512   47.4%
    Jul 11 14:45-59     819     404      81     496   49.3%
    Jul 11 15:00-14     818     434      82     466   53.1%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 19,380 trace records (658,912 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (56.8%), average size 1106 bytes
    Hit rate:   26.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              998  1c  invalidate (hit, saving non-current)
            7,381  20  load (miss)
            2,619  22  load (hit)
            8,381  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 2 -i 5 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 2,097,152 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      2    197    0.6%       0    10.7
    Jul 11 12:15   4:59     272      19      2    281    7.0%       0    26.4
    Jul 11 12:20   4:59     273      35      5    265   12.8%       0    40.4
    Jul 11 12:25   4:59     273      53      2    247   19.4%       0    54.8
    Jul 11 12:30   4:59     272      60      8    240   22.1%       0    67.1
    Jul 11 12:35   4:59     273      68      6    232   24.9%       0    79.8
    Jul 11 12:40   4:59     273      85      8    215   31.1%       0    91.4
    Jul 11 12:45   4:59     273      84      6    216   30.8%      77    99.1
    Jul 11 12:50   4:59     272     104      9    196   38.2%     196    98.9
    Jul 11 12:55   4:59     273     104      4    196   38.1%     188    99.1
    Jul 11 13:00   4:59     273      92     12    208   33.7%     213    99.3
    Jul 11 13:05   4:59     273     103      8    197   37.7%     190    99.0
    Jul 11 13:10   4:59     272     100     16    200   36.8%     203    99.2
    Jul 11 13:15   4:59     273      91     11    209   33.3%     222    98.7
    Jul 11 13:20   4:59     273      96      9    204   35.2%     210    99.2
    Jul 11 13:25   4:59     272      89     11    211   32.7%     212    99.1
    Jul 11 13:30   4:59     273      82     14    218   30.0%     220    99.1
    Jul 11 13:35   4:59     273     101      9    199   37.0%     191    99.5
    Jul 11 13:40   4:59     273      92      6    208   33.7%     214    99.4
    Jul 11 13:45   4:59     272      80      6    220   29.4%     217    99.3
    Jul 11 13:50   4:59     273      81      8    219   29.7%     214    99.2
    Jul 11 13:55   4:59     273      86     11    214   31.5%     208    98.8
    Jul 11 14:00   4:59     273      95     11    205   34.8%     188    99.3
    Jul 11 14:05   4:59     272      93     10    207   34.2%     207    99.3
    Jul 11 14:10   4:59     273     110      6    190   40.3%     198    98.8
    Jul 11 14:15   4:59     273      91      9    209   33.3%     209    99.1
    Jul 11 14:20   4:59     272      85     16    215   31.2%     210    99.3
    Jul 11 14:25   4:59     273      89      8    211   32.6%     226    99.3
    Jul 11 14:30   4:59     273      96     12    204   35.2%     214    99.3
    Jul 11 14:35   4:59     273      90     10    210   33.0%     213    99.3
    Jul 11 14:40   4:59     272     106     10    194   39.0%     196    98.8
    Jul 11 14:45   4:59     273      80      8    220   29.3%     230    99.0
    Jul 11 14:50   4:59     273      99      8    201   36.3%     202    99.0
    Jul 11 14:55   4:59     273      87      8    213   31.9%     205    99.4
    Jul 11 15:00   4:59     272      98      8    202   36.0%     211    99.3
    Jul 11 15:05   4:59     273      93     11    207   34.1%     198    99.2
    Jul 11 15:10   4:59     273      96     11    204   35.2%     184    99.2
    Jul 11 15:15      1       2       1      0      1   50.0%       1    99.2
    --------------------------------------------------------------------------
    Jul 11 12:45 2:30:01    8184    2794    286   6208   34.1%    6067    99.2

    >>> cache_run('cache4', 4)

    >>> ZEO.scripts.cache_stats.main('relstorage-trace-cache4.0.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0      16     198    0.0%
    Jul 11 12:15-29     818       5      82     895    0.6%
    Jul 11 12:30-44     818      28      82     872    3.4%
    Jul 11 12:45-59     818      72      82     828    8.8%
    Jul 11 13:00-14     818      85      82     815   10.4%
    Jul 11 13:15-29     818     169      82     731   20.7%
    Jul 11 13:30-44     819     194      81     706   23.7%
    Jul 11 13:45-59     818     223      82     677   27.3%
    Jul 11 14:00-14     818     278      82     622   34.0%
    Jul 11 14:15-29     818     337      82     563   41.2%
    Jul 11 14:30-44     818     388      82     512   47.4%
    Jul 11 14:45-59     819     404      81     496   49.3%
    Jul 11 15:00-14     818     434      82     466   53.1%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 19,380 trace records (658,912 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (56.8%), average size 1106 bytes
    Hit rate:   26.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              998  1c  invalidate (hit, saving non-current)
            7,381  20  load (miss)
            2,619  22  load (hit)
            8,381  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 4 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 4,194,304 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      2    197    0.6%       0     5.4
    Jul 11 12:15  14:59     818     107      9    793   13.1%       0    27.4
    Jul 11 12:30  14:59     818     213     22    687   26.0%       0    45.7
    Jul 11 12:45  14:59     818     322     23    578   39.4%       0    61.4
    Jul 11 13:00  14:59     818     381     43    519   46.6%       0    75.8
    Jul 11 13:15  14:59     818     450     44    450   55.0%       0    88.2
    Jul 11 13:30  14:59     819     503     47    397   61.4%      36    98.2
    Jul 11 13:45  14:59     818     496     49    404   60.6%     388    98.5
    Jul 11 14:00  14:59     818     515     48    385   63.0%     376    98.3
    Jul 11 14:15  14:59     818     529     58    371   64.7%     391    98.1
    Jul 11 14:30  14:59     818     511     51    389   62.5%     376    98.5
    Jul 11 14:45  14:59     819     529     53    371   64.6%     410    97.9
    Jul 11 15:00  14:59     818     512     49    388   62.6%     379    97.7
    Jul 11 15:15      1       2       2      0      0  100.0%       0    97.7
    --------------------------------------------------------------------------
    Jul 11 13:30 1:45:01    5730    3597    355   2705   62.8%    2356    97.7

    >>> cache_run('cache1', 1)

    >>> ZEO.scripts.cache_stats.main('relstorage-trace-cache1.0.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0      16     198    0.0%
    Jul 11 12:15-29     818       5      82     895    0.6%
    Jul 11 12:30-44     818      28      82     872    3.4%
    Jul 11 12:45-59     818      72      82     828    8.8%
    Jul 11 13:00-14     818      85      82     815   10.4%
    Jul 11 13:15-29     818     169      82     731   20.7%
    Jul 11 13:30-44     819     194      81     706   23.7%
    Jul 11 13:45-59     818     223      82     677   27.3%
    Jul 11 14:00-14     818     278      82     622   34.0%
    Jul 11 14:15-29     818     337      82     563   41.2%
    Jul 11 14:30-44     818     388      82     512   47.4%
    Jul 11 14:45-59     819     404      81     496   49.3%
    Jul 11 15:00-14     818     434      82     466   53.1%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 19,380 trace records (658,912 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (56.8%), average size 1106 bytes
    Hit rate:   26.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              998  1c  invalidate (hit, saving non-current)
            7,381  20  load (miss)
            2,619  22  load (hit)
            8,381  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 1 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 1,048,576 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      2    197    0.6%       0    21.5
    Jul 11 12:15  14:59     818     107      9    793   13.1%      96    99.6
    Jul 11 12:30  14:59     818     160     16    740   19.6%     724    99.6
    Jul 11 12:45  14:59     818     158      8    742   19.3%     741    99.2
    Jul 11 13:00  14:59     818     140     21    760   17.1%     771    99.5
    Jul 11 13:15  14:59     818     125     17    775   15.3%     781    99.6
    Jul 11 13:30  14:59     819     147     13    753   17.9%     748    99.5
    Jul 11 13:45  14:59     818     120     17    780   14.7%     763    99.5
    Jul 11 14:00  14:59     818     159     17    741   19.4%     728    99.4
    Jul 11 14:15  14:59     818     141     13    759   17.2%     787    99.6
    Jul 11 14:30  14:59     818     150     15    750   18.3%     755    99.2
    Jul 11 14:45  14:59     819     132     13    768   16.1%     771    99.5
    Jul 11 15:00  14:59     818     154     10    746   18.8%     723    99.2
    Jul 11 15:15      1       2       1      0      1   50.0%       0    99.3
    --------------------------------------------------------------------------
    Jul 11 12:15 3:00:01    9820    1694    169   9108   17.3%    8388    99.3

Cleanup:

    >>> del os.environ["ZEO_CACHE_TRACE"]
    >>> time.time = timetime
    >>> ZEO.scripts.cache_stats.ctime = time.ctime
    >>> ZEO.scripts.cache_simul.ctime = time.ctime
