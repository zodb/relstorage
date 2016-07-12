Check to make sure the cache analysis scripts work.

    >>> import time
    >>> timetime = time.time
    >>> now = 1278864701.5
    >>> time.time = lambda : now
    >>> import os
    >>> from ZODB.utils import p64
    >>> import ZEO.cache

    >>> os.environ["ZEO_CACHE_TRACE"] = 'yes'
    >>> import random2 as random
    >>> random = random.Random(42)
    >>> history = []
    >>> serial = 1
    >>> for i in range(1000):
    ...     serial += 1
    ...     oid = random.randint(i+1000, i+6000)
    ...     history.append((b's', p64(oid), p64(serial),
    ...                     b'x'*random.randint(200,2000)))
    ...     for j in range(10):
    ...         oid = random.randint(i+1000, i+6000)
    ...         history.append((b'l', p64(oid), p64(serial),
    ...                        b'x'*random.randint(200,2000)))

    >>> def cache_run(name, size):
    ...     serial = 1
    ...     random.seed(42)
    ...     global now
    ...     now = 1278864701.5
    ...     cache = ZEO.cache.ClientCache(name, size*(1<<20))
    ...     for action, oid, serial, data in history:
    ...         now += 1
    ...         if action == b's':
    ...             cache.invalidate(oid, serial)
    ...             cache.store(oid, serial, None, data)
    ...         else:
    ...             v = cache.load(oid)
    ...             if v is None:
    ...                 cache.store(oid, serial, None, data)
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

    >>> ZEO.scripts.cache_stats.main(['cache.trace'])
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     213      22     687   26.0%
    Jul 11 12:45-59     818     291      19     609   35.6%
    Jul 11 13:00-14     818     295      36     605   36.1%
    Jul 11 13:15-29     818     277      31     623   33.9%
    Jul 11 13:30-44     819     276      29     624   33.7%
    Jul 11 13:45-59     818     251      25     649   30.7%
    Jul 11 14:00-14     818     295      27     605   36.1%
    Jul 11 14:15-29     818     262      33     638   32.0%
    Jul 11 14:30-44     818     297      32     603   36.3%
    Jul 11 14:45-59     819     268      23     632   32.7%
    Jul 11 15:00-14     818     291      30     609   35.6%
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)

    >>> ZEO.scripts.cache_stats.main('-q cache.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)

    >>> ZEO.scripts.cache_stats.main('-v cache.trace'.split())
    ... # doctest: +ELLIPSIS
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11:41 00 '' 0000000000000000 0000000000000000 -
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11:42 10             1065 0000000000000002 0000000000000000 -
    Jul 11 12:11:42 52             1065 0000000000000002 0000000000000000 - 245
    Jul 11 12:11:43 20              947 0000000000000000 0000000000000000 -
    Jul 11 12:11:43 52              947 0000000000000002 0000000000000000 - 602
    Jul 11 12:11:44 20             124b 0000000000000000 0000000000000000 -
    Jul 11 12:11:44 52             124b 0000000000000002 0000000000000000 - 1418
    ...
    Jul 11 15:14:55 52             10cc 00000000000003e9 0000000000000000 - 1306
    Jul 11 15:14:56 20             18a7 0000000000000000 0000000000000000 -
    Jul 11 15:14:56 52             18a7 00000000000003e9 0000000000000000 - 1610
    Jul 11 15:14:57 22             18b5 000000000000031d 0000000000000000 - 1636
    Jul 11 15:14:58 20              b8a 0000000000000000 0000000000000000 -
    Jul 11 15:14:58 52              b8a 00000000000003e9 0000000000000000 - 838
    Jul 11 15:14:59 22             1085 0000000000000357 0000000000000000 - 217
    Jul 11 15:00-14     818     291      30     609   35.6%
    Jul 11 15:15:00 22             1072 000000000000037e 0000000000000000 - 204
    Jul 11 15:15:01 20             16c5 0000000000000000 0000000000000000 -
    Jul 11 15:15:01 52             16c5 00000000000003e9 0000000000000000 - 1712
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)

    >>> ZEO.scripts.cache_stats.main('-h cache.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     213      22     687   26.0%
    Jul 11 12:45-59     818     291      19     609   35.6%
    Jul 11 13:00-14     818     295      36     605   36.1%
    Jul 11 13:15-29     818     277      31     623   33.9%
    Jul 11 13:30-44     819     276      29     624   33.7%
    Jul 11 13:45-59     818     251      25     649   30.7%
    Jul 11 14:00-14     818     295      27     605   36.1%
    Jul 11 14:15-29     818     262      33     638   32.0%
    Jul 11 14:30-44     818     297      32     603   36.3%
    Jul 11 14:45-59     819     268      23     632   32.7%
    Jul 11 15:00-14     818     291      30     609   35.6%
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)
    <BLANKLINE>
    Histogram of object load frequency
    Unique oids: 4,585
    Total loads: 10,000
    loads objects   %obj  %load   %cum
        1   1,645  35.9%  16.4%  16.4%
        2   1,465  32.0%  29.3%  45.8%
        3     809  17.6%  24.3%  70.0%
        4     430   9.4%  17.2%  87.2%
        5     167   3.6%   8.3%  95.6%
        6      49   1.1%   2.9%  98.5%
        7      12   0.3%   0.8%  99.3%
        8       7   0.2%   0.6%  99.9%
        9       1   0.0%   0.1% 100.0%

    >>> ZEO.scripts.cache_stats.main('-s cache.trace'.split())
    ... # doctest: +ELLIPSIS
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     213      22     687   26.0%
    Jul 11 12:45-59     818     291      19     609   35.6%
    Jul 11 13:00-14     818     295      36     605   36.1%
    Jul 11 13:15-29     818     277      31     623   33.9%
    Jul 11 13:30-44     819     276      29     624   33.7%
    Jul 11 13:45-59     818     251      25     649   30.7%
    Jul 11 14:00-14     818     295      27     605   36.1%
    Jul 11 14:15-29     818     262      33     638   32.0%
    Jul 11 14:30-44     818     297      32     603   36.3%
    Jul 11 14:45-59     819     268      23     632   32.7%
    Jul 11 15:00-14     818     291      30     609   35.6%
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)
    <BLANKLINE>
    Histograms of object sizes
    <BLANKLINE>
    <BLANKLINE>
    Unique sizes written: 1,782
          size   objs writes
           200      5      5
           201      4      4
           202      4      4
           203      1      1
           204      1      1
           205      6      6
           206      8      8
    ...
         1,995      1      2
         1,996      2      2
         1,997      1      1
         1,998      2      2
         1,999      2      4
         2,000      1      1

    >>> ZEO.scripts.cache_stats.main('-S cache.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     213      22     687   26.0%
    Jul 11 12:45-59     818     291      19     609   35.6%
    Jul 11 13:00-14     818     295      36     605   36.1%
    Jul 11 13:15-29     818     277      31     623   33.9%
    Jul 11 13:30-44     819     276      29     624   33.7%
    Jul 11 13:45-59     818     251      25     649   30.7%
    Jul 11 14:00-14     818     295      27     605   36.1%
    Jul 11 14:15-29     818     262      33     638   32.0%
    Jul 11 14:30-44     818     297      32     603   36.3%
    Jul 11 14:45-59     819     268      23     632   32.7%
    Jul 11 15:00-14     818     291      30     609   35.6%
    Jul 11 15:15-15       2       1       0       1   50.0%

    >>> ZEO.scripts.cache_stats.main('-X cache.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     213      22     687   26.0%
    Jul 11 12:45-59     818     291      19     609   35.6%
    Jul 11 13:00-14     818     295      36     605   36.1%
    Jul 11 13:15-29     818     277      31     623   33.9%
    Jul 11 13:30-44     819     276      29     624   33.7%
    Jul 11 13:45-59     818     251      25     649   30.7%
    Jul 11 14:00-14     818     295      27     605   36.1%
    Jul 11 14:15-29     818     262      33     638   32.0%
    Jul 11 14:30-44     818     297      32     603   36.3%
    Jul 11 14:45-59     819     268      23     632   32.7%
    Jul 11 15:00-14     818     291      30     609   35.6%
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)

    >>> ZEO.scripts.cache_stats.main('-i 5 cache.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-19     272      19       2     281    7.0%
    Jul 11 12:20-24     273      35       5     265   12.8%
    Jul 11 12:25-29     273      53       2     247   19.4%
    Jul 11 12:30-34     272      60       8     240   22.1%
    Jul 11 12:35-39     273      68       6     232   24.9%
    Jul 11 12:40-44     273      85       8     215   31.1%
    Jul 11 12:45-49     273      84       6     216   30.8%
    Jul 11 12:50-54     272     104       9     196   38.2%
    Jul 11 12:55-59     273     103       4     197   37.7%
    Jul 11 13:00-04     273      92      12     208   33.7%
    Jul 11 13:05-09     273     103       8     197   37.7%
    Jul 11 13:10-14     272     100      16     200   36.8%
    Jul 11 13:15-19     273      91      11     209   33.3%
    Jul 11 13:20-24     273      96       9     204   35.2%
    Jul 11 13:25-29     272      90      11     210   33.1%
    Jul 11 13:30-34     273      82      14     218   30.0%
    Jul 11 13:35-39     273     102       9     198   37.4%
    Jul 11 13:40-44     273      92       6     208   33.7%
    Jul 11 13:45-49     272      82       6     218   30.1%
    Jul 11 13:50-54     273      83       8     217   30.4%
    Jul 11 13:55-59     273      86      11     214   31.5%
    Jul 11 14:00-04     273      95      11     205   34.8%
    Jul 11 14:05-09     272      91      10     209   33.5%
    Jul 11 14:10-14     273     109       6     191   39.9%
    Jul 11 14:15-19     273      89       9     211   32.6%
    Jul 11 14:20-24     272      84      16     216   30.9%
    Jul 11 14:25-29     273      89       8     211   32.6%
    Jul 11 14:30-34     273      97      12     203   35.5%
    Jul 11 14:35-39     273      93      10     207   34.1%
    Jul 11 14:40-44     272     107      10     193   39.3%
    Jul 11 14:45-49     273      80       8     220   29.3%
    Jul 11 14:50-54     273     100       8     200   36.6%
    Jul 11 14:55-59     273      88       7     212   32.2%
    Jul 11 15:00-04     272      99       8     201   36.4%
    Jul 11 15:05-09     273      95      11     205   34.8%
    Jul 11 15:10-14     273      97      11     203   35.5%
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 18,876 trace records (641,776 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (58.3%), average size 1108 bytes
    Hit rate:   31.2% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              682  10  invalidate (miss)
              318  1c  invalidate (hit, saving non-current)
            6,875  20  load (miss)
            3,125  22  load (hit)
            7,875  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 2 -i 5 cache.trace'.split())
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

    >>> ZEO.scripts.cache_stats.main('cache4.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     213      22     687   26.0%
    Jul 11 12:45-59     818     322      23     578   39.4%
    Jul 11 13:00-14     818     381      43     519   46.6%
    Jul 11 13:15-29     818     450      44     450   55.0%
    Jul 11 13:30-44     819     503      47     397   61.4%
    Jul 11 13:45-59     818     496      49     404   60.6%
    Jul 11 14:00-14     818     516      48     384   63.1%
    Jul 11 14:15-29     818     532      59     368   65.0%
    Jul 11 14:30-44     818     516      51     384   63.1%
    Jul 11 14:45-59     819     529      53     371   64.6%
    Jul 11 15:00-14     818     515      49     385   63.0%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 16,918 trace records (575,204 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (65.0%), average size 1104 bytes
    Hit rate:   50.8% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              501  10  invalidate (miss)
              499  1c  invalidate (hit, saving non-current)
            4,917  20  load (miss)
            5,083  22  load (hit)
            5,917  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 4 cache.trace'.split())
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

    >>> ZEO.scripts.cache_stats.main('cache1.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       1       2     197    0.6%
    Jul 11 12:15-29     818     107       9     793   13.1%
    Jul 11 12:30-44     818     160      16     740   19.6%
    Jul 11 12:45-59     818     158       8     742   19.3%
    Jul 11 13:00-14     818     141      21     759   17.2%
    Jul 11 13:15-29     818     128      17     772   15.6%
    Jul 11 13:30-44     819     151      13     749   18.4%
    Jul 11 13:45-59     818     120      17     780   14.7%
    Jul 11 14:00-14     818     159      17     741   19.4%
    Jul 11 14:15-29     818     141      13     759   17.2%
    Jul 11 14:30-44     818     157      16     743   19.2%
    Jul 11 14:45-59     819     133      13     767   16.2%
    Jul 11 15:00-14     818     158      10     742   19.3%
    Jul 11 15:15-15       2       1       0       1   50.0%
    <BLANKLINE>
    Read 20,286 trace records (689,716 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (54.2%), average size 1105 bytes
    Hit rate:   17.1% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
              828  10  invalidate (miss)
              172  1c  invalidate (hit, saving non-current)
            8,285  20  load (miss)
            1,715  22  load (hit)
            9,285  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 1 cache.trace'.split())
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
