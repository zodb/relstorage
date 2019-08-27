Check to make sure the cache analysis scripts work.

..

  >>> from relstorage.cache.tests.test_cache_stats import cache_run
  >>> import ZEO.scripts.cache_stats
  >>> import ZEO.scripts.cache_simul
  >>> cache_run('cache', 2)

=======
 Stats
=======

    >>> ZEO.scripts.cache_stats.main(['relstorage-trace-cache.0.trace'])
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0       0     198    0.0%
    Jul 11 12:15-29     818      16       0     884    2.0%
    Jul 11 12:30-44     818      53       0     847    6.5%
    Jul 11 12:45-59     818      93       0     807   11.4%
    Jul 11 13:00-14     818     120       0     780   14.7%
    Jul 11 13:15-29     818     207       0     693   25.3%
    Jul 11 13:30-44     819     235       0     665   28.7%
    Jul 11 13:45-59     818     258       0     642   31.5%
    Jul 11 14:00-14     818     318       0     582   38.9%
    Jul 11 14:15-29     818     383       0     517   46.8%
    Jul 11 14:30-44     818     427       0     473   52.2%
    Jul 11 14:45-59     819     453       0     447   55.3%
    Jul 11 15:00-14     818     463       0     437   56.6%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 17,973 trace records (611,074 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (61.2%), average size 1105 bytes
    Hit rate:   30.3% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
            6,972  20  load (miss)
            3,028  22  load (hit)
            7,972  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 2 -i 5 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 2,097,152 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      0    195    0.6%       0    10.6
    Jul 11 12:15   4:59     272      19      0    279    7.0%       0    26.3
    Jul 11 12:20   4:59     273      35      0    260   12.8%       0    39.9
    Jul 11 12:25   4:59     273      53      0    245   19.4%       0    54.1
    Jul 11 12:30   4:59     272      60      0    232   22.1%       0    66.0
    Jul 11 12:35   4:59     273      68      0    226   24.9%       0    78.4
    Jul 11 12:40   4:59     273      85      0    207   31.1%       0    89.5
    Jul 11 12:45   4:59     273      85      0    208   31.1%      36    99.1
    Jul 11 12:50   4:59     272     106      0    185   39.0%     187    99.0
    Jul 11 12:55   4:59     273     106      0    190   38.8%     179    99.2
    Jul 11 13:00   4:59     273      91      0    197   33.3%     210    99.3
    Jul 11 13:05   4:59     273     105      0    187   38.5%     181    99.0
    Jul 11 13:10   4:59     272     101      0    183   37.1%     181    98.8
    Jul 11 13:15   4:59     273      92      0    198   33.7%     220    98.4
    Jul 11 13:20   4:59     273      99      0    192   36.3%     193    99.1
    Jul 11 13:25   4:59     272      93      0    195   34.2%     199    98.8
    Jul 11 13:30   4:59     273      80      0    206   29.3%     207    98.8
    Jul 11 13:35   4:59     273      99      0    192   36.3%     182    99.2
    Jul 11 13:40   4:59     273      95      0    199   34.8%     198    99.4
    Jul 11 13:45   4:59     272      83      0    211   30.5%     215    98.7
    Jul 11 13:50   4:59     273      85      0    207   31.1%     205    99.2
    Jul 11 13:55   4:59     273      87      0    202   31.9%     194    98.6
    Jul 11 14:00   4:59     273      95      0    194   34.8%     180    98.9
    Jul 11 14:05   4:59     272      99      0    190   36.4%     196    98.8
    Jul 11 14:10   4:59     273     112      0    180   41.0%     172    98.8
    Jul 11 14:15   4:59     273      96      0    195   35.2%     193    99.1
    Jul 11 14:20   4:59     272      87      0    196   32.0%     196    99.3
    Jul 11 14:25   4:59     273      92      0    199   33.7%     221    98.9
    Jul 11 14:30   4:59     273     100      0    187   36.6%     197    98.9
    Jul 11 14:35   4:59     273      94      0    194   34.4%     181    99.4
    Jul 11 14:40   4:59     272     105      0    185   38.6%     192    98.9
    Jul 11 14:45   4:59     273      84      0    207   30.8%     216    99.4
    Jul 11 14:50   4:59     273      94      0    199   34.4%     205    99.1
    Jul 11 14:55   4:59     273      93      0    200   34.1%     198    99.3
    Jul 11 15:00   4:59     272     101      0    191   37.1%     190    99.2
    Jul 11 15:05   4:59     273      98      0    190   35.9%     189    99.0
    Jul 11 15:10   4:59     273      97      0    191   35.5%     164    99.3
    Jul 11 15:15      1       2       1      0      1   50.0%       2    99.2
    --------------------------------------------------------------------------
    Jul 11 12:45 2:30:01    8184    2855      0   5851   34.9%    5679    99.2

    >>> cache_run('cache4', 4)

    >>> ZEO.scripts.cache_stats.main('relstorage-trace-cache4.0.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0       0     198    0.0%
    Jul 11 12:15-29     818      16       0     884    2.0%
    Jul 11 12:30-44     818      53       0     847    6.5%
    Jul 11 12:45-59     818      93       0     807   11.4%
    Jul 11 13:00-14     818     120       0     780   14.7%
    Jul 11 13:15-29     818     207       0     693   25.3%
    Jul 11 13:30-44     819     235       0     665   28.7%
    Jul 11 13:45-59     818     258       0     642   31.5%
    Jul 11 14:00-14     818     318       0     582   38.9%
    Jul 11 14:15-29     818     383       0     517   46.8%
    Jul 11 14:30-44     818     427       0     473   52.2%
    Jul 11 14:45-59     819     453       0     447   55.3%
    Jul 11 15:00-14     818     463       0     437   56.6%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 17,973 trace records (611,074 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (61.2%), average size 1105 bytes
    Hit rate:   30.3% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
            6,972  20  load (miss)
            3,028  22  load (hit)
            7,972  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 4 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 4,194,304 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      0    195    0.6%       0     5.3
    Jul 11 12:15  14:59     818     107      0    784   13.1%       0    27.0
    Jul 11 12:30  14:59     818     213      0    665   26.0%       0    44.7
    Jul 11 12:45  14:59     818     322      0    555   39.4%       0    60.0
    Jul 11 13:00  14:59     818     381      0    476   46.6%       0    73.2
    Jul 11 13:15  14:59     818     450      0    406   55.0%       0    84.4
    Jul 11 13:30  14:59     819     503      0    350   61.4%       0    93.8
    Jul 11 13:45  14:59     818     509      0    339   62.2%     198    97.8
    Jul 11 14:00  14:59     818     535      0    315   65.4%     304    97.9
    Jul 11 14:15  14:59     818     558      0    280   68.2%     280    97.8
    Jul 11 14:30  14:59     818     537      0    309   65.6%     328    97.4
    Jul 11 14:45  14:59     819     550      0    300   67.2%     314    97.6
    Jul 11 15:00  14:59     818     542      0    304   66.3%     283    97.6
    Jul 11 15:15      1       2       2      0      0  100.0%       0    97.6
    --------------------------------------------------------------------------
    Jul 11 13:45 1:30:01    4911    3233      0   1847   65.8%    1707    97.6

    >>> cache_run('cache1', 1)

    >>> ZEO.scripts.cache_stats.main('relstorage-trace-cache1.0.trace'.split())
                       loads    hits  inv(h)  writes hitrate
    Jul 11 12:11-11       0       0       0       0     n/a
    Jul 11 12:11:41 ==================== Restart ====================
    Jul 11 12:11-14     180       0       0     198    0.0%
    Jul 11 12:15-29     818      16       0     884    2.0%
    Jul 11 12:30-44     818      53       0     847    6.5%
    Jul 11 12:45-59     818      93       0     807   11.4%
    Jul 11 13:00-14     818     120       0     780   14.7%
    Jul 11 13:15-29     818     207       0     693   25.3%
    Jul 11 13:30-44     819     235       0     665   28.7%
    Jul 11 13:45-59     818     258       0     642   31.5%
    Jul 11 14:00-14     818     318       0     582   38.9%
    Jul 11 14:15-29     818     383       0     517   46.8%
    Jul 11 14:30-44     818     427       0     473   52.2%
    Jul 11 14:45-59     819     453       0     447   55.3%
    Jul 11 15:00-14     818     463       0     437   56.6%
    Jul 11 15:15-15       2       2       0       0  100.0%
    <BLANKLINE>
    Read 17,973 trace records (611,074 bytes) in 0.0 seconds
    Versions:   0 records used a version
    First time: Sun Jul 11 12:11:41 2010
    Last time:  Sun Jul 11 15:15:01 2010
    Duration:   11,000 seconds
    Data recs:  11,000 (61.2%), average size 1105 bytes
    Hit rate:   30.3% (load hits / loads)
    <BLANKLINE>
            Count Code Function (action)
                1  00  _setup_trace (initialization)
            6,972  20  load (miss)
            3,028  22  load (hit)
            7,972  52  store (current, non-version)

    >>> ZEO.scripts.cache_simul.main('-s 1 relstorage-trace-cache.0.trace'.split())
    CircularCacheSimulation, cache size 1,048,576 bytes
      START TIME   DUR.   LOADS    HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 11 12:11   3:17     180       1      0    195    0.6%       0    21.3
    Jul 11 12:15  14:59     818     107      0    784   13.1%      81    99.7
    Jul 11 12:30  14:59     818     161      0    723   19.7%     710    99.4
    Jul 11 12:45  14:59     818     155      0    737   18.9%     741    99.3
    Jul 11 13:00  14:59     818     142      0    737   17.4%     749    99.4
    Jul 11 13:15  14:59     818     127      0    756   15.5%     762    99.5
    Jul 11 13:30  14:59     819     146      0    740   17.8%     729    99.5
    Jul 11 13:45  14:59     818     122      0    761   14.9%     747    99.6
    Jul 11 14:00  14:59     818     160      0    725   19.6%     716    99.5
    Jul 11 14:15  14:59     818     143      0    742   17.5%     765    99.7
    Jul 11 14:30  14:59     818     153      0    733   18.7%     744    99.2
    Jul 11 14:45  14:59     819     131      0    756   16.0%     751    99.7
    Jul 11 15:00  14:59     818     155      0    735   18.9%     710    99.6
    Jul 11 15:15      1       2       1      0      1   50.0%       1    99.6
    --------------------------------------------------------------------------
    Jul 11 12:15 3:00:01    9820    1703      0   8930   17.3%    8206    99.6
