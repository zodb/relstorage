======================
 Client Cache Tracing
======================

.. note:: This document contains information based on `the original
          <https://github.com/zopefoundation/ZEO/blob/master/doc/zeo-client-cache-tracing.txt>`_.
          View that document for more information.

An important question for RelStorage users is: how large should the
RelStorage local cache be? RelStorage 2 (as of RelStorage 2.0b3) has a
new feature that lets you collect a trace of cache activity and tools
to analyze this trace, enabling you to make an informed decision about
the cache size.

Don't confuse the RelStorage local cache with the ZODB object cache.  The
RelStorage local cache is only used when an object is not in the ZODB object
cache; the RelStorage local cache avoids roundtrips to the database server.

Enabling Cache Tracing
======================

To enable cache tracing, you must use a persistent cache (specify a
``cache-local-dir`` name), and set the environment variable
``ZEO_CACHE_TRACE`` to a non-empty value. The path to the trace file
is derived from the path to the persistent cache directory and the
process's PID. If the file doesn't exist, RelStorage will try to
create it. If the file does exist, it's opened for appending (previous
trace information is not overwritten). If there are problems with the
file, a warning message is logged. To start or stop tracing, the
RelStorage instance must be closed and re-opened (usually this means
stopping and restarting the entire process).

.. tip:: If you are using only one client process per
         ``cache-local-dir``, set ``ZEO_CACHE_TRACE`` to ``single``.
         This will allow process restarts to be recorded in the trace
         file. Otherwise (if you are using multiple processes sharing
         the same cache directory or use a value different than
         ``single``, each process will use its own trace file.) Note
         that using a value of ``single`` with multiple processes is undefined.

The trace file can grow pretty quickly; on a moderately loaded server,
we observed it growing by 7 MB per hour. The file consists of binary
records, each 34 bytes long if 8-byte oids are in use. No sensitive
data are logged: data record sizes (but not data records), and binary
object and transaction ids are logged, but no object pickles, object
types or names, user names, transaction comments, access paths, or
machine information (such as machine name or IP address) is logged.

Analyzing a Cache Trace
=======================

The cache_stats.py command-line tool (``python -m
ZEO.scripts.cache_stats``) is the first-line tool to analyze a cache
trace. Its default output consists of two parts: a one-line summary of
essential statistics for each segment of 15 minutes, interspersed with
lines indicating client restarts, followed by a more detailed summary
of overall statistics.

The most important statistic is the "hit rate", a percentage indicating how
many requests to load an object could be satisfied from the cache.  Hit rates
around 70% are good.  90% is excellent.  If you see a hit rate under 60% you
can probably improve the cache performance (and hence your application
server's performance) by increasing the local cache size.  This is normally
configured using key ``cache-local-mb`` in the ``relstorage`` section of your
configuration file.  The default cache size is 10 MB, which is small.

.. note:: The trace file records *all* cache activity. If you are
          using Memcached servers (via the ``cache-servers`` setting),
          hits they produce will also be recorded. To isolate just the
          effects of the local cache, disable ``cache-servers``. On
          the other hand, comparing trace files from instances with
          Memcached enabled and it disabled can help show if the
          additional overhead produces enough extra hits to be
          worthwhile.

The cache_stats.py tool (``python -m ZEO.scripts.cache_stats``) shows
its command line syntax when invoked without arguments. The tracefile
argument can be a gzipped file if it has a .gz extension. It will be
read from stdin (assuming uncompressed data) if the tracefile argument
is '-'.

.. program-output:: python -m ZEO.scripts.cache_stats --help

Simulating Different Cache Sizes
================================

Based on a cache trace file, you can make a prediction of how well the
cache might do with a different cache size. The cache_simul.py
(``python -m ZEO.scripts.cache_simul``) tool runs a simulation of the
ZEO client cache implementation based upon the events read from a
trace file. A new simulation is started each time the trace file
records a client restart event; if a trace file contains more than one
restart event, a separate line is printed for each simulation, and a
line with overall statistics is added at the end.

.. program-output:: python -m ZEO.scripts.cache_simul --help

Example, assuming the trace file is in /tmp/cachetrace.log::

    $ python -m ZEO.scripts.cache_simul.py -s 4 /tmp/cachetrace.log
    CircularCacheSimulation, cache size 4,194,304 bytes
      START TIME  DURATION    LOADS     HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 22 22:22     39:09  3218856  1429329  24046  41517   44.4%   40776    99.8

This shows that with a 4 MB cache size, the cache hit rate is 44.4%, the
percentage 1429329 (number of cache hits) is of 3218856 (number of load
requests).  The cache simulated 40776 evictions, to make room for new object
states.  At the end, 99.8% of the bytes reserved for the cache file were in
use to hold object state (the remaining 0.2% consists of "holes", bytes freed
by object eviction and not yet reused to hold another object's state).

Let's try this again with an 8 MB cache::

    $ python -m ZEO.scripts.cache_simul.py -s 8 /tmp/cachetrace.log
    CircularCacheSimulation, cache size 8,388,608 bytes
      START TIME  DURATION    LOADS     HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 22 22:22     39:09  3218856  2182722  31315  41517   67.8%   40016   100.0

That's a huge improvement in hit rate, which isn't surprising since these are
very small cache sizes.  The default cache size is 20 MB, which is still on
the small side::

    $ python -m ZEO.scripts.cache_simul.py /tmp/cachetrace.log
    CircularCacheSimulation, cache size 20,971,520 bytes
      START TIME  DURATION    LOADS     HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 22 22:22     39:09  3218856  2982589  37922  41517   92.7%   37761    99.9

Again a very nice improvement in hit rate, and there's not a lot of room left
for improvement.  Let's try 100 MB::

    $ python -m ZEO.scripts.cache_simul.py -s 100 /tmp/cachetrace.log
    CircularCacheSimulation, cache size 104,857,600 bytes
      START TIME  DURATION    LOADS     HITS INVALS WRITES HITRATE  EVICTS   INUSE
    Jul 22 22:22     39:09  3218856  3218741  39572  41517  100.0%   22778   100.0

It's very unusual to see a hit rate so high.  The application here frequently
modified a very large BTree, so given enough cache space to hold the entire
BTree it rarely needed to ask the ZEO server for data:  this application
reused the same objects over and over.

More typical is that a substantial number of objects will be referenced only
once.  Whenever an object turns out to be loaded only once, it's a pure loss
for the cache:  the first (and only) load is a cache miss; storing the object
evicts other objects, possibly causing more cache misses; and the object is
never loaded again.  If, for example, a third of the objects are loaded only
once, it's quite possible for the theoretical maximum hit rate to be 67%, no
matter how large the cache.

The cache_simul.py script also contains code to simulate different cache
strategies.  Since none of these are implemented, and only the default cache
strategy's code has been updated to be aware of MVCC, these are not further
documented here.

Simulation Limitations
======================

The cache simulation is an approximation, and actual hit rate may be higher
or lower than the simulated result.  These are some factors that inhibit
exact simulation:

- *Important* The simulation tools were designed for ZEO and may not be
  accurate for RelStorage due to the different mechanisms
  of invalidation employed. Still, they may give a useful
  idea.

  Because of the invalidation differences, some trace files
  might cause the script to produce an ``AssertionError``. These can
  typically be ignored and commented out in the script itself to proceed.

- Each time a load of an object *O* in the trace file was a cache hit, but the
  simulated cache has evicted *O*, the simulated cache has no way to repair its
  knowledge about *O*.  This is more frequent when simulating caches smaller
  than the cache used to produce the trace file.  When a real cache suffers a
  cache miss, it asks the database server for the needed information about *O*, and
  saves *O* in the local cache.  The simulated cache doesn't have a database server
  to ask, and *O* continues to be absent in the simulated cache.  Further
  requests for *O* will continue to be simulated cache misses, although in a
  real cache they'll likely be cache hits.  On the other hand, the
  simulated cache doesn't need to evict any objects to make room for *O*, so it
  may enjoy further cache hits on objects a real cache would have evicted.
