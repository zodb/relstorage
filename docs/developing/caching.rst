.. _caching-checkpoints:

==========================
 Caching with checkpoints
==========================

.. caution::

   This information only applies to RelStorage 3.0a8
   and earlier. 3.0a9 and later use a more precise system;
   see
   :class:`~relstorage.cache.interfaces.IStorageCacheMVCCDatabaseCoordinator`.


The caching strategy (both local in :class:`~relstorage.cache.storage_cache.StorageCache` and in
memcache) includes checkpoints. Checkpoint management is a bit
complex, but important for achieving a decent cache hit rate.

Checkpoints are 64 bit integer transaction IDs. Invariant: checkpoint0
is greater than or equal to checkpoint1, meaning that if they are
different, checkpoint0 is the most recent.

Cache key "$prefix:checkpoints" holds the current checkpoints (0 and 1).
If the cache key is missing, set it to the current tid, which means
checkpoint1 and checkpoint0 are at the same point.

Each StorageCache instance holds a Python map of {oid: tid} changes
after checkpoint0. This map is called delta_after0.  The map
will not be shared because each instance updates the map at
different times.

The (oid, tid) list retrieved from polling is sufficient for updating
delta_after0 directly, unless checkpoint0 has moved since the last poll.
Note that delta_after0 could have a tid more recent than the data
provided by polling, due to conflict resolution.  The combination
should use the latest tid from each map.

Also hold a map of {oid: tid} changes after checkpoint1 and before
or at checkpoint0.  It is called delta_after1.  This map is
immutable, so it would be nice to share it between threads.

When looking up an object in the cache, try to get:

    #. The state at delta_after0.

    #. The state at checkpoint0.

    #.  The state at delta_after1.

    #. The state at checkpoint1.

    #. The state from the database.

    If the retrieved state is older than checkpoint0, but it
    was not retrieved from checkpoint0, cache it at checkpoint0.
    Thus if we get data from delta_after1 or checkpoint1, we should
    copy it to checkpoint0.

The current time is ignored; we only care about transaction
timestamps. In a sense, time is frozen until the next transaction
commit. This should have a side effect of making databases that don't
change often extremely cacheable.

After polling, check the number of objects now held in delta_after0. If
it is beyond a threshold (perhaps 10k), suggest that future polls use
new checkpoints. Update "$prefix:checkpoints".

Checkpoint values stay constant within a transaction. Even if the
transaction takes hours and its data is stale, it should keep trying to
retrieve from the tids specified in delta_after(0|1) and
checkpoint(0|1); it can go ahead and cache what it retrieves. Who
knows, there might be yet another long running transaction that could
use the cached data.

If we load objects without polling, don't use the cache.

While polling, it is possible for checkpoint0 to be greater than the
latest transaction ID just polled, since other transactions might be
adding data very quickly.  If that happens, the instance should
ignore the checkpoint update, with the expectation that the new checkpoint
will be visible after the next update.

API Reference
=============

.. toctree::

   ../relstorage.cache.interfaces
   ../relstorage.cache.local_client
   ../relstorage.cache.persistence
   ../relstorage.cache.storage_cache
   ../relstorage.cache.trace
   ../relstorage.cache.mvcc
