====================
 RelStorage Options
====================

Specify these options in zope.conf, as parameters for the
:class:`relstorage.storage.RelStorage` constructor, or as attributes of a
:class:`relstorage.options.Options` instance. In the latter two cases, use
underscores instead of dashes in the option names.

General Settings
================

name
        The name of the storage.

        Defaults to a descriptive name that includes the adapter
        connection parameters, except the database password.

read-only
        If true, only reads may be executed against the storage.

        The default is false.

keep-history
        If this option is set to true (the default), the adapter
        will create and use a history-preserving database schema
        (like FileStorage). A history-preserving schema supports
        ZODB-level undo, but also grows more quickly and requires extensive
        packing on a regular basis.

        .. warning::

           Using ZODB's time-travel feature to get historical
           connections (``DB.open(before=a_timestamp)``) will result
           in RDBMS connections being left open in a transaction until
           they are removed from the ZODB historical connection pool
           and closed. PostgreSQL refers to this state as "idle in
           transaction", and it ties up certain MVCC resources on the
           RDBMS server.

        If this option is set to false, the adapter will create and
        use a history-free database schema. Undo will not be supported,
        but the database will not grow as quickly. The database will
        still require regular garbage collection (which is accessible
        through the database pack mechanism.)

        .. warning::

           This option must not change once the database schema has
           been installed, because the schemas for history-preserving and
           history-free storage are different. RelStorage will refuse
           to initialize if it detects this value has been altered.

           If you want to convert between a history-preserving and a
           history-free database, use the :doc:`zodbconvert` utility
           to copy to a new database.

commit-lock-timeout
        During commit, RelStorage acquires a database-wide lock. This
        option specifies how long to wait for the lock before
        failing the attempt to commit. The default is 30 seconds.

        .. versionchanged:: 2.0.0b1
           Add support for lock timeouts on PostgreSQL 9.3 and above.
           Previously no PostgreSQL version supported timeouts.

        .. versionchanged:: 3.0a5
           With the implementation of ZODB 5's parallel commit
           feature, this option has changed slightly. Now, individual
           rows will be locked instead of locking entire tables or
           using advisory locks. This timeout now controls the locking
           of individual rows. Because rows are locked in specific
           orders, in extreme cases of overlapping transactions,
           a transaction might wait *almost* this length of time
           multiple times. However, it will always be making forward
           progress and will still raise an exception if it cannot
           make forward progress after this amount of time.

           There is still a single row locked by all transactions, but
           that is done after most commit work has been accomplished
           and is hopefully fast. The exception is if
           ``shared-blob-dir`` is true (deprecated), in which case the
           single row is locked much earlier.

commit-lock-id
        During commit, RelStorage acquires a database-wide lock. This
        option specifies the lock ID.

        This option currently applies only to the Oracle adapter.

create-schema
        Normally, RelStorage will create or update the database schema on
        start-up.

        Set this option to false if you need to connect to a
        RelStorage database without automatic creation or updates.

Blobs
=====

blob-dir
        If supplied, the storage will provide ZODB blob support; this
        option specifies the name of the directory to hold blob data.
        The directory will be created if it does not exist.

        If no value (or an empty value) is provided, then *no blob
        support will be provided*.

shared-blob-dir
        When this option is false (the default, and recommended), the
        blob directory is treated as a cache. It should be on a local
        filesystem that properly supports file locks. It *may* be shared
        among clients on that same machine. Blob data is stored safely
        in the relational database, and the blob directory holds a cache of
        blobs.

        If true (**not** recommended), the blob directory is assumed
        to be shared among all clients using NFS or similar (file
        locks must also be supported); blob data will be stored *only*
        on the filesystem and not in the database. It is critical to
        have backups of this directory, as this is the only source of
        blobs.

        .. warning::

           When this option is true, the ability to do parallel
           commits is reduced. It is highly recommended to set this
           value to false. The possible exception is SQLite, which
           doesn't support parallel commit anyway.

        .. versionchanged:: 3.0a7

           The default changed from true to false.

blob-cache-size
        Maximum size of the blob cache, in bytes. If not set (the
        default), the cache size isn't checked and the blob directory
        will grow without bounds. This should be either empty or
        significantly larger than the largest blob you store. At least
        1 gigabyte is recommended for typical databases. More is
        recommended if you store large files such as videos, CD/DVD
        images, or virtual machine images.

        When configured, the size is checked when a process opens a
        storage for the first time, and at intervals based on
        ``blob-cache-size-check``.

        This option allows suffixes such as "mb" or "gb".

        This option is ignored if ``shared-blob-dir`` is true.

blob-cache-size-check
        Blob cache check size as percent of ``blob-cache-size``: "10"
        means "10%". The blob cache size will be checked when this
        many bytes have been loaded into the cache by any one process.
        Defaults to 10% of the blob cache size.

        This option is ignored if ``shared-blob-dir`` is true.

blob-cache-size-check-external
        When this value is false (the default), the blob cache size
        will be checked using an internal native thread (even when the
        process has been monkey-patched with gevent: gevent's
        threadpool will be used). When this value is true, an external
        subprocess will be used to check the size.

        For large blob caches, where checking the size takes
        measurable time, using an external process may improve
        response time for the application by reducing contention for
        the GIL. It may also be helpful for gevent applications.

        This is not recommended on Windows, where opening a file from
        multiple processes can be a problem.

blob-chunk-size
        When ZODB blobs are stored in MySQL, RelStorage breaks them into
        chunks to minimize the impact on RAM.  This option specifies the chunk
        size for new blobs. If RAM is available and the network
        connection to the database server is fast, a larger value can
        be more efficient because it will result in fewer roundtrips
        to the server.

        .. caution::
           On MySQL, this value cannot exceed the server's
           `max_allowed_packet
           <https://dev.mysql.com/doc/refman/5.5/en/server-system-variables.html#sysvar_max_allowed_packet>`_
           setting. If blob chunks are larger than that, it won't be
           possible to upload them. If blob chunks are uploaded and
           then that setting is later reduced, it won't be possible to
           download blobs that exceed that value.

           The driver may also influence this.

        On PostgreSQL and Oracle, this value is used as the memory
        buffer size for blob upload and download operations.

        The default is 1048576 (1 megabyte). This option allows
        suffixes such as "mb" or "gb".

        This option has no effect if ``shared-blob-dir`` is true (because
        blobs are not stored on the server).

Replication
===========

replica-conf
        If this option is provided, it specifies a text file that
        contains a list of database replicas the adapter can choose
        from. For MySQL and PostgreSQL, put in the replica file a list
        of ``host:port`` or ``host`` values, one per line. For Oracle,
        put in a list of DSN values. Blank lines and lines starting
        with ``#`` are ignored.

        The adapter prefers the first replica specified in the file. If
        the first is not available, the adapter automatically tries the
        rest of the replicas, in order. If the file changes, the
        adapter will drop existing SQL database connections and make
        new connections when ZODB starts a new transaction.

ro-replica-conf
        Like the ``replica-conf`` option, but the referenced text file
        provides a list of database replicas to use only for read-only
        load connections. This allows RelStorage to load objects from
        read-only database replicas, while using read-write replicas
        for all other database interactions.

        If this option is not provided, load connections will fall back
        to the replica pool specified by ``replica-conf``. If
        ``ro-replica-conf`` is provided but ``replica-conf`` is not,
        RelStorage will use replicas for load connections but not for
        other database interactions.

        Note that if read-only replicas are asynchronous, the next
        interaction after a write operation might not be up to date.
        When that happens, RelStorage will log a "backward time travel"
        warning and clear the ZODB cache to prevent consistency errors.
        This will likely result in temporarily reduced performance as
        the ZODB cache is repopulated.

        .. versionadded:: 1.6.0

replica-timeout
        If this option has a nonzero value, when the adapter selects
        a replica other than the primary replica, the adapter will
        try to revert to the primary replica after the specified
        timeout (in seconds).

        The default is 600, meaning 10 minutes.

revert-when-stale
        Specifies what to do when a database connection is stale.
        This is especially applicable to asynchronously replicated
        databases: RelStorage could switch to a replica that is not
        yet up to date.

        When ``revert-when-stale`` is ``false`` (the default) and the
        database connection is stale, RelStorage will raise a
        ReadConflictError if the application tries to read or write
        anything. The application should react to the
        ReadConflictError by retrying the transaction after a delay
        (possibly multiple times.) Once the database catches
        up, a subsequent transaction will see the update and the
        ReadConflictError will not occur again.

        When ``revert-when-stale`` is ``true`` and the database connection
        is stale, RelStorage will log a warning, clear the affected
        ZODB connection cache (to prevent consistency errors), and let
        the application continue with database state from
        an earlier transaction. This behavior is intended to be useful
        for highly available, read-only ZODB clients. Enabling this
        option on ZODB clients that read and write the database is
        likely to cause confusion for users whose changes
        seem to be temporarily reverted.

        .. versionadded:: 1.6.0

GC and Packing
==============

pack-gc
        If pack-gc is false, pack operations do not perform garbage
        collection. Garbage collection is enabled by default.

        If garbage collection is disabled, pack operations keep at
        least one revision of every object that hasn't been deleted.
        With garbage collection disabled, the pack code does not need
        to follow object references, making packing conceivably much
        faster. However, some of that benefit may be lost due to an
        ever increasing number of unused objects.

        Disabling garbage collection is **required** in a
        multi-database to prevent breaking inter-database references.
        The only safe way to collect and then pack databases in a
        multi-database is to use `zc.zodbdgc
        <https://pypi.org/project/zc.zodbdgc/>`_ and run
        ``multi-zodb-gc``, and only then pack each individual
        database.

        .. note::

           In history-free databases, packing after running
           ``multi-zodb-gc`` is not necessary. The garbage collection
           process itself handles the packing. Packing is only
           required in history-preserving databases.

        .. versionchanged:: 3.0

           Add support for ``zc.zodbdgc`` to history-preserving
           databases.

           Objects that have been deleted will be removed during a
           pack with ``pack-gc`` disabled.

        .. versionchanged:: 2.0

           Add support for ``zc.zodbdgc`` to history-free databases.

pack-prepack-only
        If pack-prepack-only is true, pack operations perform a full analysis
        of what to pack, but no data is actually removed.  After a pre-pack,
        the pack_object, pack_state, and pack_state_tid tables are filled
        with the list of object states and objects that would have been
        removed.

        If pack-gc is true, the object_ref table will also be fully
        populated. The object_ref table can be queried to discover
        references between stored objects.

pack-skip-prepack
        If pack-skip-prepack is true, the pre-pack phase is skipped and it
        is assumed the pack_object, pack_state and pack_state_tid tables have
        been filled already. Thus packing will only affect records already
        targeted for packing by a previous pre-pack analysis run.

        Use this option together with pack-prepack-only to split packing into
        distinct phases, where each phase can be run during different
        timeslots, or where a pre-pack analysis is run on a copy of the
        database to alleviate a production database load.

pack-batch-timeout
        Packing occurs in batches of transactions; this specifies the
        timeout in seconds for each batch.  Note that some database
        configurations have unpredictable I/O performance
        and might stall much longer than the timeout.

        The default timeout is 1.0 seconds.

pack-commit-busy-delay
        .. versionchanged:: 3.0a5

           This option is now deprecated and does nothing. The commit
           lock is not held during packing anymore. The remainder of
           the documentation for this option only applies to older
           versions.

        Before each pack batch, the commit lock is requested. If the lock is
        already held by for a regular commit, packing is paused for a short
        while. This option specifies how long the pack process should be
        paused before attempting to get the commit lock again.

        The default delay is 5.0 seconds.

Database Caching
================

In addition to the ZODB Connection object caches, RelStorage uses
pickle caches to reduce the number of queries to the database server.
(This is similar to ZEO.) Caches can be both local to a process
(within its memory) and remote (and shared between many processes) but
outside of special circumstances this is not recommended.

These options affect all caching operations.

cache-prefix
        The prefix for all keys in the remote shared cache; also used as part of
        persistent cache file names. All clients using a database should
        use the same cache-prefix. Defaults to the database name. (For
        example, in PostgreSQL, the database name is determined by
        executing ``SELECT current_database()``.) Set this if you have
        multiple databases with the same name.

        .. versionchanged:: 1.6.0b1
           Start defaulting to the database name.


Local Caching
-------------

RelStorage caches pickled objects in memory, similar to a ZEO cache.
The "local" cache is shared between all threads in a process. An
adequately sized local cache is important for the highest possible
performance. Using a suitably-sized local cache, especially with
persistent data files, can make a substantial performance difference,
even if the write volume is relatively high.

For help understanding and tuning the cache behaviour, see :doc:`cache-tracing`.

cache-local-mb
        This option configures the approximate maximum amount of memory the
        cache should consume, in megabytes. It defaults to 10.

        Set to 0 to disable the in-memory cache. (*This is not recommended.*)

cache-local-object-max
        This option configures the maximum size of an object's pickle
        (in bytes) that can qualify for the "local" cache.  The size is
        measured after compression. Larger objects can still qualify
        for the remote cache.

        The default is 16384 (1 << 14) bytes.

        .. versionchanged:: 2.0b2
           Measure the size after compression instead of before.

cache-local-compression
        This option configures compression within the "local" cache.
        This option names a Python module that provides two functions,
        ``compress()`` and ``decompress()``.  Supported values include
        ``zlib``, ``bz2``, and ``none`` (no compression).

        The default is ``none`` to avoid copying data more than necessary.

        If you use the compressing storage wrapper `zc.zlibstorage
        <https://pypi.python.org/pypi/zc.zlibstorage>`_, this option
        automatically does nothing. With other compressing storage
        wrappers this should be set to ``none``.

        .. versionadded:: 1.6

cache-delta-size-limit
        This is an advanced option related to the MVCC implementation
        used by RelStorage's cache.

        The default is 100,000 on CPython, 50,000 on PyPy.

        .. versionchanged:: 3.0a9
           This parameter has changed meanings. Previously, it
           controlled a memory allocation per-ZODB connection, and it
           also controlled how often each connection would have to
           make an expensive database query. Smaller values of around
           20,000 are appropriate on these older versions.

           In 3.0a9 this is a global value shared among all
           connections. It is no longer directly related to polling,
           but if a read connection is open for more new or changed
           objects than this limit, the next time the connection is
           used it will drop its ZODB object cache rather than attempt
           to pick through the invalidations. Idle connections like
           that may indicate a ZODB connection pool that's too large;
           consider enabling the idle connection timeout.

        .. versionchanged:: 3.0a3
           Increase the sizes again. With better persistent caching,
           these become increasingly important.

        .. versionchanged:: 2.0b7
           Double the default size from 10000 to 20000 on CPython. The
           use of LLBTree for the internal data structure means we use
           much less memory than we did before.


Persistent Local Caching
~~~~~~~~~~~~~~~~~~~~~~~~

Like ZEO, RelStorage can store its local cache to disk for a quicker
application warmup period.

.. versionchanged:: 3.0a7
   Raise a descriptive error if the ``sqlite3`` library is too old to
   be used by the local cache. RelStorage requires at least 3.8.3 but
   works better with 3.15 and best with 3.24 or newer.

   This feature is no longer considered experimental.

.. versionchanged:: 3.0a1
   The persistent file format and contents have been substantially
   changed to produce much better hit rates.

   Reading and writing the cache files is slower, however, as the
   cache size gets larger.

   The cache files must be located on a local (not network) filesystem.

.. versionadded:: 2.0b2
   This is a new, *experimental* feature. While there should
   be no problems enabling it, the exact details of its
   function are subject to change in the future based on
   feedback and experience.


cache-local-dir
        The path to a directory where the local cache will be saved
        when the database is closed. On startup, RelStorage will look
        in this directory for cache files to load into memory.

        This option can dramatically reduce the amount of time it
        takes for your application to warm up after a restart,
        especially if there were relatively few writes in the
        meantime. Some synthetic benchmarks show an 8-10x improvement
        after a restart.

        This is similar to the ZEO persistent cache, but adds *no
        overhead* to normal transactions.

        This directory will be populated with files written each time a
        RelStorage instance is closed. If multiple RelStorage
        processes are working against the same database (for example,
        the workers of gunicorn), then they will each write and read
        files in this directory. On startup, the files will be
        combined to get the "warmest" possible cache.

        The time taken to load the cache file (which only occurs when
        RelStorage is first opened) and the time taken to save the
        cache file (which only occurs when the database is closed) are
        proportional to the total size of the cache; thus, a cache
        that is too large (holding many unused entries) will slow down
        startup/shutdown for no benefit. However, the timing is
        probably not a practical concern compared to the disk usage;
        on one system, a 300MB uncompressed cache file can be saved in
        3-4 seconds and read in 2-3 seconds.

        This directory can be shared among storages connected to
        different databases, so long as they all have a distinct
        ``cache-prefix``.

        If this directory does not exist, we will attempt to create it
        on startup. This directory must be a local filesystem, not
        network storage.

        .. tip::
           If the database (ZODB.DB object) is not properly
           :class:`closed <ZODB.interfaces.IDatabase>`, then the cache files will not be written.

        .. tip::
           This requires at least sqlite3 3.8.3 or better. Improved
           performance comes with version 3.15 and the best
           performance is with 3.24 or higher.

Deprecated Options
++++++++++++++++++

The following local cache options apply only to RelStorage 2.x. They
are deprecated, ignored, and have no impact on RelStorage 3.0a1 or
newer. RelStorage releases in 2021 will no longer accept them.

cache-local-dir-count
        How many files that ``cache-local-dir`` will be allowed to
        contain before files start getting reused. Set this equal to
        the number of workers that will be sharing the directory.

        The default is 20.

cache-local-dir-compress
        Whether to compress the persistent cache files on disk. The
        default is false because individual entries are usually already
        compressed and the tested workloads do not show a space
        benefit from the compression; in addition, compression can
        slow the reading and writing process by 2 to 3 times or more
        (and hence slow down opening the storage).

        .. versionadded:: 2.0b5


cache-local-dir-read-count
        The maximum number of files to read to populate the cache on
        startup.

        By default, RelStorage will read all the appropriate files (so
        up to ``cache-local-dir-count`` files), from newest to oldest,
        collecting the distinct entries out of them, until the cache
        is fully populated (``cache-local-mb``) or there are no more
        files. This ensures that after startup, all workers have the
        most fully populated cache. This strategy works well if the
        workers have a good distribution of work (relatively few
        overlapping items) and the cache size is relatively small;
        after startup they will all be equally warm without spending
        too much startup time.

        However, if the workers all do nearly the same work (so most
        items in the cache files are the same) and the cache sizes are
        very large, then the benefits of reading each subsequent file
        diminish (because it has very few if any new entries to add,
        and reading them all takes a lot of time). In that case, set
        this value to 1 to only read the first ("best") cache file.

        For situations in between, choose a number in between.

        .. versionadded:: 2.0b5

cache-local-dir-write-max-size
        The *approximate* maximum size of each individual cache file
        on disk. When not specified (the default), the maximum file
        size will be the same as ``cache-local-mb``.

        This is an approximate number because there is some overhead
        associated with the storage format that varies based on the
        number of entries in the cache.

        RelStorage will write to disk, from most important to least
        important, the entries in the cache until all the entries are
        written or this limit is reached. If you use a size smaller
        than ``cache-local-mb``, however, you may miss important
        entries that are only used at application startup.

        .. versionadded:: 2.0b7


Remote Caching
--------------

RelStorage can use Memcached servers as a secondary, semi-persistent
database cache. This is generally not recommended; most RelStorage
queries are simple enough that even in extremely large databases
Memcached is not faster than the database. The need for a cache that
persists across restarts is better met with local persistent caches.

However, if local persistent caches are not an option, memcache may be
useful if the ratio of writes to reads is extremely low (because they
add substantial overhead to each write operation), and if the database
server is behind a high-latency connection or otherwise responds
slowly.

.. important::

   Remote caching is scheduled for removal with RelStorage releases
   in 2021.

cache-servers
        Specifies a list of memcached servers. Using memcached with
        RelStorage improves the speed of frequent object accesses while
        slightly reducing the speed of other operations.

        Provide a list of host:port pairs, separated by whitespace.
        "127.0.0.1:11211" is a common setting.  Some memcached modules,
        such as pylibmc, allow you to specify a path to a Unix socket
        instead of a host:port pair.

        The default is to disable memcached integration.

        .. versionadded:: 1.1rc1

cache-module-name
        Specifies which Python memcache module to use. The default is
        "relstorage.pylibmc_wrapper", which requires `pylibmc <https://pypi.python.org/pypi/pylibmc>`_. An
        alternative module is "`memcache <https://pypi.python.org/pypi/python-memcached>`_", a pure Python module. If you
        use the memcache module, use at least version 1.47.

        This option has no effect unless cache-servers is set.
