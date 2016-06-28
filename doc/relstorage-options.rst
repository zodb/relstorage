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

        If this option is set to false, the adapter will create and
        use a history-free database schema. Undo will not be supported,
        but the database will not grow as quickly. The database will
        still require regular garbage collection (which is accessible
        through the database pack mechanism.)

        .. warning::

           This option must not change once the database schema has
           been installed, because the schemas for history-preserving and
           history-free storage are different. If you want to convert
           between a history-preserving and a history-free database, use
           the :doc:`zodbconvert` utility to copy to a new database.

poll-interval
        Defer polling the database for the specified maximum time interval,
        in seconds.  Set to 0 (the default) to always poll.  Fractional
        seconds are allowed.  Use this to lighten the database load on
        servers with high read volume and low write volume.

        The poll-interval option works best in conjunction with
        the ``cache-servers`` option.  If both are enabled, RelStorage will
        poll a single cache key for changes on every request.
        The database will not be polled unless the cache indicates
        there have been changes, or the timeout specified by poll-interval
        has expired.  This configuration keeps clients fully up to date,
        while removing much of the polling burden from the database.
        A good cluster configuration is to use memcache servers
        and a high poll-interval (say, 60 seconds).

        .. caution::

           This option can be used without the ``cache-servers`` option,
           but a large poll-interval without cache-servers increases the
           probability of basing transactions on stale data, which does not
           affect database consistency, but does increase the probability
           of conflict errors, leading to low performance.

commit-lock-timeout
        During commit, RelStorage acquires a database-wide lock. This
        option specifies how long to wait for the lock before
        failing the attempt to commit. The default is 30 seconds.

        The MySQL and Oracle adapters support this option. The
        PostgreSQL adapter supports this when the PostgreSQL server is
        at least version 9.3; otherwise it is ignored.

        .. versionchanged:: 2.0.0b1
           Add support for lock timeouts on PostgreSQL 9.3 and above.
           Previously no PostgreSQL version supported timeouts.

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
        If true (the default), the blob directory is assumed to be
        shared among all clients using NFS or similar; blob data will
        be stored only on the filesystem and not in the database. If
        false, blob data is stored in the relational database and the
        blob directory holds a cache of blobs.

        When this option is false, the blob directory should not be
        shared among clients.

blob-cache-size
        Maximum size of the blob cache, in bytes. If empty (the
        default), the cache size isn't checked and the blob directory
        will grow without bounds. This should be either empty or
        significantly larger than the largest blob you store. At least
        1 gigabyte is recommended for typical databases. More is
        recommended if you store large files such as videos, CD/DVD
        images, or virtual machine images.

        This option allows suffixes such as "mb" or "gb".
        This option is ignored if shared-blob-dir is true.

blob-cache-size-check
        Blob cache check size as percent of blob-cache-size: "10" means
        "10%". The blob cache size will be checked when this many bytes
        have been loaded into the cache. Defaults to 10% of the blob
        cache size.

        This option is ignored if shared-blob-dir is true.

blob-chunk-size
        When ZODB blobs are stored in MySQL, RelStorage breaks them into
        chunks to minimize the impact on RAM.  This option specifies the chunk
        size for new blobs. On PostgreSQL and Oracle, this value is used as
        the memory buffer size for blob upload and download operations. The
        default is 1048576 (1 mebibyte).

        This option allows suffixes such as "mb" or "gb".
        This option is ignored if shared-blob-dir is true.

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
        If pack-gc is false, pack operations do not perform
        garbage collection.  Garbage collection is enabled by default.

        If garbage collection is disabled, pack operations keep at least one
        revision of every object.  With garbage collection disabled, the
        pack code does not need to follow object references, making
        packing conceivably much faster.  However, some of that benefit
        may be lost due to an ever increasing number of unused objects.

        Disabling garbage collection is also a hack that ensures
        inter-database references never break.

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
        Before each pack batch, the commit lock is requested. If the lock is
        already held by for a regular commit, packing is paused for a short
        while. This option specifies how long the pack process should be
        paused before attempting to get the commit lock again.

        The default delay is 5.0 seconds.

Local and Remote Caching
========================

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

cache-prefix
        The prefix for all keys in the cache. All clients using a
        database should use the same cache-prefix. Defaults to the
        database name. (For example, in PostgreSQL, the database
        name is determined by executing ``SELECT current_database()``.)
        Set this if you have multiple databases with the same name.

        .. versionchanged:: 1.6.0b1
           Start defaulting to the database name.

cache-local-mb
        RelStorage caches pickled objects in memory, similar to a ZEO
        cache. The "local" cache is shared between threads. This option
        configures the approximate maximum amount of memory the cache
        should consume, in megabytes.  It defaults to 10.

        Set to 0 to disable the in-memory cache.

cache-local-object-max
        This option configures the maximum size of an object's pickle
        (in bytes) that can qualify for the "local" cache.  The size is
        measured before compression. Larger objects can still qualify
        for memcache.

        The default is 16384 (1 << 14) bytes.

cache-local-compression
        This option configures compression within the "local" cache.
        This option names a Python module that provides two functions,
        ``compress()`` and ``decompress()``.  Supported values include
        ``zlib``, ``bz2``, and ``none`` (no compression).

        The default is ``zlib``.

cache-delta-size-limit
        This is an advanced option. RelStorage uses a system of
        checkpoints to improve the cache hit rate. This option
        configures how many objects should be stored before creating a
        new checkpoint.

        The default is 10000.
