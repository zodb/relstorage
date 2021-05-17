=========
 Changes
=========

3.5.0a1 (2021-05-17)
====================

- Increase the default value of the ``RS_CACHE_MVCC_MAX_DEPTH``
  advanced tuning parameter from 100 to 1000 based on observations of
  production workloads. (Connections that haven't polled
  for the last ``RS_CACHE_MVCC_MAX_DEPTH`` committed transactions ---
  and thus are expected to have a large number of invalidations ---
  are "detached" and forced to invalidate their entire persistent
  object cache if they get used again.)

- Add StatsD counter metric
  "relstorage.cache.mvcc.invalidate_all_detached" that is incremented
  when a previously-detached Connection is required to invalidate its
  entire persistent object cache. In a well-tuned environment, this
  counter should be very low and as such is not sampled but always sent.

- Fix the logging of some environment variables RelStorage uses.

- If there is a read conflict error, PostgreSQL no longer holds any
  database locks while the error is raised and the transaction is
  rolled back in Python. Previously, shared locks could be held during
  this process, preventing other transactions from moving forward.

- Take exclusive locks first, and then shared locks in NOWAIT mode.
  This reverses :pr:`317`, but it eliminates the requirement that the
  database server finds and breaks deadlocks (by eliminating
  deadlocks). Deadlocks could never be resolved without retrying the
  entire transaction, and which transaction got killed was unknowable.
  Provisions are made to keep fast detection of ``readCurrent``
  conflicts. Benchmarks with zodbshootout find no substantial
  differences. See :issue:`469`.

3.4.5 (2021-04-23)
==================

- Scale the new timing metrics introduced in 3.4.2 to milliseconds.
  This matches the scale of other timing metrics produced
  automatically by the use of ``perfmetrics`` in this package.
  Similarly, append ``.t`` to the end of their names for the same
  reason.


3.4.4 (2021-04-23)
==================

- Fix an exception sending stats when TPC is aborted because of an error
  during voting such as a ``ConflictError``. This only affected those
  deployments with perfmetrics configured to use a StatsD client. See
  :issue:`464`.


3.4.3 (2021-04-22)
==================

- PostgreSQL: Log the backend PID at the start of TPC. This can help
  correlate error messages from the server. See :issue:`460`.

- Make more conflict errors include information about the OIDs and
  TIDs that may have been involved in the conflict.

- Add support for pg8000 1.17 and newer; tested with 1.19.2. See
  :issue:`438`.

3.4.2 (2021-04-21)
==================

- Fix write replica selection after a disconnect, and generally
  further improve handling of unexpectedly closed store connections.

- Release the critical section a bit sooner at commit time, when
  possible. Only affects gevent-based drivers. See :issue:`454`.

- Add support for mysql-connector-python-8.0.24.

- Add StatsD counter metrics
  "relstorage.storage.tpc_vote.unable_to_acquire_lock",
  "relstorage.storage.tpc_vote.total_conflicts,"
  "relstorage.storage.tpc_vote.readCurrent_conflicts,"
  "relstorage.storage.tpc_vote.committed_conflicts," and
  "relstorage.storage.tpc_vote.resolved_conflicts". Also add StatsD
  timer metrics "relstorage.storage.tpc_vote.objects_locked" and
  "relstorage.storage.tpc_vote.between_vote_and_finish" corresponding
  to existing log messages. The rate at which these are sampled, as
  well as the rate at which many method timings are sampled, defaults
  to 10% (0.1) and can be controlled with the
  ``RS_PERF_STATSD_SAMPLE_RATE`` environment variable. See :issue:`453`.

3.4.1 (2021-04-12)
==================

- RelStorage has moved from Travis CI to `GitHub Actions
  <https://github.com/zodb/relstorage/actions>`_ for macOS and Linux
  tests and manylinux wheel building. See :issue:`437`.
- RelStorage is now tested with PostgreSQL 13.1. See :issue:`427`.
- RelStorage is now tested with PyMySQL 1.0. See :issue:`434`.
- Update the bundled boost C++ library from 1.71 to 1.75.
- Improve the way store connections are managed to make it less likely
  a "stale" store connection that hasn't actually been checked for
  liveness gets used.

3.4.0 (2020-10-19)
==================

- Improve the logging of ``zodbconvert``. The regular minute logging
  contains more information and takes blob sizes into account, and
  debug logging is more useful, logging about four times a minute.
  Some extraneous logging was bumped down to trace.

- Fix psycopg2 logging debug-level warnings from the PostgreSQL server
  on transaction commit about not actually being in a transaction.
  (Sadly this just squashes the warning, it doesn't eliminate the
  round trip that generates it.)

- Improve the performance of packing databases, especially
  history-free databases. See :issue:`275`.

- Give ``zodbpack`` the ability to check for missing references in
  RelStorages with the ``--check-refs-only`` argument. This will
  perform a pre-pack with GC, and then report on any objects that
  would be kept and refer to an object that does not exist. This can
  be much faster than external scripts such as those provided by
  ``zc.zodbdgc``, though it definitely only reports missing references
  one level deep.

  This is new functionality. Feedback, as always, is very welcome!

- Avoid extra pickling operations of transaction meta data extensions
  by using the new ``extension_bytes`` property introduced in ZODB
  5.6. This results in higher-fidelity copies of storages, and may
  slightly improve the speed of the process too. See :issue:`424`.

- Require ZODB 5.6, up from ZODB 5.5. See :issue:`424`.

- Make ``zodbconvert`` *much faster* (around 5 times faster) when the
  destination is a history-free RelStorage and the source supports
  ``record_iternext()`` (like RelStorage and FileStorage do). This
  also applies to the ``copyTransactionsFrom`` method. This is disabled
  with the ``--incremental`` option, however. Be sure to read the
  updated zodbconvert documentation.

3.3.2 (2020-09-21)
==================

- Fix an ``UnboundLocalError`` in case a store connection could not be
  opened. This error shadowed the original error opening the
  connection. See :issue:`421`.


3.3.1 (2020-09-14)
==================

- Manylinux wheels: Do not specify the C++ standard to use when
  compiling. This seemed to result in an incompatibility with
  manylinux1 systems that was not caught by ``auditwheel``.


3.3.0 (2020-09-14)
==================

- The "MySQLdb" driver didn't properly use server-side cursors when
  requested. This would result in unexpected increased memory usage
  for things like packing and storage iteration.

- Make RelStorage instances implement
  ``IStorageCurrentRecordIteration``. This lets both
  history-preserving and history-free storages work with
  ``zodbupdate``. See :issue:`389`.

- RelStorage instances now pool their storage connection. Depending on
  the workload and ZODB configuration, this can result in requiring
  fewer storage connections. See :issue:`409` and :pr:`417`.

  There is a potential semantic change: Under some circumstances, the
  ``loadBefore`` and ``loadSerial`` methods could be used to load
  states from the future (not visible to the storage's load
  connection) by using the store connection. This ability has been
  removed.

- Add support for Python 3.9.

- Drop support for Python 3.5.

- Build manylinux x86-64 and macOS wheels on Travis CI as part of the
  release process. These join the Windows wheels in being
  automatically uploaded to PyPI.


3.2.1 (2020-08-28)
==================

- Improve the speed of loading large cache files by reducing the cost
  of cache validation.

- The timing metrics for ``current_object_oids`` are always collected,
  not just sampled. MySQL and PostgreSQL will only call this method
  once at startup during persistent cache validation. Other databases
  may call this method once during the commit process.

- Add the ability to limit how long persistent cache validation will
  spend polling the database for invalid OIDs. Set the environment
  variable ``RS_CACHE_POLL_TIMEOUT`` to a number of seconds before
  importing RelStorage to use this.

- Avoid an ``AttributeError`` if a persistent ``zope.component`` site
  manager is installed as the current site, it's a ghost, and we're
  making a load query for the first time in a particular connection.
  See :issue:`411`.

- Add some DEBUG level logging around forced invalidations of
  persistent object caches due to exceeding the cache MVCC limits. See
  :issue:`338`.

3.2.0 (2020-07-20)
==================

- Make the ``gevent psycopg2`` driver support critical sections. This
  reduces the amount of gevent switches that occur while database
  locks are held under a carefully chosen set of circumstances that
  attempt to balance overall throughput against latency. See
  :issue:`407`.

- Source distributions: Fix installation when Cython isn't available.
  Previously it incorrectly assumed a '.c' extension which lead to
  compiler errors. See :issue:`405`.

- Improve various log messages.

3.1.2 (2020-07-14)
==================

- Fix the psycopg2cffi driver inadvertently depending on the
  ``psycopg2`` package. See :issue:`403`.
- Make the error messages for unavailable drivers include more
  information on underlying causes.
- Log a debug message when an "auto" driver is successfully resolved.
- Add a ``--debug`` argument to the ``zodbconvert`` command line tool
  to enable DEBUG level logging.
- Add support for pg8000 1.16. Previously, a ``TypeError`` was raised.

3.1.1 (2020-07-02)
==================

- Add support for pg8000 >= 1.15.3. Previously, a ``TypeError`` was
  raised.

- SQLite: Committing a transaction releases some resources sooner.
  This makes it more likely that auto-checkpointing of WAL files will be
  able to reclaim space in some scenarios. See :issue:`401`.


3.1.0 (2020-06-11)
==================

- Use unsigned BTrees for internal data structures to avoid wrapping
  in large databases. Requires BTrees 4.7.2.


3.0.1 (2019-11-22)
==================

- Oracle: Fix an AttributeError saving to Oracle. See :pr:`380` by Mauro
  Amico.

- MySQL+gevent: Release the critical section a bit sooner. See :issue:`381`.

- SQLite+gevent: Fix possible deadlocks with gevent if switches
  occurred at unexpected times. See :issue:`382`.

- MySQL+gevent: Fix possible deadlocks with gevent if switches
  occurred at unexpected times. See :issue:`385`.  This also included
  some minor optimizations.

  .. caution::

     This introduces a change in a stored procedure that is not
     compatible with older versions of RelStorage. When this version
     is first deployed, if there are older versions of RelStorage
     still running, they will be unable to commit. They will fail with
     a transient conflict error; they may attempt retries, but wil not
     succeed. Read-only transactions will continue to work.

3.0.0 (2019-11-12)
==================

- Build binary wheels for Python 3.8 on Windows.


3.0rc1 (2019-11-08)
===================

- SQLite: Avoid logging (at DEBUG level) an error executing ``PRAGMA
  OPTIMIZE`` when closing a read-only (load) connection. Now, the
  error is avoided by making the connection writable.

- PostgreSQL: Reduce the load connection's isolation level from
  ``SERIALIZABLE`` to ``REPEATABLE READ`` (two of the three other
  supported databases also operate at this level). This allows
  connecting to hot standby/streaming replicas. Since the connection
  is read-only, and there were no other ``SERIALIZABLE`` transactions
  (the store connection operates in ``READ COMMITTED`` mode), there
  should be no other visible effects. See :issue:`376`.

- PostgreSQL: pg8000: Properly handle a ``port`` specification in the
  ``dsn`` configuration. See :issue:`378`.

- PostgreSQL: All drivers pass the ``application_name`` parameter at
  connect time instead of later. This solves an issue with psycopg2
  and psycopg2cffi connecting to hot standbys.

- All databases: If ``create-schema`` is false, use a read-only
  connection to verify that the schema is correct.

- Packaging: Prune unused headers from the include/ directory.


3.0b3 (2019-10-29)
==================

- SQLite: Fix a bug that could lead to invalid OIDs being allocated if
  transactions were imported from another storage.


3.0b2 (2019-10-28)
==================

- SQLite: Require the database to be in dedicated directory.

  .. caution::

     This introduces a change to the <sqlite3> configuration.
     Please review the documentation. It is possible to migrate a
     database created earlier to the new structure, but no automated
     tooling or documentation is provided for that.

- SQLite: Allow configuration of many of SQLite's PRAGMAs for advanced
  tuning.

- SQLite: Fix resetting OIDs when zapping a storage. This could be a
  problem for benchmarks.

- SQLite: Fix large prefetches resulting in ``OperationalError``

- SQLite: Improve the speed of copying transactions into a SQLite
  storage (e.g., with zodbconvert).

- SQLite: Substantially improve general performance. See :pr:`368`.

- SQLite: Add the ``gevent sqlite3`` driver that periodically yields
  to the gevent loop at configurable intervals.

- PostgreSQL: Improve the speed of  writes when using the 'gevent
  psycopg2' driver.

3.0b1 (2019-10-22)
==================

- Make SQLite and Oracle both use UPSERT queries instead of multiple
  database round trips.

- Fix an exception with large transactions on SQLite.

- Fix compiling the C extension on very new versions of Microsoft
  Visual Studio.

3.0a13 (2019-10-21)
===================

- Further speed improvements and memory efficiency gains of around 30%
  for the cache.

- Restore support for Python 2.7 on Windows.

- No longer require Cython to build from a sdist (.tar.gz).

- Add support for using a SQLite file as a RelStorage backend, if all
  processes accessing it will be on a single machine. The advantage
  over FileStorage is that multiple processes can use the database
  concurrently. To allow multiple processes to use a FileStorage one
  must deploy ZEO, even if all processes are on a single machine. See
  :pr:`362`.

- Fix and test Oracle. The minimum required cx_oracle is now 6.0.

- Add support for Python 3.8.

3.0a12 (2019-10-09)
===================

- Add the ``gevent psycopg2`` driver to allow using the fast psycopg2
  driver with gevent.

- Conflict resolution prefetches data for conflicted objects, reducing
  the number of database queries and locks needed.

- Introduce a driver-agnostic method for elevating database connection
  priority during critical times of two-phase commit, and implement it
  for the ``gevent MySQLdb`` driver. This reduces the amount of gevent
  switches that occur while database locks are held under a carefully
  chosen set of circumstances that attempt to balance overall
  throughput against latency. See :issue:`339`.

- Drop support for Python 2.7 on Windows. The required compiler is
  very old. See :issue:`358`.

- Substantially reduce the overhead of the cache, making it mome
  memory efficient. Also make it substantially faster. This was done
  by rewriting it in C. See :issue:`358`.

3.0a11 (2019-09-25)
===================

- Make ``poll_invalidations`` handle other retryable internal
  exceptions besides just ``ReadConflictError`` so they don't
  propagate out to ``transaction.begin()``.

- Make the zodburi resolver entry points not require a specific
  RelStorage extra such as 'postgres', in case there is a desire to
  use a different database driver than the default that's installed
  with that extra. See :issue:`342`, reported by Éloi Rivard.

- Make the zodburi resolvers accept the 'driver' query paramater to
  allow selecting a specific driver to use. This functions the same as
  in a ZConfig configuration.

- Make the zodburi resolvers more strict on the distinction between
  boolean arguments and arbitrary integer arguments. Previously, a
  query like ``?read_only=12345&cache_local_mb=yes`` would have been
  interpreted as ``True`` and ``1``, respectively. Now it produces errors.

- Fix the calculation of the persistent cache size, especially on
  Python 2. This is used to determine when to shrink the disk cache.
  See :issue:`317`.

- Fix several race conditions when packing history-free storages
  through a combination of changes in ordering and more strongly
  consistent (``READ ONLY REPEATABLE READ``) transactions.
  Reported in :issue:`325` by krissik with initial PR by Andreas
  Gabriel.

- Make ``zodbpack`` pass RelStorage specific options like
  ``--prepack`` and ``--use-prepack-state`` to the RelStorage, even
  when it has been wrapped in a ``zc.zlibstorage``.

- Reduce the amount of memory required to pack a RelStorage through
  more careful datastructure choices. On CPython 3, the peak
  memory usage of the prepack phase can be up to 9 times less. On
  CPython 2, pre-packing a 30MM row storage required 3GB memory; now
  it requires about 200MB.

- Use server-side cursors during packing when available, further
  reducing the amount of memory required. See :issue:`165`.

- Make history-free database iterators from the same storage use a
  consistent view of the database (until a transaction is committed
  using the storage or ``sync()`` is called). This prevents data loss
  in some cases. See :issue:`344`.

- Make copying transactions *from* a history-free RelStorage (e.g., with
  ``zodbconvert``) require substantially less memory (75% less).

- Make copying transactions *to* a RelStorage clean up temporary blob
  files.

- Make ``zodbconvert`` log progress at intervals instead of for every
  transaction. Logging every transaction could add significant overhead
  unless stdout was redirected to a file.

- Avoid attempting to lock objects being created. See :issue:`329`.

- Make cache vacuuming faster.

3.0a10 (2019-09-04)
===================

- Fix a bug where the persistent cache might not properly detect
  object invalidations if the MVCC index pulled too far ahead at save
  time. Now it explicitly checks for invalidations at load time, as
  earlier versions did. See :pr:`343`.

- Require perfmetrics 3.0.

3.0a9 (2019-08-28)
==================

- Several minor logging improvements.

- Allow many internal constants to be set with environment variables
  at startup for experimentation. These are presently undocumented; if
  they prove useful to adjust in different environments they may be
  promoted to full configuration options.

- Fix importing RelStorage when ``zope.schema`` is not installed.
  ``zope.schema`` is intended to be a test dependency and optional for
  production deployments. Reported in :issue:`334` by Jonathan Lung.

- Make the gevent MySQL driver more efficient at avoiding needless  waits.

- Due to a bug in MySQL (incorrectly rounding the 'minute' value of a
  timestamp up), TIDs generated in the last half second of a minute
  would suddenly jump ahead by 4,266,903,756 integers (a full minute).

- Fix leaking an internal value for ``innodb_lock_timeout`` across
  commits on MySQL. This could lead to ``tpc_vote`` blocking longer
  than desired. See :issue:`331`.

- Fix ``undo`` to purge the objects whose transaction was revoked from
  the cache.

- Make historical storages read-only, raising
  ``ReadOnlyHistoryError``, during the commit process. Previously this
  was only enforced at the ``Connection`` level.

- Rewrite the cache to understand the MVCC nature of the connections
  that use it.

  This eliminates the use of "checkpoints." Checkpoints established a
  sort of index for objects to allow them to be found in the cache
  without necessarily knowing their ``_p_serial`` value. To achieve
  good hit rates in large databases, large values for the
  ``cache-delta-size-limit`` were needed, but if there were lots of
  writes, polling to update those large checkpoints could become very
  expensive. Because checkpoints were separate in each ZODB connection
  in a process, and because when one connection changed its
  checkpoints every other connection would also change its checkpoints
  on the next access, this could quickly become a problem in highly
  concurrent environments (many connections making many large database
  queries at the same time). See :issue:`311`.

  The new system uses a series of chained maps representing polling
  points to build the same index data. All connections can share all
  the maps for their view of the database and earlier. New polls add
  new maps to the front of the list as needed, and old mapps are
  removed once they are no longer needed by any active transaction.
  This simulates the underlying database's MVCC approach.

  Other benefits of this approach include:

  - No more large polls. While each connection still polls for each
    transaction it enters, they now share state and only poll against
    the last time a poll occurred, not the last time they were used.
    The result should be smaller, more predictable polling.

  - Having a model of object visibility allows the cache to use more
    efficient data structures: it can now use the smaller LOBTree to
    reduce the memory occupied by the cache. It also requires
    fewer cache entries overall to store multiple revisions of an
    object, reducing the overhead. And there are no more key copies
    required after a checkpoint change, again reducing overhead and
    making the LRU algorithm more efficient.

  - The cache's LRU algorithm is now at the object level, not the
    object/serial pair.

  - Objects that are known to have been changed but whose old revision
    is still in the cache are preemptively removed when no references
    to them are possible, reducing cache memory usage.

  - The persistent cache can now guarantee not to write out data that
    it knows to be stale.

  Dropping checkpoints probably makes memcache less effective, but
  memcache hasn't been recommended for awhile.


3.0a8 (2019-08-13)
==================

- Improve the safety of the persistent local cache in high-concurrency
  environments using older versions of SQLite. Perform a quick
  integrity check on startup and refuse to use the cache files if they
  are reported corrupt.

- Switch the order in which object locks are taken: try shared locks
  first and only then attempt exclusive locks. Shared locks do not
  have to block, so a quick lock timeout here means that a
  ``ReadConflictError`` is inevitable. This works best on PostgreSQL
  and MySQL 8, which support true non-blocking locks. On MySQL 5.7,
  non-blocking locks are emulated with a 1s timeout. See :issue:`310`.

  .. note:: The transaction machinery will retry read conflict errors
            by default. The more rapid detection of them may lead to
            extra retries if there was a process still finishing its
            commit. Consider adding small sleep backoffs to retry
            logic.

- Fix MySQL to immediately rollback its transaction when it gets a
  lock timeout, while still in the stored procedure on the database.
  Previously it would have required a round trip to the Python
  process, which could take an arbitrary amount of time while the
  transaction may have still been holding some locks. (After
  :issue:`310` they would only be shared locks, but before they would
  have been exclusive locks.) This should make for faster recovery in
  heavily loaded environments with lots of conflicts. See :issue:`313`.

- Make MySQL clear its temp tables using a single round trip.
  Truncation is optional and disabled by default. See :issue:`319`.

- Fix PostgreSQL to not send the definition of the temporary tables
  for every transaction. This is only necessary for the first
  transaction.

- Improve handling of commit and rollback, especially on PostgreSQL.
  We now generate many fewer unneeded rollbacks. See :issue:`289`.

- Stop checking the status of ``readCurrent`` OIDs twice.

- Make the gevent MySQL driver yield more frequently while getting
  large result sets. Previously it would block in C to read the entire
  result set. Now it yields according to the cursor's ``arraysize``.
  See :issue:`315`.

- Polling for changes now iterates the cursor instead of using
  ``fetchall()``. This can reduce memory usage and provide better
  behaviour in a concurrent environment, depending on the cursor
  implementation.

- Add three environment variables to control the odds of whether any
  given poll actually suggests shifted checkpoints. These are all
  floating point numbers between 0 and 1. They are
  ``RELSTORAGE_CP_REPLACEMENT_CHANCE_WHEN_FULL`` (default to 0.7,
  i.e., 70%), ``RELSTORAGE_CP_REPLACEMENT_BEGIN_CONSIDERING_PERCENT``
  (default 0.8) and ``RELSTORAGE_CP_REPLACEMENT_CHANCE_WHEN_CLOSE``
  (default 0.2). (There are corresponding class variables on the
  storage cache that could also be set.) Use values of ``1``, ``1``
  and ``0`` to restore the old completely deterministic behaviour.
  It's not clear whether these will be useful, so they are not
  officially options yet but they may become so. Feedback is
  appreciated! See :issue:`323`.

  .. note::

     These were removed in 3.0a9.

3.0a7 (2019-08-07)
==================

- Eliminate runtime dependency on ZEO. See :issue:`293`.

- Fix a rare race condition allocating OIDs on MySQL. See
  :issue:`283`.

- Optimize the ``loadBefore`` method. It appears to be mostly used in
  the tests.

- Fix the blob cache cleanup thread to use a real native thread if
  we're monkey-patched by gevent, using gevent's thread pool.
  Previously, cleaning up the blob cache would block the event loop
  for the duration. See :issue:`296`.

- Improve the thread safety and resource usage of blob cache cleanup.
  Previously it could spawn many useless threads.

- When caching a newly uploaded blob for a history free storage, if
  there's an older revision of the blob in the cache, and it is not in
  use, go ahead and preemptively remove it from disk. This can help
  prevent the cache size from growing out of hand and limit the number
  of expensive full cache checks required. See :issue:`297`.

- Change the default value of the configuration setting
  ``shared-blob-dir`` to false, meaning that the default is now to use
  a blob cache. If you were using shared blobs before, you'll need to
  explicitly set a value for ``shared-blob-dir`` to ``true`` before
  starting RelStorage.

- Add an option, ``blob-cache-size-check-external``, that causes the
  blob cache cleanup process to run in a subprocess instead of a
  thread. This can free up the storage process to handle requests.
  This is not recommended on Windows. (``python -m
  relstorage.blobhelper.cached /path/to/cache size_in_bytes`` can be
  used to run a manual cleanup at any time. This is currently an
  internal implementation detail.)

- Abort storage transactions immediately when an exception occurs.
  Previously this could be specified by setting the environment
  variable ``RELSTORAGE_ABORT_EARLY``. Aborting early releases
  database locks to allow other transactions to make progress
  immediately. See :issue:`50`.

- Reduce the strength of locks taken by ``Connection.readCurrent`` so
  that they don't conflict with other connections that just want to
  verify they haven't changed. This also lets us immediately detect a
  conflict error with an in-progress transaction that is trying to
  alter those objects. See :issue:`302`.

- Make databases that use row-level locks (MySQL and PostgreSQL) raise
  specific exceptions on failures to acquire those locks. A different
  exception is raised for rows a transaction needs to modify compared
  to rows it only needs to read. Both are considered transient to
  encourage transaction middleware to retry. See :issue:`303`.

- Move more of the vote phase of transaction commit into a database
  stored procedure on MySQL and PostgreSQL, beginning with taking the
  row-level locks. This eliminates several more database round trips
  and the need for the Python thread (or greenlet) to repeatedly
  release and then acquire the GIL while holding global locks. See
  :issue:`304`.

- Make conflict resolution require fewer database round trips,
  especially on PostgreSQL and MySQL, at the expense of using more
  memory. In the ideal case it now only needs one (MySQL) or two
  (PostgreSQL) queries. Previously it needed at least twice the number
  of trips as there were conflicting objects. On both databases, the
  benchmarks are 40% to 80% faster (depending on cache configuration).

3.0a6 (2019-07-29)
==================

Enhancements
------------

- Eliminate a few extra round trips to the database on transaction
  completion: One extra ``ROLLBACK`` in all databases, and one query
  against the ``transaction`` table in history-preserving databases.
  See :issue:`159`.

- Prepare more statements used during regular polling.

- Gracefully handle certain disconnected exceptions when rolling back
  connections in between transactions. See :issue:`280`.

- Fix a cache error ("TypeError: NoneType object is not
  subscriptable") when an object had been deleted (such as through
  undoing its creation transaction, or with ``multi-zodb-gc``).

- Implement ``IExternalGC`` for history-preserving databases. This
  lets them be used with `zc.zodbdgc
  <https://pypi.org/project/zc.zodbdgc/>`_, allowing for
  multi-database garbage collection (see :issue:`76`). Note that you
  must pack the database after running ``multi-zodb-gc`` in order to
  reclaim space.

  .. caution::

     It is critical that ``pack-gc`` be turned off (set to false) in a
     multi-database and that only ``multi-zodb-gc`` be used to perform
     garbage collection.

Packing
~~~~~~~

- Make ``RelStorage.pack()`` also accept a TID from the RelStorage
  database to pack to. The usual Unix timestamp form for choosing a
  pack time can be ambiguous in the event of multiple transactions
  within a very short period of time. This is mostly a concern for
  automated tests.

  Similarly, it will accept a value less than 0 to mean the most
  recent transaction in the database. This is useful when machine
  clocks may not be well synchronized, or from automated tests.

Implementation
--------------

- Remove vestigial top-level thread locks. No instance of RelStorage
  is thread safe.

  RelStorage is an ``IMVCCStorage``, which means that each ZODB
  ``Connection`` gets its own new storage object. No visible storage
  state is shared among Connections. Connections are explicitly
  documented as not being thread safe. Since 2.0, RelStorage's
  Connection instances have taken advantage of that fact to be a
  little lighter weight through not being thread safe. However, they
  still paid the overhead of locking method calls and code complexity.

  The top-level storage (the one belonging to a ``ZODB.DB``) still
  used heavyweight locks in earlier releases. ``ZODB.DB.storage`` is
  documented as being only useful for tests, and the ``DB`` object
  itself does not expose any operations that use the storage in a way
  that would require thread safety.

  The remaining thread safety support has been removed. This
  simplifies the code and reduces overhead.

  If you were previously using the ``ZODB.DB.storage`` object, or a
  ``RelStorage`` instance you constructed manually, from multiple
  threads, instead make sure each thread has a distinct
  ``RelStorage.new_instance()`` object.

- A ``RelStorage`` instance now only implements the appropriate subset
  of ZODB storage interfaces according to its configuration. For
  example, if there is no configured ``blob-dir``, it won't implement
  ``IBlobStorage``, and if ``keep-history`` is false, it won't
  implement ``IStorageUndoable``.

- Refactor RelStorage internals for a cleaner separation of concerns.
  This includes how (some) queries are written and managed, making it
  easier to prepare statements, but only those actually used.


MySQL
-----

- On MySQL, move allocating a TID into the database. On benchmarks
  of a local machine this can be a scant few percent faster, but it's
  primarily intended to reduce the number of round-trips to the
  database. This is a step towards :issue:`281`. See :pr:`286`.

- On MySQL, set the connection timezone to be UTC. This is necessary
  to get values consistent between ``UTC_TIMESTAMP``,
  ``UNIX_TIMESTAMP``, ``FROM_UNIXTIME``, and Python's ``time.gmtime``,
  as used for comparing TIDs.

- On MySQL, move most steps of finishing a transaction into a stored
  procedure. Together with the TID allocation changes, this reduces
  the number of database queries from::

    1 to lock
     + 1 to get TID
     + 1 to store transaction (0 in history free)
     + 1 to move states
     + 1 for blobs (2 in history free)
     + 1 to set current (0 in history free)
     + 1 to commit
    = 7 or 6 (in history free)

  down to 1. This is expected to be especially helpful for gevent
  deployments, as the database lock is held, the transaction finalized
  and committed, and the database lock released, all without involving
  greenlets or greenlet switches. By allowing the GIL to be released
  longer it may also be helpful for threaded environments. See
  :issue:`281` and :pr:`287` for benchmarks and specifics.

  .. caution::

    MySQL 5.7.18 and earlier contain a severe bug that causes the
    server to crash when the stored procedure is executed.


- Make PyMySQL use the same precision as mysqlclient when sending
  floating point parameters.

- Automatically detect when MySQL stored procedures in the database
  are out of date with the current source in this package and replace
  them.

PostgreSQL
----------

- As for MySQL, move allocating a TID into the database.

- As for MySQL, move most steps of finishing a transaction into a
  stored procedure. On psycopg2 and psycopg2cffi this is done in a
  single database call. With pg8000, however, it still takes two, with
  the second call being the COMMIT call that releases locks.

- Speed up getting the approximate number of objects
  (``len(storage)``) in a database by using the estimates collected by
  the autovacuum process or analyzing tables, instead of asking for a
  full table scan.

3.0a5 (2019-07-11)
==================

- Reduce the time that MySQL will wait to perform OID garbage
  collection on startup. See :issue:`271`.

- Fix several instances where RelStorage could attempt to perform
  operations on a database connection with outstanding results on a
  cursor. Some database drivers can react badly to this, depending on
  the exact circumstances. For example, mysqlclient can raise
  ``ProgrammingError: (2014, "Commands out of sync; you can't run this
  command now")``. See :issue:`270`.

- Fix the "gevent MySQLdb" driver to be cooperative during ``commit``
  and ``rollback`` operations. Previously, it would block the event
  loop for the entire time it took to send the commit or rollback
  request, the server to perform the request, and the result to be
  returned. Now, it frees the event loop after sending the request.
  See :issue:`272`.

- Call ``set_min_oid`` less often if a storage is just updating
  existing objects, not creating its own.

- Fix an occasional possible deadlock in MySQL's ``set_min_oid``. See
  :pr:`276`.

3.0a4 (2019-07-10)
==================

- Add support for the ZODB 5 ``connection.prefetch(*args)`` API. This
  takes either OIDs (``obj._p_oid``) or persistent ghost objects, or
  an iterator of those things, and asks the storage to load them into
  its cache for use in the future. In RelStorage, this uses the shared
  cache and so may be useful for more than one thread. This can be
  3x or more faster than loading objects on-demand. See :issue:`239`.

- Stop chunking blob uploads on PostgreSQL. All supported PostgreSQL
  versions natively handle blobs greater than 2GB in size, and the
  server was already chunking the blobs for storage, so our layer of
  extra chunking has become unnecessary.

  .. important::

     The first time a storage is opened with this version,
     blobs that have multiple chunks will be collapsed into a single
     chunk. If there are many blobs larger than 2GB, this could take
     some time.

     It is recommended you have a backup before installing this
     version.

     To verify that the blobs were correctly migrated, you should
     clean or remove your configured blob-cache directory, forcing new
     blobs to be downloaded.

- Fix a bug that left large objects behind if a PostgreSQL database
  containing any blobs was ever zapped (with ``storage.zap_all()``).
  The ``zodbconvert`` command, the ``zodbshootout`` command, and the
  RelStorage test suite could all zap databases. Running the
  ``vacuumlo`` command included with PostgreSQL will free such
  orphaned large objects, after which a regular ``vacuumdb`` command
  can be used to reclaim space. See :issue:`260`.

- Conflict resolution can use data from the cache, thus potentially
  eliminating a database hit during a very time-sensitive process.
  Please file issues if you encounter any strange behaviour when
  concurrently packing to the present time and also resolving
  conflicts, in case there are corner cases.

- Packing a storage now invalidates the cached values that were packed
  away. For the global caches this helps reduce memory pressure; for
  the local cache this helps reduce memory pressure and ensure a more
  useful persistent cache (this probably matters most when running on
  a single machine).

- Make MySQL use ``ON DUPLICATE KEY UPDATE`` rather than ``REPLACE``.
  This can be friendlier to the storage engine as it performs an
  in-place ``UPDATE`` rather than a ``DELETE`` followed by an
  ``INSERT``. See :issue:`189`.

- Make PostgreSQL use an upsert query for moving rows into place on
  history-preserving databases.

- Support ZODB 5's parallel commit feature. This means that the
  database-wide commit lock is taken much later in the process, and
  held for a much shorter time than before.

  Previously, the commit lock was taken during the ``tpc_vote`` phase,
  and held while we checked ``Connection.readCurrent`` values, and
  checked for (and hopefully resolved) conflicts. Other transaction
  resources (such as other ZODB databases in a multi-db setup) then
  got to vote while we held this lock. Finally, in ``tpc_finally``,
  objects were moved into place and the lock was released. This
  prevented any other storage instances from checking for
  ``readCurrent`` or conflicts while we were doing that.

  Now, ``tpc_vote`` is (usually) able to check
  ``Connection.readCurrent`` and check and resolve conflicts without
  taking the commit lock. Only in ``tpc_finish``, when we need to
  finally allocate the transaction ID, is the commit lock taken, and
  only held for the duration needed to finally move objects into
  place. This allows other storages for this database, and other
  transaction resources for this transaction, to proceed with voting,
  conflict resolution, etc, in parallel.

  Consistent results are maintained by use of object-level row
  locking. Thus, two transactions that attempt to modify the same
  object will now only block each other.

  There are two exceptions. First, if the ``storage.restore()`` method
  is used, the commit lock must be taken very early (before
  ``tpc_vote``). This is usually only done as part of copying one
  database to another. Second, if the storage is configured with a
  shared blob directory instead of a blob cache (meaning that blobs
  are *only* stored on the filesystem) and the transaction has added
  or mutated blobs, the commit lock must be taken somewhat early to
  ensure blobs can be saved (after conflict resolution, etc, but
  before the end of ``tpc_vote``). It is recommended to store blobs on
  the RDBMS server and use a blob cache. The shared blob layout can be
  considered deprecated for this reason).

  In addition, the new locking scheme means that packing no longer
  needs to acquire a commit lock and more work can proceed in parallel
  with regular commits. (Though, there may have been some regressions
  in the deletion phase of packing speed MySQL; this has not been
  benchmarked.)

  .. note::

     If the environment variable ``RELSTORAGE_LOCK_EARLY`` is
     set when RelStorage is imported, then parallel commit will not be
     enabled, and the commit lock will be taken at the beginning of
     the tpc_vote phase, just like before: conflict resolution and
     readCurrent will all be handled with the lock held.

     This is intended for use diagnosing and temporarily working
     around bugs, such as the database driver reporting a deadlock
     error. If you find it necessary to use this setting, please
     report an issue at https://github.com/zodb/relstorage/issues.

  See :issue:`125`.

- Deprecate the option ``shared-blob-dir``. Shared blob dirs prevent
  using parallel commits when blobs are part of a transaction.

- Remove the 'umysqldb' driver option. This driver exhibited failures
  with row-level locking used for parallel commits. See :issue:`264`.

- Migrate all remaining MySQL tables to InnoDB. This is primarily the
  tables used during packing, but also the table used for allocating
  new OIDs.

  Tables will be converted the first time a storage is opened that is
  allowed to create the schema (``create-schema`` in the
  configuration; default is true). For large tables, this may take
  some time, so it is recommended to finish any outstanding packs
  before upgrading RelStorage.

  If schema creation is not allowed, and required tables are not using
  InnoDB, an exception will be raised. Please contact the RelStorage
  maintainers on GitHub if you have a need to use a storage engine
  besides InnoDB.

  This allows for better error detection during packing with parallel
  commits. It is also required for `MySQL Group Replication
  <https://dev.mysql.com/doc/refman/8.0/en/group-replication-requirements.html>`_.
  Benchmarking also shows that creating new objects can be up to 15%
  faster due to faster OID allocation.

  Things to be aware of:

    - MySQL's `general conversion notes
      <https://dev.mysql.com/doc/refman/8.0/en/converting-tables-to-innodb.html>`_
      suggest that if you had tuned certain server parameters for
      MyISAM tables (which RelStorage only used during packing) it
      might be good to evaluate those parameters again.
    - InnoDB tables may take more disk space than MyISAM tables.
    - The ``new_oid`` table may temporarily have more rows in it at one
      time than before. They will still be garbage collected
      eventually. The change in strategy was necessary to handle
      concurrent transactions better.

  See :issue:`188`.

- Fix an ``OperationalError: database is locked`` that could occur on
  startup if multiple processes were reading or writing the cache
  database. See :issue:`266`.


3.0a3 (2019-06-26)
==================

- Zapping a storage now also removes any persistent cache files. See
  :issue:`241`.

- Zapping a MySQL storage now issues ``DROP TABLE`` statements instead
  of ``DELETE FROM`` statements. This is much faster on large
  databases. See :issue:`242`.

- Workaround the PyPy 7.1 JIT bug using MySQL Connector/Python. It is no
  longer necessary to disable the JIT in PyPy 7.1.

- On PostgreSQL, use PostgreSQL's efficient binary ``COPY FROM`` to
  store objects into the database. This can be 20-40% faster. See
  :issue:`247`.

- Use more efficient mechanisms to poll the database for current TIDs
  when verifying serials in transactions.

- Silence a warning about ``cursor.connection`` from pg8000. See
  :issue:`238`.

- Poll the database for the correct TIDs of older transactions when
  loading from a persistent cache, and only use the entries if they
  are current. This restores the functionality lost in the fix for
  :issue:`249`.

- Increase the default cache delta limit sizes.

- Fix a race condition accessing non-shared blobs when the blob cache
  limit was reached which could result in blobs appearing to be
  spuriously empty. This was only observed on macOS. See :issue:`219`.

- Fix a bug computing the cache delta maps when restoring from
  persistent cache that could cause data from a single transaction to
  be stale, leading to spurious conflicts.

3.0a2 (2019-06-19)
==================

- Drop support for PostgreSQL versions earlier than 9.6. See
  :issue:`220`.

- Make MySQL and PostgreSQL use a prepared statement to get
  transaction IDs. PostgreSQL also uses a prepared statement to set
  them. This can be slightly faster. See :issue:`246`.

- Make PostgreSQL use a prepared statement to move objects to their
  final destination during commit (history free only). See
  :issue:`246`.

- Fix an issue with persistent caches written to from multiple
  instances sometimes getting stale data after a restart. Note: This
  makes the persistent cache less useful for objects that rarely
  change in a database that features other actively changing objects;
  it is hoped this can be addressed in the future. See :issue:`249`.

3.0a1 (2019-06-12)
==================

- Add support for Python 3.7.

- Drop support for Python 3.4.

- Drop support for Python 2.7.8 and earlier.

- Drop support for ZODB 4 and ZEO 4.

- Officially drop support for versions of MySQL before 5.7.9. We haven't
  been testing on anything older than that for some time, and older
  than 5.6 for some time before that.

- Drop the ``poll_interval`` parameter. It has been deprecated with a
  warning and ignored since 2.0.0b2. See :issue:`222`.

- Drop support for pg8000 older than 1.11.0.

- Drop support for MySQL Connector/Python older than 8.0.16. Many
  older versions are known to be broken. Note that the C extension,
  while available, is not currently recommended due to internal
  errors. See :issue:`228`.

- Test support for MySQL Connector/Python on PyPy. See :issue:`228`.

  .. caution:: Prior to PyPy 7.2 or RelStorage 3.0a3, it is necessary to disable JIT
               inlining due to `a PyPy bug
               <https://bitbucket.org/pypy/pypy/issues/3014/jit-issue-inlining-structunpack-hh>`_
               with ``struct.unpack``.

- Drop support for PyPy older than 5.3.1.

- Drop support for the "MySQL Connector/Python" driver name since it
  wasn't possible to know if it would use the C extension or the
  Python implementation. Instead, explicitly use the 'Py' or 'C'
  prefixed name. See :pr:`229`.

- Drop the internal and undocumented environment variables that could be
  used to force configurations that did not specify a database driver
  to use a specific driver. Instead, list the driver in the database
  configuration.

- Opening a RelStorage configuration object read from ZConfig more
  than once would lose the database driver setting, reverting to
  'auto'. It now retains the setting. See :issue:`231`.

- Fix Python 3 with mysqlclient 1.4. See :issue:`213`.

- Drop support for mysqlclient < 1.4.

- Make driver names in RelStorage configurations case-insensitive
  (e.g., 'MySQLdb' and 'mysqldb' are both valid). See :issue:`227`.

- Rename the column ``transaction.empty`` to ``transaction.is_empty``
  for compatibility with MySQL 8.0, where ``empty`` is now a reserved
  word. The migration will happen automatically when a storage is
  first opened, unless it is configured not to create the schema.

  .. note:: This migration has not been tested for Oracle.

  .. note:: You must run this migration *before* attempting to upgrade
            a MySQL 5 database to MySQL 8. If you cannot run the
            upgrade through opening the storage, the statement is
            ``ALTER TABLE transaction CHANGE empty is_empty BOOLEAN
            NOT NULL DEFAULT FALSE``.

- Stop getting a warning about invalid optimizer syntax when packing a
  MySQL database (especially with the PyMySQL driver). See
  :issue:`163`.

- Add ``gevent MySQLdb``, a new driver that cooperates with gevent
  while still using the C extensions of ``mysqlclient`` to communicate
  with MySQL. This is now recommended over ``umysqldb``, which is
  deprecated and will be removed.

- Rewrite the persistent cache implementation. It now is likely to
  produce much higher hit rates (100% on some benchmarks, compared to
  1-2% before). It is currently slower to read and write, however.
  This is a work in progress. See :pr:`243`.

- Add more aggressive validation and, when possible, corrections for
  certain types of cache consistency errors. Previously an
  ``AssertionError`` would be raised with the message "Detected an
  inconsistency between RelStorage and the database...". We now
  proactively try harder to avoid that situation based on some
  educated guesses about when it could happen, and should it still
  happen we now reset the cache and raise a type of ``TransientError``
  allowing the application to retry. A few instances where previously
  incorrect data could be cached may now raise such a
  ``TransientError``. See :pr:`245`.

2.1.1 (2019-01-07)
==================

- Avoid deleting attributes of DB driver modules we import. Fixes
  :issue:`206` reported by Josh Zuech.


2.1.0 (2018-02-07)
==================

- Document that installing RelStorage from source requires a working
  CFFI compilation environment. Fixes :issue:`187`, reported by
  Johannes Raggam.

- Test with MySQL Connector/Python 8.0.6, up from 2.1.5. Note that
  PyPy 5.8.0 is known to *not* work with MySQL Connector/Python
  (although PyPy 5.6.0 did).


2.1a2 (2017-04-15)
==================

- Implemented the storage ``afterCompletion`` method, which allows
  RelStorage storages to be notified of transaction endings for
  transactions that don't call the two-phase commit API.  This allows
  resources to be used more efficiently because it prevents RDBMS
  transactions from being held open.

  Fixes: :issue:`147` (At least for ZODB 5.2.)

- Oracle: Fix two queries that got broken due to the performance work
  in 2.1a1.

- MySQL: Workaround a rare issue that could lead to a ``TypeError``
  when getting new OIDs. See :issue:`173`.

- The ``len`` of a RelStorage instance now correctly reflects the
  approximate number of objects in the database. Previously it
  returned a hardcoded 0. See :issue:`178`.

- MySQL: Writing blobs to the database is much faster and scales much
  better as more blobs are stored. The query has been rewritten to use
  existing primary key indexes, whereas before it used a table scan
  due to deficiencies in the MySQL query optimizer. Thanks to Josh
  Zuech and enfold-josh. See :issue:`175`.

2.1a1 (2017-02-01)
==================

- 3.6.0 final release is tested on CI servers.
- Substantial performance improvements for PostgreSQL, both on reading
  and writing. Reading objects can be 20-40% faster. Writing objects
  can be 15-25% faster (the most benefit will be seen by history-free
  databases on PostgreSQL 9.5 and above). MySQL may have a (much)
  smaller improvement too, especially for small transactions. This was
  done through the use of prepared statements for the most important
  queries and the new `'ON CONFLICT UPDATE'
  <https://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.5#INSERT_..._ON_CONFLICT_DO_NOTHING.2FUPDATE_.28.22UPSERT.22.29>`_
  syntax. See :pr:`157` and :issue:`156`.
- The umysqldb driver no longer attempts to automatically reconnect on
  a closed cursor exception. That fails now that prepared statements
  are in use. Instead, it translates the internal exception to one
  that the higher layers of RelStorage recognize as requiring
  reconnection at consistent times (transaction boundaries).
- Add initial support for the `MySQL Connector/Python
  <https://dev.mysql.com/doc/connector-python/en/>`_ driver. See
  :issue:`155`.
- Backport `ZODB #140
  <https://github.com/zopefoundation/ZODB/pull/140>`_ to older
  versions of ZODB. This improves write performance, especially in
  multi-threaded scenarios, by up to 10%. See :pr:`160`.
- MySQL temporary tables now use the InnoDB engine instead of MyISAM.
  See :pr:`162`.

2.0.0 (2016-12-23)
==================

- MySQL and Postgres now use the same optimized methods to get the
  latest TID at transaction commit time as they do at poll time. This
  is similar to :issue:`89`.
- MySQL now releases the commit lock (if acquired) during pre-pack
  with GC of a history-free storage at the same time as PostgreSQL and
  Oracle did (much earlier). Reported and initial fix provided in
  :pr:`9` by jplouis.


2.0.0rc1 (2016-12-12)
=====================

- Writing persistent cache files has been changed to reduce the risk
  of stale temporary files remaining. Also, files are kept open for a
  shorter period of time and removed in a way that should work better
  on Windows.

- RelStorage is now tested on Windows for MySQL and PostgreSQL thanks
  to AppVeyor.

- Add support for Python 3.6.

2.0.0b9 (2016-11-29)
====================

- The MySQL adapter will now produce a more informative error if it
  gets an unexpected result taking the commit lock. Reported by Josh
  Zuech.

- Compatibility with transaction 2.0 on older versions of ZODB (prior
  to the unreleased version that handles encoding meta data for us),
  newer versions of ZODB (that do the encoding), while maintaining
  compatibility with transaction 1.x. In particular, the ``history``
  method consistently returns bytes for username and description.

- In very rare cases, persistent cache files could result in a corrupt
  cache state in memory after loading them, resulting in
  AttributeErrors until the cache files were removed and the instance
  restarted. Reported in :issue:`140` by Carlos Sanchez.

2.0.0b8 (2016-10-02)
====================

- List CFFI in `setup_requires` for buildout users.


2.0.0b7 (2016-10-01)
====================

- Add the ability to limit the persistent cache files size. Thanks to
  Josh Zuech for the suggestion, which led to the next change.

- Move the RelStorage shared cache to a `windowed-LFU with segmented
  LRU
  <http://highscalability.com/blog/2016/1/25/design-of-a-modern-cache.html>`_
  instead of a pure LRU model. This can be a nearly optimal caching
  strategy for many workloads. The caching code itself is also faster
  in all tested cases.

  It's especially helpful when using persistent cache files together
  with a file size limit, as we can now ensure we write out the most
  frequently useful data to the file instead of just the newest.

  For more information see :issue:`127` and :pr:`128`. Thanks to Ben
  Manes for assistance talking through issues related to the cache
  strategy.

  For write-heavy workloads, you may want to increase
  ``cache_delta_size_limit``.

  The internal implementation details of the cache have been
  completely changed. Only the ``StorageCache`` class remains
  unchanged (though that's also an implementation class). CFFI is now
  required, and support for PyPy versions older than 2.6.1 has been dropped.

- On CPython, use LLBTrees for the cache delta maps. This allows using
  a larger, more effective size while reducing memory usage. Fixes :issue:`130`.

- Persistent cache files use the latest TID in the cache as the file's
  modification time. This allows a more accurate choice of which file
  to read at startup. Fixes :issue:`126`.

- Fix packing of history-preserving Oracle databases. Reported in
  :issue:`135` by Peter Jacobs.

2.0.0b6 (2016-09-08)
====================

- Use ``setuptools.find_packages`` and ``include_package_data`` to
  ensure wheels have all the files necessary. This corrects an issue
  with the 2.0.0b5 release on PyPI. See :issue:`121` by Carlos Sanchez.


2.0.0b5 (2016-08-24)
====================

- Supporting new databases should be simpler due to a code
  restructuring. Note that many internal implementation classes have
  moved or been renamed.
- The umysqldb support handles query transformations more efficiently.
- umysqldb now raises a more informative error when the server sends
  too large a packet.

  .. note:: If you receive "Socket receive buffer full" errors, you
            are likely experiencing `this issue <https://github.com/esnme/ultramysql/issues/34>`_ in ultramysql and
            will need a patched version, such as the one provided in
            `this pull request
            <https://github.com/esnme/ultramysql/pull/61>`_.
- The local persistent cache file format has been changed to improve
  reading and writing speed. Old files will be cleaned up
  automatically. Users of the default settings could see improvements
  of up to 3x or more on reading and writing.
- Compression of local persistent cache files has been disabled by
  default (but there is still an option to turn it back on).
  Operational experience showed that it didn't actually save that much
  disk space, while substantially slowing down the reading and writing
  process (2-4x).
- Add an option, ``cache-local-dir-read-count`` to limit the maximum
  number of persistent local cache files will be used to populate a
  storages's cache. This can be useful to reduce startup time if cache
  files are large and workers have mostly similar caches.

2.0.0b4 (2016-07-17)
====================

- Add experimental support for umysqldb as a MySQL driver for Python
  2.7. This is a gevent-compatible driver implemented in C for speed.
  Note that it may not be able to store large objects (it has been
  observed to fail for a 16M object---it hardcodes a
  ``max_allowed_packet`` of exactly 16MB for read and write buffers),
  and has been observed to have some other stability issues.


2.0.0b3 (2016-07-16)
====================

- Add support for ZODB 5. RelStorage continues to run on ZODB 4 >=
  4.4.2.
- Add support for tooling to help understand RelStorage cache
  behaviour. This can help tune cache sizes and the choice to use
  Memcached or not. See :issue:`106` and :pr:`108`.
- Fix a threading issue with certain database drivers.

2.0.0b2 (2016-07-08)
====================

Breaking Changes
----------------

- Support for cx_Oracle versions older than 5.0 has been dropped. 5.0
  was released in 2008.

- Support for PostgreSQL 8.1 and earlier has been dropped. 8.2 is
  likely to still work, but 9.0 or above is recommended. 8.2 was
  released in 2006 and is no longer supported by upstream. The oldest
  version still supported by upstream is 9.1, released in 2011.


Platform Support
----------------

- Using ZODB >= 4.4.2 (*but not 5.0*) is recommended to avoid
  deprecation warnings due to the introduction of a new storage
  protocol. The next major release of RelStorage will require ZODB
  4.4.2 or above and should work with ZODB 5.0.

- Change the recommended and tested MySQL client for Python 2.7 away
  from the unmaintained MySQL-python to the maintained mysqlclient
  (the same one used by Python 3).

- PyMySQL now works and is tested on Python 3.

- A pure-Python PostgreSQL driver, pg8000, now works and is tested on
  all platforms. This is a gevent-compatible driver. Note that it
  requires a PostgreSQL 9.4 server or above for BLOB support.

- Support explicitly specifying the database driver to use. This can
  be important when there is a large performance difference between
  drivers, and more than one might be installed. (Also, RelStorage no
  longer has the side-effect of registering ``PyMySQL`` as ``MySQLdb`` and
  ``psycopg2cffi`` as ``psycopg2``.) See :issue:`86`.


Bug Fixes
---------

- Memcache connections are explicitly released instead of waiting for
  GC to do it for us. This is especially important with PyPy and/or
  ``python-memcached``. See :issue:`80`.

- The ``poll-interval`` option is now ignored and polling is performed
  when the ZODB Connection requests it (at transaction boundaries).
  Experience with delayed polling has shown it typically to do more
  harm than good, including introducing additional possibilities for
  error and leading to database performance issues. It is expected
  that most sites won't notice any performance difference. A larger
  discussion can be found in :issue:`87`.

Performance
-----------

- Support a persistent on-disk cache. This can greatly speed up
  application warmup after a restart (such as when deploying new code).
  Some synthetic benchmarks show an 8-10x improvement. See :issue:`92`
  for a discussion, and see the options ``cache-local-dir`` and
  ``cache-local-dir-count``.

- Instances of :class:`.RelStorage` no longer use threading locks by
  default and hence are not thread safe. A ZODB :class:`Connection
  <ZODB.interfaces.IConnection>` is documented as not being
  thread-safe and must be used only by a single thread at a time.
  Because RelStorage natively implements MVCC, each Connection has a
  unique storage object. It follows that the storage object is used
  only by a single thread. Using locks just adds unneeded overhead to
  the common case. If this is a breaking change for you, please open
  an issue. See :pr:`91`.

- MySQL uses (what should be) a slightly more efficient poll query.
  See :issue:`89`.

- The in-memory cache allows for higher levels of concurrent
  operation via finer-grained locks. For example, compression and
  decompression are no longer done while holding a lock.

- The in-memory cache now uses a better approximation of a LRU
  algorithm with less overhead, so more data should fit in the same
  size cache. (For best performance, CFFI should be installed; a
  warning is generated if that is not the case.)

- The in-memory cache is now smart enough not to store compressed
  objects that grow during compression, and it uses the same
  compression markers as zc.zlibstorage to avoid double-compression.
  It can also gracefully handle changes to the compression format in
  persistent files.

2.0.0b1 (2016-06-28)
====================

Breaking Changes
----------------

- Update the ZODB dependency from ZODB3 3.7.0 to ZODB 4.3.1. Support
  for ZODB older than 3.10 has been removed; ZODB 3.10 may work, but
  only ZODB 4.3 is tested.

- Remove support for Python 2.6 and below. Python 2.7 is now required.

Platform Support
----------------

- Add support for PyPy on MySQL and PostgreSQL using PyMySQL and
  psycopg2cffi respectively. PyPy can be substantially faster than
  CPython in some scenarios; see :pr:`23`.

- Add initial support for Python 3.4+ for MySQL (using mysqlclient), PostgreSQL,
  and Oracle.

Bug Fixes
---------

- Fixed ``loadBefore`` of a deleted/undone object to correctly raise a
  POSKeyError instead of returning an empty state. (Revealed by
  updated tests for FileStorage in ZODB 4.3.1.)

- Oracle: Packing should no longer produce LOB errors. This partially
  reverts the speedups in 1.6.0b2. Reported in :issue:`30` by Peter
  Jacobs.

- :meth:`.RelStorage.registerDB` and :meth:`.RelStorage.new_instance`
  now work with storage wrappers like zc.zlibstorage. See :issue:`70`
  and :issue:`71`.

Included Utilities
------------------

- zodbconvert: The ``--incremental`` option is supported with a
  FileStorage (or any storage that implements
  ``IStorage.lastTransaction()``) as a destination, not just
  RelStorages.

- zodbconvert: The ``--incremental`` option works correctly with a
  RelStorage as a destination. See :pr:`22`. With contributions by
  Sylvain Viollon, Mauro Amico, and Peter Jacobs. Originally reported
  by Jan-Wijbrand Kolman.

- PostgreSQL: ``zodbconvert --clear`` should be much faster when the
  destination is a PostgreSQL schema containing lots of data. *NOTE*:
  There can be no other open RelStorage connections to the destination,
  or any PostgreSQL connection in general that might be holding locks
  on the RelStorage tables, or ``zodbconvert`` will block indefinitely
  waiting for the locks to be released. Partial fix for :issue:`16`
  reported by Chris McDonough.

- ``zodbconvert`` and ``zodbpack`` use :mod:`argparse` instead of
  :mod:`optparse` for command line handling.

Performance
-----------

- MySQL: Use the "binary" character set to avoid producing "Invalid
  utf8 character string" warnings. See :issue:`57`.

- Conflict resolution uses the locally cached state instead of
  re-reading it from the database (they are guaranteed to be the
  same). See :issue:`38`.

- Conflict resolution reads all conflicts from the database in one
  query, instead of querying for each individual conflict. See
  :issue:`39`.

- PostgreSQL no longer encodes and decodes object state in Base64
  during database communication thanks to database driver
  improvements. This should reduce network overhead and CPU usage for
  both the RelStorage client and the database server. psycopg2 2.4.1
  or above is required; 2.6.1 or above is recommended. (Or
  psycopg2cffi 2.7.4.)

- PostgreSQL 9.3: Support ``commit-lock-timeout``. Contributed in :pr:`20`
  by Sean Upton.


Other Enhancements
------------------

- Raise a specific exception when acquiring the commit lock
  (:exc:`~relstorage.adapters.interfaces.UnableToAcquireCommitLockError`) or pack
  lock (:exc:`~relstorage.adapters.interfaces.UnableToAcquirePackUndoLockError`)
  fails. See :pr:`18`.

- ``RelStorage.lastTransaction()`` is more consistent with FileStorage
  and ClientStorage, returning a useful value in more cases.

- Oracle: Add support for getting the database size. Contributed in
  :pr:`21` by Mauro Amico.

- Support :class:`ZODB.interfaces.IExternalGC` for history-free
  databases, allowing multi-database garbage collection with
  ``zc.zodbdgc``. See :issue:`47`.

Project Details
---------------

- Travis CI is now used to run RelStorage tests against MySQL and
  PostgreSQL on every push and pull request. CPython 2 and 3 and PyPy
  are all tested with the recommended database drivers.

- Documentation has been reorganized and moved to `readthedocs
  <http://relstorage.readthedocs.io>`_.

- Updated the buildout configuration to just run relstorage tests and
  to select which databases to use at build time.


1.6.1 (2016-08-30)
==================

- Tests: Basic integration testing is done on Travis CI. Thanks to
  Mauro Amico.

- ``RelStorage.lastTransaction()`` is more consistent with FileStorage
  and ClientStorage, returning a useful value in more cases.

- zodbconvert: The ``--incremental`` option is supported with a
  FileStorage (or any storage that implements
  ``IStorage.lastTransaction()``) as a destination, not just
  RelStorages.

- zodbconvert: The ``--incremental`` option is supported with a
  RelStorage as a destination. See :pr:`22`. With contributions by
  Sylvain Viollon, Mauro Amico, and Peter Jacobs. Originally reported
  by Jan-Wijbrand Kolman.

- Oracle: Packing should no longer produce LOB errors. This partially
  reverts the speedups in 1.6.0b2. Reported in :issue:`30` by Peter
  Jacobs.

1.6.0 (2016-06-09)
==================

- Tests: Use the standard library doctest module for compatibility
  with newer zope.testing releases.

1.6.0b3 (2014-12-08)
====================

- Packing: Significantly reduced the RAM consumed by graph traversal during
  the pre_pack phase.  (Tried several methods; encoded 64 bit IISets turned
  out to be the most optimal.)


1.6.0b2 (2014-10-03)
====================

- Packing: Used cursor.fetchmany() to make packing more efficient.


1.6.0b1 (2014-09-04)
====================

- The local cache is now more configurable and uses ``zlib`` compression
  by default.

- Added support for ``zodburi``, which means you can open a storage
  using "postgres:", "mysql:", or "oracle:" URIs.

- Packing: Reduced RAM consumption while packing by using IIBTree.Set
  instead of built-in set objects.

- MySQL 5.5: The test suite was freezing in checkBackwardTimeTravel. Fixed.

- Added performance metrics using the perfmetrics package.

- zodbconvert: Add an --incremental option to the zodbconvert script,
  letting you convert additional transactions at a later date, or
  update a non-live copy of your database, copying over missing
  transactions.

- Replication: Added the ro-replica-conf option, which tells RelStorage
  to use a read-only database replica for load connections. This makes
  it easy for RelStorage clients to take advantage of read-only
  database replicas.

- Replication: When the database connection is stale (such as when
  RelStorage switches to an asynchronous replica that is not yet up to
  date), RelStorage will now raise ReadConflictError by default.
  Ideally, the application will react to the error by transparently
  retrying the transaction, while the database gets up to date. A
  subsequent transaction will no longer be stale.

- Replication: Added the revert-when-stale option. When this option is
  true and the database connection is stale, RelStorage reverts the
  ZODB connection to the stale state rather than raise
  ReadConflictError. This option is intended for highly available,
  read-only ZODB clients. This option would probably confuse users of
  read-write ZODB clients, whose changes would sometimes seem to be
  temporarily reverted.

- Caching: Use the database name as the cache-prefix by default. This
  will hopefully help people who accidentally use a single memcached for
  multiple databases.

- Fixed compatibility with persistent 4.0.5 and above.


1.5.1 (2011-11-12)
==================

- Packing: Lowered garbage collection object reference finding log level to
  debug; this stage takes mere seconds, even in large sites, but could produce
  10s of thousands of lines of log output.

- RelStorage was opening a test database connection (and was leaving it
  idle in a transaction with recent ZODB versions that support
  IMVCCStorage.) RelStorage no longer opens that test connection.

- zodbconvert: Avoid holding a list of all transactions in memory.

- Just after installing the database schema, verify the schema was
  created correctly. This affects MySQL in particular.


1.5.0 (2011-06-30)
==================

- PostgreSQL: Fixed another minor compatibility issue with PostgreSQL 9.0.
  Packing raised an error when the client used old an version of libpq.

- Delete empty transactions in batches of 1000 rows instead of all in one
  go, to prevent holding the transaction lock for longer than absolutely
  necessary.

- Oracle: Fix object reference downloading performance for large RelStorage
  databases during the garbage collection phase of a pack.

- Oracle, PostgreSQL: Switch to storing ZODB blob in chunks up to 4GB
  (the maximum supported by cx_Oracle) or 2GB (PostgreSQL maximum blob size)
  to maximize blob reading and writing performance.

  The PostgreSQL blob_chunk schema changed to support this, see
  notes/migrate-to-1.5.txt to update existing databases.

- zodbconvert: When copying a database containing blobs, ensure the source
  blob file exists long enough to copy it.


1.5.0b2 (2011-03-02)
====================

- Better packing based on experience with large databases.  Thanks
  to Martijn Pieters!

    - Added more feedback to the packing process. It'll now report
      during batch commit how much of the total work has been
      completed, but at most every .1% of the total number of
      transactions or objects to process.

    - Renamed the --dry-run option to --prepack and added a
      --use-prepack-state to zodbpack. With these 2 options the
      pre-pack and pack phases can be run separately, allowing re-use
      of the pre-pack analysis data or even delegating the pre-pack
      phase off to a separate server.

    - Replaced the packing duty cycle with a nowait locking strategy.
      The pack operation will now request the commit lock but pauses if
      it is already taken. It releases the lock after every batch
      (defaulting to 1 second processing). This makes the packing
      process faster while at the same time yielding to regular ZODB
      commits when busy.

    - Do not hold the commit lock during pack cleanup while deleting
      rows from the object reference tables; these tables are
      pack-specific and regular ZODB commits never touch these.

- Added an option to control schema creation / updating on startup.
  Setting the ``create-schema`` option to false will let you use
  RelStorage without a schema update.

- Fixed compatibility with PostgreSQL 9.0, which is capable of
  returning a new 'hex' type to the client. Some builds of psycopg2
  return garbage or raise an error when they see the new type. The fix
  was to encode more SQL query responses using base 64.

- With the new shared-blob-dir option set to false, it was possible
  for a thread to read a partially downloaded blob.  Fixed.  Thanks for
  the report from Maurits van Rees.

- Support for "shared-blob-dir false" now requires ZODB 3.9 or better.
  The code in the ZODB 3.8 version of ZODB.blob is not compatible with
  BlobCacheLayout, leading to blob filename collisions.


1.5.0b1 (2011-02-05)
====================

- Added a state_size column to object_state, making it possible
  to query the size of objects without loading the state.  The new
  column is intended for gathering statistics.  A schema migration
  is required.

- Added more logging during zodbconvert to show that something is
  happening and give an indication of how far along the process is.

- Fixed a missing import in the blob cache cleanup code.

- Added a --dry-run option to zodbpack.

- Replaced the graph traversal portion of the pack code with
  a more efficient implementation using Python sets (instead of SQL).
  The new code is much faster for packing databases with deeply
  nested objects.


1.5.0a1 (2010-10-21)
====================

- Added an option to store ZODB blobs in the database.  The option is
  called "shared-blob-dir" and it behaves very much like the ZEO
  option of the same name.  Blobs stored in the database are broken
  into chunks to reduce the impact on RAM.

- Require setuptools or distribute.  Plain distutils is not sufficient.


1.4.2 (2011-02-04)
==================

- Fixed compatibility with ZODB 3.10.  As reported by JĂźrgen Herrmann,
  there was a problem with conflict errors.  The RelStorage patch of the
  sync() method now works with ZODB 3.10.

- Fixed a bug in packing history-free databases.  If changes were
  made to the database during the pack, the pack code could delete
  too many objects.  Thanks to Chris Withers for writing test code
  that revealed the bug.  A schema migration is required for history-free
  databases; see notes/migration-to-1.4.txt.

- Enabled logging to stderr in zodbpack.


1.4.1 (2010-10-21)
==================

- Oracle: always connect in threaded mode.  Without threaded mode,
  clients of Oracle 11g sometimes segfault.


1.4.0 (2010-09-30)
==================

- Made compatible with ZODB 3.10.0b7.

- Enabled ketama and compression in pylibmc_wrapper.  Both options
  are better for clusters.  [Helge Tesdal]

- Oracle: Use a more optimal query for POSKeyError logging.  [Helge Tesdal]

- Fixed a NameError that occurred when getting the history of an
  object where transaction extended info was set.  [Helge Tesdal]


1.4.0c4 (2010-09-17)
====================

- Worked around an Oracle RAC bug: apparently, in a RAC environment,
  the read-only transaction mode does not isolate transactions in the
  manner specified by the documentation, so Oracle users now have to
  use serializable isolation like everyone else. It's slower but more
  reliable.

- Use the client time instead of the database server time as a factor
  in the transaction ID.  RelStorage was using the database server time
  to reduce the need for synchronized clocks, but in practice, that
  policy broke tests and did not really avoid the need to synchronize
  clocks.  Also, the effect of unsynchronized clocks is predictable
  and manageable: you'll get bunches of transactions with sequential
  timestamps.

- If the database returns an object from the future (which should never
  happen), generate a ReadConflictError, hopefully giving the application
  a chance to recover.  The most likely causes of this are a broken
  database and threading bugs.


1.4.0c3 (2010-07-31)
====================

- Always update the RelStorage cache when opening a database connection for
  loading, even when no ZODB Connection is using the storage.  Otherwise,
  code that used the storage interface directly could cause the cache
  to fall out of sync; the effects would be seen in the next
  ZODB.Connection.

- Added a ZODB monkey patch that passes the "force" parameter to the
  sync method.  This should help the poll-interval option do its job
  better.


1.4.0c2 (2010-07-28)
====================

- Fixed a subtle bug in the cache code that could lead to an
  AssertionError indicating a cache inconsistency.  The inconsistency
  was caused by after_poll(), which was ignoring the randomness of
  the order of the list of recent changes, leading it to sometimes
  put the wrong transfer ID in the "delta_after" dicts.  Also expanded
  the AssertionError with debugging info, since cache inconsistency
  can still be caused by database misconfiguration and mismatched
  client versions.

- Oracle: updated the migration notes.  The relstorage_util package
  is not needed after all.


1.4.0c1 (2010-06-19)
====================

- History-preserving storages now replace objects on restore instead of
  just inserting them.  This should solve problems people were
  having with the zodbconvert utility.

- Oracle: call the DBMS_LOCK.REQUEST function directly instead of using
  a small package named ``relstorage_util``. The ``relstorage_util``
  package was designed as a secure way to access the DBMS_LOCK package,
  but the package turned out to be confusing to DBAs and provided no
  real security advantage.  People who have already deployed
  RelStorage 1.4.x on Oracle need to do the following:

      GRANT EXECUTE ON DBMS_LOCK TO <zodb_user>;

  You can also drop the ``relstorage_util`` package.  Keep the
  ``relstorage_op`` package.

- Made compatible with ZODB 3.10.

- MySQL: specify the transaction isolation mode for every connection,
  since the default is apparently not necessarily "read committed"
  anymore.


1.4.0b3 (2010-02-02)
====================

- Auto-reconnect in new_oid().


1.4.0b2 (2010-01-30)
====================

- Include all test subpackages in setup.py.

- Raise an error if MySQL reverts to MyISAM rather than using the InnoDB
  storage engine.


1.4.0b1 (2009-11-17)
====================

- Added the keep-history option. Set it to false to keep no history.
  (Packing is still required for garbage collection and blob deletion.)

- Added the replica-conf and replica-timeout options.  Set replica-conf
  to a filename containing the location of database replicas.  Changes
  to the file take effect at transaction boundaries.

- Expanded the option documentation in README.txt.

- Revised the way RelStorage uses memcached.  Minimized the number of
  trips to both the cache server and the database.

- Added an in-process pickle cache that serves a function similar to the
  ZEO cache.

- Added a wrapper module for pylibmc.

- Store operations now use multi-insert and multi-delete SQL
  statements to reduce the effect of network latency.

- Renamed relstorage.py to storage.py to overcome import issues.
  Also moved the Options class to options.py.

- Updated the patch for ZODB 3.7 and 3.8 to fix an issue with
  blobs and subtransactions.

- Divided the implementation of database adapters into many small
  objects, making the adapter code more modular.  Added interfaces
  that describe the duties of each part.

- Oracle: Sped up restore operations by sending short blobs inline.

- Oracle: Use a timeout on commit locks.  This requires installation
  of a small PL/SQL package that can access DBMS_LOCK.  See README.txt.

- Oracle: Used PL/SQL bulk insert operations to improve write
  performance.

- PostgreSQL: use the documented ALTER SEQUENCE RESTART WITH
  statement instead of ALTER SEQUENCE START WITH.

- Moved MD5 sum computation to the adapters so they can choose not
  to use MD5.

- Changed loadSerial to load from the store connection only if the
  load connection can not provide the object requested.

- Stopped wrapping database disconnect exceptions.  Now the code
  catches and handles them directly.

- Use the store connection rather than the load connection for OID
  allocation.

- Detect and handle backward time travel, which can happen after
  failover to an out-of-date asynchronous slave database. For
  simplicity, invalidate the whole ZODB cache when this happens.

- Replaced the speed test script with a separately distributed package,
  ``zodbshootout``.

- Added the ``zodbpack`` script.


1.3.0b1 (2009-09-04)
====================

- Added support for a blob directory. No BlobStorage wrapper is needed.
  Cluster nodes will need to use a shared filesystem such as NFS or
  SMB/CIFS.

- Added the blob-dir parameter to the ZConfig schema and README.txt.



1.2.0 (2009-09-04)
==================

- In Oracle, trim transaction descriptions longer than 2000 bytes.

- When opening the database for the first time, don't issue a warning
  about the inevitable POSKeyError on the root OID.

- If RelStorage tries to unpickle a corrupt object state during packing,
  it will now report the oid and tid in the log.



1.2.0b2 (2009-05-05)
====================

- RelStorage now implements IMVCCStorage, making it compatible with
  ZODB 3.9.0b1 and above.

- Removed two-phase commit support from the PostgreSQL adapter. The
  feature turned out to be unnecessary.

- Added MySQL 5.1.34 and above to the list of supportable databases.

- Fixed minor test failures under Windows. Windows is now a supportable
  platform.
