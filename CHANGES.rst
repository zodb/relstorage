=========
 Changes
=========


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
  waiting for the locks to be release. Partial fix for :issue:`16`
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

Information about older releases can be found :doc:`here <HISTORY>`.
