====================
 Historical Changes
====================

1.1.3 (2009-02-04)
==================

- In rare circumstances, ZODB can legitimately commit an object twice in a
  single transaction.  Fixed RelStorage to accept that.

- Auto reconnect to Oracle sometimes did not work because cx_Oracle was
  raising a different kind of exception than expected.  Fixed.

- Included LICENSE.txt in the source distribution.


1.1.2 (2009-01-27)
==================

- When both cache-servers and poll-interval are set, we now poll the
  cache for changes on every request.  This makes it possible to use
  a high poll-interval to reduce the database polling burden, yet
  every client can see changes immediately.

- Added the pack-dry-run option, which causes pack operations to only
  populate the pack tables with the list of objects and states to pack,
  but not actually pack.

- Refined the pack algorithm.  It was not removing as many object states
  as it should have.  As a bonus, there is now a method of adapters called
  fill_object_refs(), which could be useful for debugging.  It ensures the
  object_ref table is fully populated.

- Began using zc.buildout for development.

- Increased automated test coverage.

- Fixed KeyError reporting to not trip over a related KeyError while logging.


1.1.1 (2008-12-27)
==================

- Worked around MySQL performance bugs in packing.  Used temporary
  tables and another column in the pack_object table.  The
  other databases may benefit from the optimization as well.

- Applied an optimization using setinputsizes() to the Oracle code,
  bringing write speed back up to where it was in version 1.0.


1.1 (2008-12-19)
================

- Normalized poll-invalidation patches as Solaris' patch command would not
  accept the current format. The patches now apply with:
  patch -d lib/python/ZODB -p0 < poll-invalidation-1-zodb-3-X-X.patch

- In MySQL, Use DROP TABLE IF EXISTS instead of TRUNCATE to clear 'temp_store'
  because:

  - TRUNCATE has one page of caveats in the MySQL documentation.
  - TEMPORARY TABLEs have half a page of caveats when it comes to
    replication.
  - The end result is that 'temp_store' may not exist on the
    replication slave at the exact same time(s) it exists on the
    master.

- Implemented the database size query in MySQL, based on a patch from
  Kazuhiko Shiozaki.  Thanks!

- Optimized Oracle object retrieval by causing BLOBs to be sent inline
  when possible, based on a patch by Helge Tesdal.  By default, the
  optimization is activated automatically when cx_Oracle 5 is used.

- Updated the storage iterator code to be compatible with ZODB 3.9.
  The RelStorage tests now pass with the shane-poll-invalidations branch
  of ZODB 3.9.

- Added a translation of README.txt to Brazilian Portuguese by
  Rogerio Ferreira.  Thanks!


1.1c1
=====

- Added optional memcache integration.  This is useful when the connection
  to the relational database has high latency.

- Made it possible to set the pack and memcache options in zope.conf.

- Log more info when a KeyError occurs within RelStorage.


1.1b2
=====

- Made the MySQL locks database-specific rather than server-wide.  This is
  important for multi-database configurations.

- In the PostgreSQL adapter, made the pack lock fall back to table locking
  rather than advisory locks for PostgreSQL 8.1.

- Changed a query for following object references (used during packing)
  to work around a MySQL performance bug.  Thanks to Anton Stonor for
  discovering this.


1.1b1
=====

- Fixed the use of setup.py without setuptools.  Thanks to Chris Withers.

- Fixed type coercion of the transaction extension field.  This fixes
  an issue with converting databases.  Thanks to Kevin Smith for
  discovering this.

- Added logging to the pack code to help diagnose performance issues.

- Additions to the object_ref table are now periodically committed
  during pre_pack so that the work is not lost if pre_pack fails.

- Modified the pack code to pack one transaction at a time and
  release the commit lock frequently.  This should help large pack
  operations.

- Fixed buildout-based installation of the zodbconvert script.  Thanks to
  Jim Fulton.


1.0.1 (2008-03-11)
==================

- The speedtest script failed if run on a test database that has no tables.
  Now the script creates the tables if needed.  Thanks to Flavio Coelho
  for discovering this.

- Reworked the auto-reconnect logic so that applications never see
  temporary database disconnects if possible.  Thanks to Rigel Di Scala
  for pointing out this issue.

- Improved the log messages explaining database connection failures.

- Moved poll_invalidations to the common adapter base class, reducing the
  amount of code to maintain.


1.0 (2008-02-29)
================

- Added a utility for converting between storages called zodbconvert.


1.0c1
=====

- The previous fix for non-ASCII characters was incorrect.  Now transaction
  metadata is stored as raw bytes.  A schema migration is required; see
  notes/migrate-1.0-beta.txt.

- Integrated setuptools and made an egg.


1.0 beta
========

- Renamed to reflect expanding database support.

- Added support for Oracle 10g.

- Major overhaul with many scalability and reliability improvements,
  particularly in the area of packing.

- Moved to svn.zope.org and switched to ZPL 2.1.

- Made two-phase commit optional in both Oracle and PostgreSQL.  They
  both use commit_lock in such a way that the commit is not likely to
  fail in the second phase.

- Switched most database transaction isolation levels from serializable
  to read committed.  It turns out that commit_lock already provides
  the serializability guarantees we need, so it is safe to take advantage
  of the potential speed gains.  The one major exception is the load
  connection, which requires an unchanging view of the database.

- Stored objects are now buffered in a database table rather than a file.

- Stopped using the LISTEN and NOTIFY statements in PostgreSQL since
  they are not strictly transactional in the sense we require.

- Started using a prepared statement in PostgreSQL for getting the
  newest transaction ID quickly.

- Removed the code in the Oracle adapter for retrying connection attempts.
  (It is better to just reconfigure Oracle.)

- Added support for MySQL 5.0.

- Added the poll_interval option.  It reduces the frequency of database
  polls, but it also increases the potential for conflict errors on
  servers with high write volume.

- Implemented the storage iterator protocol, making it possible to copy
  transactions to and from FileStorage and other RelStorage instances.

- Fixed a bug that caused OIDs to be reused after importing transactions.
  Added a corresponding test.

- Made it possible to disable garbage collection during packing.
  Exposed the option in zope.conf.

- Valery Suhomlinov discovered a problem with non-ASCII data in transaction
  metadata.  The problem has been fixed for all supported databases.

=================
PGStorage history
=================


0.4
===

- Began using the PostgreSQL LISTEN and NOTIFY statements as a shortcut
  for invalidation polling.

- Removed the commit_order code.  The commit_order idea was intended to
  allow concurrent commits, but that idea is a little too ambitious while
  other more important ideas are being tested.  Something like it may
  come later.

- Improved connection management: only one database connection is
  held continuously open per storage instance.

- Reconnect to the database automatically.

- Removed test mode.

- Switched from using a ZODB.Connection subclass to a ZODB patch.  The
  Connection class changes in subtle ways too often to subclass reliably;
  a patch is much safer.

- PostgreSQL 8.1 is now a dependency because PGStorage uses two phase commit.

- Fixed an undo bug.  Symptom: attempting to examine the undo log revealed
  broken pickles.  Cause: the extension field was not being wrapped in
  psycopg2.Binary upon insert.  Solution: used psycopg2.Binary.
  Unfortunately, this doesn't fix existing transactions people have
  committed.  If anyone has any data to keep, fixing the old transactions
  should be easy.

- Moved from a private CVS repository to Sourceforge.
  See http://pgstorage.sourceforge.net .  Also switched to the MIT license.

- David Pratt added a basic getSize() implementation so that the Zope
  management interface displays an estimate of the size of the database.

- Turned PGStorage into a top-level package.  Python generally makes
  top-level packages easier to install.


0.3
===

- Made compatible with Zope 3, although an undo bug apparently remains.


0.2
===

- Fixed concurrent commits, which were generating deadlocks.  Fixed by
  adding a special table, "commit_lock", which is used for
  synchronizing increments of commit_seq (but only at final commit.)
  If you are upgrading from version 0.1, you need to change your
  database using the 'psql' prompt:

    create table commit_lock ();

- Added speed tests and an OpenDocument spreadsheet comparing
  FileStorage / ZEO with PGStorage.  PGStorage wins at reading objects
  and writing a lot of small transactions, while FileStorage / ZEO
  wins at writing big transactions.  Interestingly, they tie when
  writing a RAM disk.
