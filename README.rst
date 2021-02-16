
..
  This file is the long-description for PyPI so it can only use plain
  ReST, no sphinx extensions.

RelStorage is a storage implementation for ZODB that stores pickles in
a relational database (`RDBMS`_). PostgreSQL 9.6 and above, MySQL
5.7.19 / 8.0, Oracle 10g and above, and SQLite 3.8.3 and above are
currently supported. RelStorage replaced the PGStorage project.

.. _RDBMS: https://en.wikipedia.org/wiki/Relational_database_

==========
 Features
==========

* It is a drop-in replacement for FileStorage and ZEO, with several
  enhancements:

  * Supports undo, packing, and object history preservation just like
    FileStorage.
  * RelStorage can be configured *not* to keep object histories for
    reduced disk space usage and improved performance.
  * Multiple processes on a single machine can read and write a local
    ZODB database using SQLite without needing to start and manage
    another process (i.e., ZEO).
  * Blobs can be stored on a shared filesystem, or (recommended) in
    the relational database and only cached locally.
  * Multiple threads in the same process share a high-performance
    in-memory pickle cache to reduce the number of queries to the
    RDBMS. This is similar to ZEO, and the ZEO cache trace tools are
    supported.
  * The in-memory pickle cache can be saved to disk and read when a
    process starts up. This can dramatically speed up site warmup time
    by eliminating a flood of RDBMS queries. Unlike ZEO, this cache
    is automatically shared by all processes on the machine (no need
    to configure separate client identifiers.)

* Ideal for large, high volume sites.

  * Multiple Python processes on multiple machines can read and write
    the same ZODB database concurrently. This is similar to ZEO, but
    RelStorage does not require ZEO.
  * Supports ZODB 5's parallel commit feature: Database writers only
    block each other when they would conflict (except for a small
    window at the end of the twophase commit protocol when the
    transaction ID is allocated; that still requires a global database
    lock).
  * According to some tests, RelStorage handles concurrency better than
    the standard combination of ZEO and FileStorage.
  * Whereas FileStorage takes longer to start as the database grows
    due to an in-memory index of all objects, RelStorage starts
    quickly regardless of database size.
  * Capable of failover to replicated SQL databases.
* Tested integration with `gevent`_ for PostgreSQL, MySQL, and SQLite.
* There is a simple way (`zodbconvert`_) to (incrementally) convert
  FileStorage to RelStorage and back again. You can also convert a
  RelStorage instance to a different relational database. This is a
  general tool that can be used to convert between any two ZODB
  storage implementations.
* There is a simple way (`zodbpack`_) to pack databases.
* Supports `zodburi`_ .
* Free, open source (ZPL 2.1)

.. _gevent: http://gevent.org
.. _zodbconvert: https://relstorage.readthedocs.io/en/latest/zodbconvert.html
.. _zodbpack: https://relstorage.readthedocs.io/en/latest/zodbpack.html
.. _zodburi: https://relstorage.readthedocs.io/en/latest/zodburi.html

Features Supported by Databases
===============================

Some of RelStorage's features are only supported on certain versions
of certain databases. If the database doesn't support the feature,
RelStorage will still work, but possibly with a performance penalty.


.. list-table:: Supported Features
   :widths: auto
   :header-rows: 1
   :stub-columns: 1

   * -
     - Parallel Commit
     - Shared readCurrent locks
     - Non-blocking readCurrent locks
     - Streaming blobs
     - Central transaction ID allocation
     - Atomic lock and commit without Python involvement
   * - PostgreSQL
     - Yes
     - Yes
     - Yes
     - With psycopg2 driver
     - Yes
     - Yes, except with PG8000 driver
   * - MySQL
     - Yes
     - Yes
     - Native on MySQL 8.0, emulated on MySQL 5.7
     - No, emulated via chunking
     - Yes
     - Yes
   * - Oracle
     - Yes
     - No
     - Yes
     - Yes
     - No (could probably be implemented)
     - No (could probably be implemented)
   * - SQLite
     - No
     - No
     - N/A (there is no distinction in lock types)
     - No, consider using a shared-blob-dir
     - N/A (essentially yes because it happens on one machine)
     - No


===============
 Documentation
===============

Documentation including `installation instructions`_ is hosted on `readthedocs`_.

The complete `changelog`_ is also there.

.. image:: https://readthedocs.org/projects/relstorage/badge/?version=latest
     :target: http://relstorage.readthedocs.io/en/latest/?badge=latest


.. _`installation instructions`: http://relstorage.readthedocs.io/en/latest/install.html
.. _`readthedocs`: http://relstorage.readthedocs.io/en/latest/
.. _`changelog`: http://relstorage.readthedocs.io/en/latest/changelog.html


=============
 Development
=============

RelStorage is hosted at GitHub:

    https://github.com/zodb/relstorage

Continuous integration
======================

A test suite is run for every push and pull request submitted. GitHub
Actions is used to test on Linux and macOS, and AppVeyor runs the builds on
Windows.

.. image:: https://github.com/zodb/relstorage/workflows/tests/badge.svg
    :target: https://github.com/zodb/relstorage/actions

.. image:: https://ci.appveyor.com/api/projects/status/pccddlgujdoqvl83?svg=true
   :target: https://ci.appveyor.com/project/jamadden/relstorage/branch/master

Builds on CI automatically submit updates to `coveralls.io`_ to
monitor test coverage.

.. image:: https://coveralls.io/repos/zodb/relstorage/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/zodb/relstorage?branch=master

.. _coveralls.io: https://coveralls.io/github/zodb/relstorage
