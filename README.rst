
RelStorage is a storage implementation for ZODB that stores pickles in
a relational database. PostgreSQL 9.6 and above (but not 12), MySQL 5.7.19 / 8.0, and
Oracle 10g and 11g are currently supported. RelStorage replaced the
PGStorage project.


==========
 Features
==========

* It is a drop-in replacement for FileStorage and ZEO.
* There is a simple way to convert FileStorage to RelStorage and back again.
  You can also convert a RelStorage instance to a different relational database.
* Designed for high volume sites: multiple ZODB instances can share the same
  database. This is similar to ZEO, but RelStorage does not require ZEO.
* According to some tests, RelStorage handles high concurrency better than
  the standard combination of ZEO and FileStorage.
* Whereas FileStorage takes longer to start as the database grows due to an
  in-memory index of all objects, RelStorage starts quickly regardless of
  database size.
* Supports undo, packing, and filesystem-based ZODB blobs.
* Both history-preserving and history-free storage are available.
* Capable of failover to replicated SQL databases.
* ``zodbconvert`` utility to copy databases.
* Free, open source (ZPL 2.1)


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
----------------------

A test suite is run for every push and pull request submitted. Travis
CI is used to test on Linux, and AppVeyor runs the builds on
Windows.

.. image:: https://travis-ci.org/zodb/relstorage.svg?branch=master
    :target: https://travis-ci.org/zodb/relstorage

.. image:: https://ci.appveyor.com/api/projects/status/pccddlgujdoqvl83?svg=true
   :target: https://ci.appveyor.com/project/jamadden/relstorage/branch/master

Builds on Travis CI automatically submit updates to `coveralls.io`_ to
monitor test coverage.

.. image:: https://coveralls.io/repos/zodb/relstorage/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/zodb/relstorage?branch=master

.. _coveralls.io: https://coveralls.io/github/zodb/relstorage
