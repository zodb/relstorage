=====================
 What is RelStorage?
=====================

RelStorage is a storage implementation for ZODB that stores pickles in
a relational database. PostgreSQL 9.0 and above, MySQL 5.0.32+ /
5.1.34+, and Oracle 10g and 11g are currently supported. RelStorage
replaced the PGStorage project.



Features
========

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
* :doc:`zodbconvert <zodbconvert>` utility to copy/transform
  databases.
* :doc:`zodbpack <zodbpack>` utility to pack databases.
* :doc:`zodburi <zodburi>` support.
* Free, open source (ZPL 2.1)


See :doc:`the rest of the documentation <contents>` for more information.


.. toctree::
   :maxdepth: 2

   contents


Development
===========

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

.. image:: https://readthedocs.org/projects/relstorage/badge/?version=latest
   :target: http://relstorage.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

Builds on Travis CI automatically submit updates to `coveralls.io`_ to
monitor test coverage.

.. image:: https://coveralls.io/repos/zodb/relstorage/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/zodb/relstorage?branch=master

Likewise, builds on Travis CI will automatically submit updates to
`landscape.io`_ to monitor code health (adherence to PEP8, absence of
common code smells, etc).

.. image:: https://landscape.io/github/zodb/relstorage/master/landscape.svg?style=flat
   :target: https://landscape.io/github/zodb/relstorage/master
   :alt: Code Health

.. _coveralls.io: https://coveralls.io/github/zodb/relstorage
.. _landscape.io: https://landscape.io/github/zodb/relstorage


Project URLs
============

* http://pypi.python.org/pypi/RelStorage       (PyPI entry and downloads)
* http://shane.willowrise.com/                 (blog)

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
