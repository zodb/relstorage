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


Development
===========

RelStorage is hosted at GitHub:

    https://github.com/zodb/relstorage



Project URLs
============

* http://pypi.python.org/pypi/RelStorage       (PyPI entry and downloads)
* http://shane.willowrise.com/                 (blog)

.. toctree::
   contents
