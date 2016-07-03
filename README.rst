
RelStorage is a storage implementation for ZODB that stores pickles in
a relational database. PostgreSQL 9.0 and above, MySQL 5.0.32+ /
5.1.34+, and Oracle 10g and 11g are currently supported. RelStorage
replaced the PGStorage project.


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

`Documentation`_ including `installation instructions`_ is hosted on `readthedocs`_.

The complete `changelog`_ is also there.

.. _`Documentation`: http://relstorage.readthedocs.io/en/latest/
.. _`installation instructions`: http://relstorage.readthedocs.io/en/latest/install.html
.. _`readthedocs`: http://relstorage.readthedocs.io/en/latest/
.. _`changelog`: http://relstorage.readthedocs.io/en/latest/changelog.html


=============
 Development
=============

RelStorage is hosted at GitHub:

    https://github.com/zodb/relstorage
