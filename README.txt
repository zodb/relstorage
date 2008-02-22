

Overview
========

  RelStorage is a storage implementation for ZODB that stores pickles in a
relational database.  PostgreSQL 8.1 and above (via psycopg2), MySQL 5.0 and
above (via MySQLdb), and Oracle 10g (via cx_Oracle) are currently supported.

  RelStorage replaces the PGStorage project.

  See:

     http://wiki.zope.org/ZODB/RelStorage         (wiki)
     http://shane.willowrise.com/                 (blog)
     http://www.zope.org/Members/shane/RelStorage (downloads)
     http://pypi.python.org/pypi/RelStorage       (PyPI entry)


Highlights
==========

  * It is a drop-in replacement for FileStorage and ZEO.
  * Designed for high volume sites: Any number of load-balanced Zope instances
    can share the same database. This is similar to ZEO, but RelStorage does
    not require ZEO.
  * According to some tests, RelStorage handles high concurrency better than
    the standard combination of ZEO and FileStorage.
  * Supports undo and packing.
  * Open source (ZPL 2.1)


Installation in Zope
====================

  Get the latest release here:

    http://www.zope.org/Members/shane/RelStorage

  Before you can use relstorage, ZODB must have the invalidation polling patch
applied.  Two versions of the patch are included in the downloadable package:
one for ZODB 3.7.1 (which is part of Zope 2.10.5) and one for ZODB 3.8.0 (which
is part of Zope 2.11).  The patch has no effect on ZODB except when using
RelStorage.  Hopefully, a future release of ZODB will include the feature.

  Place the relstorage package in the lib/python directory of either the
SOFTWARE_HOME or the INSTANCE_HOME.  You can do this with the following
command::

    python2.4 setup.py install --install-lib=${INSTANCE_HOME}/lib/python

  You need the Python database adapter that corresponds with your database. 
Install psycopg2, MySQLdb 1.2.2+, or cx_Oracle 4.3+.  Note that Debian Etch
ships MySQLdb 1.2.1, but that version has a bug in BLOB handling that manifests
itself only with certain character set configurations.  MySQLdb 1.2.2 fixes the
bug.

  Finally, modify etc/zope.conf of your Zope instance.  Remove the main mount
point and add one of the following blocks.  For PostgreSQL::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <postgresql>
          # The dsn is optional, as are each of the parameters in the dsn.
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>
    </zodb_db>

  For MySQL::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <mysql>
          # Most of the options provided by MySQLdb are available.
          # See component.xml.
          db zodb
        </mysql>
      </relstorage>
    </zodb_db>

  For Oracle (10g XE in this example)::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <oracle>
          user username
          password pass
          dsn XE
        </oracle>
     </relstorage>
    </zodb_db>


Migrating from FileStorage
==========================

  It is fairly easy for any Python coder to migrate a FileStorage instance to
RelStorage while retaining all transactions and object history.  Use a script
similar to the following. Note that it first blindly deletes all data from the
destination database.  **Make backups** and proceed with caution! ::

    source_db = '/zope/var/Data.fs'

    from ZODB import DB
    from ZODB.FileStorage import FileStorage
    from relstorage.relstorage import RelStorage
    from relstorage.adapters.mysql import MySQLAdapter

    src = FileStorage(source_db, read_only=True)
    adapter = MySQLAdapter(db='zodb')
    dst = RelStorage(adapter)

    # remove all objects and history from the destination database
    dst.zap_all()
    # copy all transactions from the source database
    dst.copyTransactionsFrom(src)

    src.close()
    dst.close()


Optional Features
=================

  poll-interval

    This option is useful if you need to reduce database traffic.  If set,
    RelStorage will poll the database for changes less often.  A setting of
    1 to 5 seconds should be sufficient for most systems.  Fractional seconds
    are allowed.

    While this setting should not affect database integrity, it increases the
    probability of basing transactions on stale data, leading to conflicts.
    Thus a nonzero setting can hurt the performance of servers with high write
    volume.

    To enable this feature, add a line similar "poll-interval 2" inside a
    <relstorage> section of zope.conf.

  pack-gc

    If pack-gc is false, pack operations do not perform garbage collection. 
    Garbage collection is enabled by default.

    If garbage collection is disabled, pack operations keep at least one
    revision of every object.  With garbage collection disabled, the pack
    code does not need to follow object references, making packing conceivably
    much faster.  However, some of that benefit may be lost due to an ever
    increasing number of unused objects.

    Disabling garbage collection is also a hack that ensures inter-database
    references never break.

    To disable garbage collection, add the line "pack-gc no" inside a
    <relstorage> section of zope.conf.


Development
===========

  You can checked out from Subversion using the following command::

    svn co svn://svn.zope.org/repos/main/relstorage/trunk RelStorage

  You can also browse the code:

    http://svn.zope.org/relstorage/trunk/


Roadmap
=======

  * RelStorage currently passes all ZODB tests with all three supported
    databases.
  * A release is planned for the end of February 2008.
  * The current focus is on making RelStorage easier to install and configure.
  * Ask questions about RelStorage here on the wiki or on the zodb-dev mailing
    list.


Probable FAQs
=============

  Q: How can I help?

    A: The best way to help is to test and to provide database-specific
expertise.

  Q: Can I perform SQL queries on the data in the database?

    A: No.  Like FileStorage and DirectoryStorage, RelStorage stores the data
as pickles, making it hard for anything but ZODB to interpret the data.  An
earlier project called Ape attempted to store data in a truly relational way,
but it turned out that Ape worked too much against ZODB principles and
therefore could not be made reliable enough for production use.  RelStorage, on
the other hand, is much closer to an ordinary ZODB storage, and is therefore
much safer for production use.

  Q: How does RelStorage performance compare with FileStorage?

    A: According to benchmarks, RelStorage with PostgreSQL is often faster than
FileStorage, especially under high concurrency.

  Q: Why should I choose RelStorage?

    A: Because RelStorage is a fairly small layer that builds on world-class
databases.  These databases have proven reliability and scalability, along with
numerous support options.

  Q: Can RelStorage replace ZRS (Zope Replication Services)?

    A: In theory, yes.  With RelStorage, you can use the replication features
native to your database.  However, this capability has not yet been tested.

