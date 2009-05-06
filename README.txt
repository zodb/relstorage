
RelStorage is a storage implementation for ZODB that stores pickles in
a relational database. PostgreSQL 8.1 and above (via psycopg2), MySQL
5.0.32+ / 5.1.34+ (via MySQLdb 1.2.2 and above), and Oracle 10g (via
cx_Oracle) are currently supported. RelStorage replaces the PGStorage
project.

.. contents::


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
* Supports undo and packing.
* Free, open source (ZPL 2.1)


Installation
============

You can install RelStorage using easy_install::

    easy_install RelStorage

If you are not using easy_install (part of the setuptools package), you can
get the latest release at PyPI (http://pypi.python.org/pypi/RelStorage), then
place the relstorage package in the lib/python directory of either the
SOFTWARE_HOME or the INSTANCE_HOME.  You can do this with the following
command::

    python2.4 setup.py install --install-lib=${INSTANCE_HOME}/lib/python

RelStorage requires a version of ZODB with the invalidation polling patch
applied.  You can get versions of ZODB with the patch already applied here:

    http://packages.willowrise.org

The patches are also included in the source distribution of RelStorage.

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


Migration
=========

Migrating from FileStorage
--------------------------

You can convert a FileStorage instance to RelStorage and back using a utility
called ZODBConvert.  See http://wiki.zope.org/ZODB/ZODBConvert .


Migrating from PGStorage
------------------------

The following script migrates your database from PGStorage to RelStorage 1.0
beta:

    migrate.sql_

    .. _migrate.sql: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate.sql

After you do this, you still need to migrate from 1.0 beta to the latest
release.


Migrating to a new version of RelStorage
----------------------------------------

Sometimes RelStorage needs a schema modification along with a software
upgrade.  Hopefully, this will not often be necessary.

Version 1.2.0b1 does not require a schema migration from version 1.1.3.

To migrate from version 1.1.1 to version 1.1.2 or 1.1.3, see:

  migrate-to-1.1.2.txt_

  .. _migrate-to-1.1.2.txt: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-to-1.1.2.txt

To migrate from version 1.1 to version 1.1.1, see:

  migrate-to-1.1.1.txt_

  .. _migrate-to-1.1.1.txt: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-to-1.1.1.txt

To migrate from version 1.0.1 to version 1.1, see:

  migrate-to-1.1.txt_

  .. _migrate-to-1.1.txt: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-to-1.1.txt

To migrate from version 1.0 beta to version 1.0c1 through 1.0.1, see:

  migrate-to-1.0.txt_

  .. _migrate-to-1.0.txt: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-to-1.0.txt


Optional Features
=================

Specify these options in zope.conf, as parameters for the RelStorage
constructor, or as attributes of a relstorage.Options instance.
In the latter two cases, use underscores instead of dashes in the
parameter names.

poll-interval

        Defer polling the database for the specified maximum time interval,
        in seconds.  Set to 0 (the default) to always poll.  Fractional
        seconds are allowed.  Use this to lighten the database load on
        servers with high read volume and low write volume.

        The poll-interval option works best in conjunction with
        the cache-servers option.  If both are enabled, RelStorage will
        poll a single cache key for changes on every request.
        The database will not be polled unless the cache indicates
        there have been changes, or the timeout specified by poll-interval
        has expired.  This configuration keeps clients fully up to date,
        while removing much of the polling burden from the database.
        A good cluster configuration is to use memcache servers
        and a high poll-interval (say, 60 seconds).

        This option can be used without the cache-servers option,
        but a large poll-interval without cache-servers increases the
        probability of basing transactions on stale data, which does not
        affect database consistency, but does increase the probability
        of conflict errors, leading to low performance.

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

pack-dry-run

        If pack-dry-run is true, pack operations perform a full analysis
        of what to pack, but no data is actually removed.  After a dry run,
        the pack_object, pack_state, and pack_state_tid tables are filled
        with the list of object states and objects that would have been
        removed.

pack-batch-timeout

        Packing occurs in batches of transactions; this specifies the
        timeout in seconds for each batch.  Note that some database
        configurations have unpredictable I/O performance
        and might stall much longer than the timeout.
        The default timeout is 5.0 seconds.

pack-duty-cycle

        After each batch, the pack code pauses for a time to
        allow concurrent transactions to commit.  The pack-duty-cycle
        specifies what fraction of time should be spent on packing.
        For example, if the duty cycle is 0.75, then 75% of the time
        will be spent packing: a 6 second pack batch
        will be followed by a 2 second delay.  The duty cycle should
        be greater than 0.0 and less than or equal to 1.0.  Specify
        1.0 for no delay between batches.

        The default is 0.5.  Raise it to finish packing faster; lower it
        to reduce the effect of packing on transaction commit performance.

pack-max-delay

        This specifies a maximum delay between pack batches.  Sometimes
        the database takes an extra long time to finish a pack batch; at
        those times it is useful to cap the delay imposed by the
        pack-duty-cycle.  The default is 20 seconds.

cache-servers

        Specifies a list of memcache servers.  Enabling memcache integration
        is useful if the connection to the relational database has high
        latency and the connection to memcache has significantly lower
        latency.  On the other hand, if the connection to the relational
        database already has low latency, memcache integration may actually
        hurt overall performance.

        Provide a list of host:port pairs, separated by whitespace.
        "127.0.0.1:11211" is a common setting.  The default is to disable
        memcache integration.

cache-module-name

        Specifies which Python memcache module to use.  The default is
        "memcache", a pure Python module.  There are several alternative
        modules available through PyPI.  This setting has no effect unless
        cache-servers is set.

Development
===========

You can checkout from Subversion using the following command::

    svn co svn://svn.zope.org/repos/main/relstorage/trunk RelStorage

You can also browse the code:

    http://svn.zope.org/relstorage/trunk/

The best place to discuss development of RelStorage is on the zodb-dev
mailing list.



FAQs
====

Q: How can I help improve RelStorage?

    A: The best way to help is to test and to provide database-specific
    expertise.  Ask questions about RelStorage on the zodb-dev mailing list.

Q: Can I perform SQL queries on the data in the database?

    A: No.  Like FileStorage and DirectoryStorage, RelStorage stores the data
    as pickles, making it hard for anything but ZODB to interpret the data.  An
    earlier project called Ape attempted to store data in a truly relational
    way, but it turned out that Ape worked too much against ZODB principles and
    therefore could not be made reliable enough for production use.  RelStorage,
    on the other hand, is much closer to an ordinary ZODB storage, and is
    therefore much safer for production use.

Q: How does RelStorage performance compare with FileStorage?

    A: According to benchmarks, RelStorage with PostgreSQL is often faster than
    FileStorage, especially under high concurrency.

Q: Why should I choose RelStorage?

    A: Because RelStorage is a fairly small layer that builds on world-class
    databases.  These databases have proven reliability and scalability, along
    with numerous support options.

Q: Can RelStorage replace ZRS (Zope Replication Services)?

    A: In theory, yes.  With RelStorage, you can use the replication features
    native to your database.  However, this capability has not yet been tested.


Project URLs
============

* http://wiki.zope.org/ZODB/RelStorage         (wiki)
* http://shane.willowrise.com/                 (blog)
* http://pypi.python.org/pypi/RelStorage       (PyPI entry and downloads)
