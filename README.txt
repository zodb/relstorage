
RelStorage is a storage implementation for ZODB that stores pickles in
a relational database. PostgreSQL 8.1 and above (via psycopg2), MySQL
5.0.32+ / 5.1.34+ (via MySQLdb 1.2.2 and above), and Oracle 10g and 11g
(via cx_Oracle) are currently supported. RelStorage replaced the
PGStorage project.

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
* Supports undo, packing, and filesystem-based ZODB blobs.
* Both history-preserving and history-free storage are available.
* Capable of failover to replicated SQL databases.
* Free, open source (ZPL 2.1)


Installation
============

You can install RelStorage using easy_install::

    easy_install RelStorage

RelStorage requires a version of ZODB that is aware of MVCC storages.
ZODB 3.9 supports RelStorage without any patches. ZODB 3.7 and 3.8 can
support RelStorage if you first apply a patch to ZODB. You can get
versions of ZODB with the patch already applied here:

    http://packages.willowrise.org

The patches are also included in the source distribution of RelStorage.

You need the Python database adapter that corresponds with your database.
Install psycopg2, MySQLdb 1.2.2+, or cx_Oracle 4.3+.

Configuring Your Database
-------------------------

You need to configure a database and user account for RelStorage.
RelStorage will populate the database with its schema the first time it
connects.

PostgreSQL
~~~~~~~~~~

If you installed PostgreSQL from a binary package, you probably have a
user account named ``postgres``. Since PostgreSQL respects the name of
the logged-in user by default, switch to the ``postgres`` account to
create the RelStorage user and database. Even ``root`` does not have
the PostgreSQL privileges that the ``postgres`` account has. For
example::

    $ sudo su - postgres
    $ createuser --pwprompt zodbuser
    $ createdb -O zodbuser zodb

New PostgreSQL accounts often require modifications to ``pg_hba.conf``,
which contains host-based access control rules. The location of
``pg_hba.conf`` varies, but ``/etc/postgresql/8.4/main/pg_hba.conf`` is
common. PostgreSQL processes the rules in order, so add new rules
before the default rules rather than after. Here is a sample rule that
allows only local connections by ``zodbuser`` to the ``zodb``
database::

    local  zodb  zodbuser  md5

PostgreSQL re-reads ``pg_hba.conf`` when you ask it to reload its
configuration file::

    /etc/init.d/postgresql reload

MySQL
~~~~~

Use the ``mysql`` utility to create the database and user account. Note
that the ``-p`` option is usually required. You must use the ``-p``
option if the account you are accessing requires a password, but you
should not use the ``-p`` option if the account you are accessing does
not require a password. If you do not provide the ``-p`` option, yet
the account requires a password, the ``mysql`` utility will not prompt
for a password and will fail to authenticate.

Most users can start the ``mysql`` utility with the following shell
command, using any login account::

    $ mysql -u root -p

Here are some sample SQL statements for creating the user and database::

    CREATE USER 'zodbuser'@'localhost' IDENTIFIED BY 'mypassword';
    CREATE DATABASE zodb;
    GRANT ALL ON zodb.* TO 'zodbuser'@'localhost';
    FLUSH PRIVILEGES;

Oracle
~~~~~~

Initial setup will require ``SYS`` privileges. Using Oracle 10g XE, you
can start a ``SYS`` session with the following shell commands::

    $ su - oracle
    $ sqlplus / as sysdba

You need to create a database user and grant execute privileges on
the DBMS_LOCK package to that user.
Here are some sample SQL statements for creating the database user
and granting the required permissions::

    CREATE USER zodb IDENTIFIED BY mypassword;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO zodb;
    GRANT EXECUTE ON DBMS_LOCK TO zodb;

Configuring Plone
-----------------

To install RelStorage in Plone, see the instructions in the following
article:

    http://shane.willowrise.com/archives/how-to-install-plone-with-relstorage-and-mysql/

Plone uses the ``plone.recipe.zope2instance`` Buildout recipe to
generate zope.conf, so the easiest way to configure RelStorage in a
Plone site is to set the ``rel-storage`` parameter in ``buildout.cfg``.
The ``rel-storage`` parameter contains options separated by newlines,
with these values:

    * ``type``: any database type supported (``postgresql``, ``mysql``,
      or ``oracle``)
    * RelStorage options like ``cache-servers`` and ``poll-interval``
    * Adapter-specific options

An example::

    rel-storage =
        type mysql
        db plone
        user plone
        passwd PASSWORD

Configuring Zope 2
------------------

To integrate RelStorage in Zope 2, specify a RelStorage backend in
``etc/zope.conf``. Remove the main mount point and add one of the
following blocks. For PostgreSQL::

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

To add ZODB blob support, provide a blob-dir option that specifies
where to store the blobs.  For example::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        blob-dir ./blobs
        <postgresql>
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>
    </zodb_db>

Configuring ``repoze.zodbconn``
-------------------------------

To use RelStorage with ``repoze.zodbconn``, a package that makes ZODB
available to WSGI applications, create a configuration file with
contents similar to the following::

    %import relstorage
    <zodb main>
      <relstorage>
        <mysql>
          db zodb
        </mysql>
      </relstorage>
      cache-size 100000
    </zodb>

``repoze.zodbconn`` expects a ZODB URI.  Use a URI of the form
``zconfig://path/to/configuration#main``.


Included Utilities
==================

``zodbconvert``
---------------

RelStorage comes with a script named ``zodbconvert`` that converts
databases between formats. Use it to convert a FileStorage instance to
RelStorage and back, or to convert between different kinds of
RelStorage instances, or to convert other kinds of storages that
support the storage iterator protocol.

When converting between two history-preserving databases (note that
FileStorage uses a history-preserving format), ``zodbconvert``
preserves all objects and transactions, meaning you can still use the
ZODB undo feature after the conversion, and you can convert back using
the same process in reverse. When converting from a history-free
database to either a history-free database or a history-preserving
database, ``zodbconvert`` retains all data, but the converted
transactions will not be undoable. When converting from a
history-preserving storage to a history-free storage, ``zodbconvert``
drops all historical information during the conversion.

How to use ``zodbconvert``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ZConfig style configuration file that specifies two storages,
one named "source", the other "destination". The configuration file
format is very much like zope.conf. Then run ``zodbconvert``, providing
the name of the configuration file as a parameter.

The utility does not modify the source storage. Before copying the
data, the utility verifies the destination storage is completely empty.
If the destination storage is not empty, the utility aborts without
making any changes to the destination. (Adding transactions to an
existing database is complex and out of scope for ``zodbconvert``.)

Here is a sample ``zodbconvert`` configuration file::

  <filestorage source>
    path /zope/var/Data.fs
  </filestorage>

  <relstorage destination>
    <mysql>
      db zodb
    </mysql>
  </relstorage>

This configuration file specifies that the utility should copy all of
the transactions from Data.fs to a MySQL database called "zodb". If you
want to reverse the conversion, exchange the names "source" and
"destination". All storage types and storage options available in
zope.conf are also available in this configuration file.

Options for ``zodbconvert``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

  ``--clear``
    Clears all data from the destination storage before copying. Use
    this only if you are certain the destination has no useful data.
    Currently only works when the destination is a RelStorage instance.

  ``--dry-run``
    Opens both storages and analyzes what would be copied, but does not
    actually copy.


``zodbpack``
------------

RelStorage also comes with a script named ``zodbpack`` that packs any
ZODB storage that allows concurrent connections (including RelStorage
and ZEO, but not including FileStorage). Use ``zodbpack`` in ``cron``
scripts. Pass the script the name of a configuration file that lists
the storages to pack, in ZConfig format. An example configuration file::

  <relstorage>
    pack-gc true
    <mysql>
      db zodb
    </mysql>
  </relstorage>

Options for ``zodbpack``
~~~~~~~~~~~~~~~~~~~~~~~~

  ``--days`` or ``-d``
    Specifies how many days of historical data to keep. Defaults to 0,
    meaning no history is kept. This is meaningful even for
    history-free storages, since unreferenced objects are not removed
    from the database until the specified number of days have passed.

  ``--prepack``
    Instructs the storage to only run the pre-pack phase of the pack but not
    actually delete anything.  This is equivalent to specifying
    ``pack-prepack-only true`` in the storage options.

  ``--use-prepack-state``
    Instructs the storage to only run the deletion (packing) phase, skipping
    the pre-pack analysis phase. This is equivalento to specifying
    ``pack-skip-prepack true`` in the storage options.


Migrating to a new version of RelStorage
========================================

Sometimes RelStorage needs a schema modification along with a software
upgrade.  Hopefully, this will not often be necessary.

Migration to RelStorage version 1.5 requires a schema upgrade.
See `migrate-to-1.5.txt`_.

.. _`migrate-to-1.5.txt`: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-to-1.5.txt

Migration to RelStorage version 1.4.2 requires a schema upgrade if
you are using a history-free database (meaning keep-history is false).
See `migrate-to-1.4.txt`_.

.. _`migrate-to-1.4.txt`: http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-to-1.4.txt

See the `notes subdirectory`_ if you are upgrading from an older version.

.. _`notes subdirectory`: http://svn.zope.org/relstorage/trunk/notes/


RelStorage Options
==================

Specify these options in zope.conf, as parameters for the
``relstorage.storage.RelStorage`` constructor, or as attributes of a
``relstorage.options.Options`` instance. In the latter two cases, use
underscores instead of dashes in the option names.

``name``
        The name of the storage. Defaults to a descriptive name that
        includes the adapter connection parameters, except the database
        password.

``read-only``
        If true, only reads may be executed against the storage.

``blob-dir``
        If supplied, the storage will provide ZODB blob support; this
        option specifies the name of the directory to hold blob data.
        The directory will be created if it does not exist. If no value
        (or an empty value) is provided, then no blob support will be
        provided.

``shared-blob-dir``
        If true (the default), the blob directory is assumed to be
        shared among all clients using NFS or similar; blob data will
        be stored only on the filesystem and not in the database. If
        false, blob data is stored in the relational database and the
        blob directory holds a cache of blobs. When this option is
        false, the blob directory should not be shared among clients.

        This option must be true when using ZODB 3.8, because ZODB 3.8
        is not compatible with the file layout required for a blob
        cache.  Use ZODB 3.9 or later if you want to store blobs in
        the relational database.

``blob-cache-size``
        Maximum size of the blob cache, in bytes. If empty (the
        default), the cache size isn't checked and the blob directory
        will grow without bounds. This should be either empty or
        significantly larger than the largest blob you store. At least
        1 gigabyte is recommended for typical databases. More is
        recommended if you store large files such as videos, CD/DVD
        images, or virtual machine images.

        This option allows suffixes such as "mb" or "gb".
        This option is ignored if shared-blob-dir is true.

``blob-cache-size-check``
        Blob cache check size as percent of blob-cache-size: "10" means
        "10%". The blob cache size will be checked when this many bytes
        have been loaded into the cache. Defaults to 10% of the blob
        cache size.

        This option is ignored if shared-blob-dir is true.

``blob-chunk-size``
        When ZODB blobs are stored in MySQL, RelStorage breaks them into
        chunks to minimize the impact on RAM.  This option specifies the chunk
        size for new blobs. On PostgreSQL and Oracle, this value is used as
        the memory buffer size for blob upload and download operations. The
        default is 1048576 (1 mebibyte).

        This option allows suffixes such as "mb" or "gb".
        This option is ignored if shared-blob-dir is true.

``keep-history``
        If this option is set to true (the default), the adapter
        will create and use a history-preserving database schema
        (like FileStorage). A history-preserving schema supports
        ZODB-level undo, but also grows more quickly and requires extensive
        packing on a regular basis.

        If this option is set to false, the adapter will create and
        use a history-free database schema. Undo will not be supported,
        but the database will not grow as quickly. The database will
        still require regular garbage collection (which is accessible
        through the database pack mechanism.)

        This option must not change once the database schema has
        been installed, because the schemas for history-preserving and
        history-free storage are different. If you want to convert
        between a history-preserving and a history-free database, use
        the ``zodbconvert`` utility to copy to a new database.

``replica-conf``
        If this option is provided, it specifies a text file that
        contains a list of database replicas the adapter can choose
        from. For MySQL and PostgreSQL, put in the replica file a list
        of ``host:port`` or ``host`` values, one per line. For Oracle,
        put in a list of DSN values. Blank lines and lines starting
        with ``#`` are ignored.

        The adapter prefers the first replica specified in the file. If
        the first is not available, the adapter automatically tries the
        rest of the replicas, in order. If the file changes, the
        adapter will drop existing SQL database connections and make
        new connections when ZODB starts a new transaction.

``ro-replica-conf``
        Like the ``replica-conf`` option, but the referenced text file
        provides a list of database replicas to use only for read-only
        load connections. This allows RelStorage to load objects from
        read-only database replicas, while using read-write replicas
        for all other database interactions.

        If this option is not provided, load connections will fall back
        to the replica pool specified by ``replica-conf``. If
        ``ro-replica-conf`` is provided but ``replica-conf`` is not,
        RelStorage will use replicas for load connections but not for
        other database interactions.

        Note that if read-only replicas are asynchronous, the next
        interaction after a write operation might not be up to date.
        When that happens, RelStorage will log a "backward time travel"
        warning and clear the ZODB cache to prevent consistency errors.
        This will likely result in temporarily reduced performance as
        the ZODB cache is repopulated.

``replica-timeout``
        If this option has a nonzero value, when the adapter selects
        a replica other than the primary replica, the adapter will
        try to revert to the primary replica after the specified
        timeout (in seconds).  The default is 600, meaning 10 minutes.

``revert-when-stale``
        Specifies what to do when a database connection is stale.
        This is especially applicable to asynchronously replicated
        databases: RelStorage could switch to a replica that is not
        yet up to date.

        When ``revert-when-stale`` is ``false`` (the default) and the
        database connection is stale, RelStorage will raise a
        ReadConflictError if the application tries to read or write
        anything. The application should react to the
        ReadConflictError by retrying the transaction after a delay
        (possibly multiple times.) Once the database catches
        up, a subsequent transaction will see the update and the
        ReadConflictError will not occur again.

        When ``revert-when-stale`` is ``true`` and the database connection
        is stale, RelStorage will log a warning, clear the affected
        ZODB connection cache (to prevent consistency errors), and let
        the application continue with database state from
        an earlier transaction. This behavior is intended to be useful
        for highly available, read-only ZODB clients. Enabling this
        option on ZODB clients that read and write the database is
        likely to cause confusion for users whose changes
        seem to be temporarily reverted.

``poll-interval``
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

``pack-gc``
        If pack-gc is false, pack operations do not perform
        garbage collection.  Garbage collection is enabled by default.

        If garbage collection is disabled, pack operations keep at least one
        revision of every object.  With garbage collection disabled, the
        pack code does not need to follow object references, making
        packing conceivably much faster.  However, some of that benefit
        may be lost due to an ever increasing number of unused objects.

        Disabling garbage collection is also a hack that ensures
        inter-database references never break.

``pack-prepack-only``
        If pack-prepack-only is true, pack operations perform a full analysis
        of what to pack, but no data is actually removed.  After a pre-pack,
        the pack_object, pack_state, and pack_state_tid tables are filled
        with the list of object states and objects that would have been
        removed.  If pack-gc is true, the object_ref table will also be fully
        populated. The object_ref table can be queried to discover references
        between stored objects.

``pack-skip-prepack``
        If pack-skip-prepack is true, the pre-pack phase is skipped and it
        is assumed the pack_object, pack_state and pack_state_tid tables have
        been filled already. Thus packing will only affect records already
        targeted for packing by a previous pre-pack analysis run.

        Use this option together with pack-prepack-only to split packing into
        distinct phases, where each phase can be run during different
        timeslots, or where a pre-pack analysis is run on a copy of the
        database to alleviate a production database load.

``pack-batch-timeout``
        Packing occurs in batches of transactions; this specifies the
        timeout in seconds for each batch.  Note that some database
        configurations have unpredictable I/O performance
        and might stall much longer than the timeout.
        The default timeout is 1.0 seconds.

``pack-commit-busy-delay``
        Before each pack batch, the commit lock is requested. If the lock is
        already held by for a regular commit, packing is paused for a short
        while. This option specifies how long the pack process should be
        paused before attempting to get the commit lock again.
        The default delay is 5.0 seconds.

``cache-servers``
        Specifies a list of memcached servers. Using memcached with
        RelStorage improves the speed of frequent object accesses while
        slightly reducing the speed of other operations.

        Provide a list of host:port pairs, separated by whitespace.
        "127.0.0.1:11211" is a common setting.  Some memcached modules,
        such as pylibmc, allow you to specify a path to a Unix socket
        instead of a host:port pair.

        The default is to disable memcached integration.

``cache-module-name``
        Specifies which Python memcache module to use. The default is
        "relstorage.pylibmc_wrapper", which requires pylibmc. An
        alternative module is "memcache", a pure Python module. If you
        use the memcache module, use at least version 1.47. This
        option has no effect unless cache-servers is set.

``cache-prefix``
        The prefix for all keys in the cache. All clients using a
        database should use the same cache-prefix. Defaults to the
        database name. (For example, in PostgreSQL, the database
        name is determined by executing ``SELECT current_database()``.)
        Set this if you have multiple databases with the same name.

``cache-local-mb``
        RelStorage caches pickled objects in memory, similar to a ZEO
        cache. The "local" cache is shared between threads. This option
        configures the approximate maximum amount of memory the cache
        should consume, in megabytes.  It defaults to 10.  Set to
        0 to disable the in-memory cache.

``cache-local-object-max``
        This option configures the maximum size of an object's pickle
        (in bytes) that can qualify for the "local" cache.  The size is
        measured before compression. Larger objects can still qualify
        for memcache.  The default is 16384 (1 << 14) bytes.

``cache-local-compression``
        This option configures compression within the "local" cache.
        This option names a Python module that provides two functions,
        ``compress()`` and ``decompress()``.  Supported values include
        ``zlib``, ``bz2``, and ``none`` (no compression).  The default is
        ``zlib``.

``cache-delta-size-limit``
        This is an advanced option. RelStorage uses a system of
        checkpoints to improve the cache hit rate. This option
        configures how many objects should be stored before creating a
        new checkpoint. The default is 10000.

``commit-lock-timeout``
        During commit, RelStorage acquires a database-wide lock. This
        option specifies how long to wait for the lock before
        failing the attempt to commit. The default is 30 seconds.

        The MySQL and Oracle adapters support this option. The
        PostgreSQL adapter currently does not.

``commit-lock-id``
        During commit, RelStorage acquires a database-wide lock. This
        option specifies the lock ID. This option currently applies
        only to the Oracle adapter.

``create-schema``
        Normally, RelStorage will create or update the database schema on
        start-up. Set this option to false if you need to connect to a
        RelStorage database without automatic creation or updates.

Adapter Options
===============

PostgreSQL Adapter Options
--------------------------

The PostgreSQL adapter accepts:

``dsn``
    Specifies the data source name for connecting to PostgreSQL.
    A PostgreSQL DSN is a list of parameters separated with
    whitespace.  A typical DSN looks like::

        dbname='zodb' user='username' host='localhost' password='pass'

    If dsn is omitted, the adapter will connect to a local database with
    no password.  Both the user and database name will match the
    name of the owner of the current process.

MySQL Adapter Options
---------------------

The MySQL adapter accepts most parameters supported by the MySQL-python
library, including:

``host``
    string, host to connect
``user``
    string, user to connect as
``passwd``
    string, password to use
``db``
    string, database to use
``port``
    integer, TCP/IP port to connect to
``unix_socket``
    string, location of unix_socket (UNIX-ish only)
``conv``
    mapping, maps MySQL FIELD_TYPE.* to Python functions which convert a
    string to the appropriate Python type
``connect_timeout``
    number of seconds to wait before the connection attempt fails.
``compress``
    if set, gzip compression is enabled
``named_pipe``
    if set, connect to server via named pipe (Windows only)
``init_command``
    command which is run once the connection is created
``read_default_file``
    see the MySQL documentation for mysql_options()
``read_default_group``
    see the MySQL documentation for mysql_options()
``client_flag``
    client flags from MySQLdb.constants.CLIENT
``load_infile``
    int, non-zero enables LOAD LOCAL INFILE, zero disables

Oracle Adapter Options
----------------------

The Oracle adapter accepts:

``user``
        The Oracle account name
``password``
        The Oracle account password
``dsn``
        The Oracle data source name.  The Oracle client library will
        normally expect to find the DSN in /etc/oratab.

Use with zodburi
================

This package also enable the use of the ``postgres://``, ``mysql://``
and ``oracle://`` URI schemes for zodburi_.
For more information about zodburi, please refer to its documentation. This
section contains information specific to the these schemes.

.. _zodburi: http://pypi.python.org/pypi/zodburi

URI schemes
--------------------------

The ``postgres://`` , ``mysql://`` and ``oracle://`` URI schemes can
be passed as ``zodbconn.uri`` to create a RelStorage PostgresSQL,
MySQL or Oracle database factory.  The uri should contain the user,
the password, the host, the port and the db name e.g.::

  postgres://someuser:somepass@somehost:5432/somedb?connection_cache_size=20000
  mysql://someuser:somepass@somehost:5432/somedb?connection_cache_size=20000

Because oracle connection information are most often given as dsn, the
oracle uri should not contain the same information as the other, but
only the dsn ::

  oracle://?dsn="HERE GOES THE DSN"

The URI scheme also accepts query string arguments.  The query string
arguments honored by this scheme are as follows.

RelStorage-constructor related
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These arguments generally inform the RelStorage constructor about
values of the same names :

poll_interval, cache_local_mb, commit_lock_timeout, commit_lock_id,
read_only, shared_blob_dir, keep_history, pack_gc, pack_dry_run,
strict_tpc, create, blob_cache_size, blob_cache_size_check,
blob_cache_chunk_size, replica_timeout, pack_batch_timeout,
pack_duty_cycle, pack_max_delay, name, blob_dir, replica_conf,
cache_module_name, cache_prefix, cache_delta_size_limit, cache_servers

Usual zodburi arguments
~~~~~~~~~~~~~~~~~~~~~~~

Arguments that are usual with zodburi are also available here (see
http://docs.pylonsproject.org/projects/zodburi/en/latest/) :

demostorage
  boolean (if true, wrap RelStorage in a DemoStorage)
database_name
  string
connection_cache_size
  integer (default 10000)
connection_pool_size
  integer (default 7)

Postgres specific
~~~~~~~~~~~~~~~~~

connection_timeout
  integer
ssl_mode
  string

Mysql specific
~~~~~~~~~~~~~~

connection_timeout
  integer
client_flag
  integer
load_infile
  integer
compress
  boolean
named_pipe
  boolean
unix_socket
  string
init_command
  string
read_default_file
  string
read_default_group
  string

Oracle specific
~~~~~~~~~~~~~~~

twophase
  integer
user
  string
password
  string
dsn
  string

Example
~~~~~~~

An example that combines a path with a query string::

  postgres://someuser:somepass@somehost:5432/somedb?connection_cache_size=20000

Development
===========

RelStorage is hosted at GitHub:

    https://github.com/zodb/relstorage

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
    therefore more appropriate for production use.

Q: How does RelStorage performance compare with FileStorage?

    A: According to benchmarks, RelStorage with PostgreSQL is often faster than
    FileStorage, especially under high concurrency.

Q: Why should I choose RelStorage?

    A: Because RelStorage is a fairly small layer that builds on world-class
    databases.  These databases have proven reliability and scalability, along
    with numerous support options.

Q: Can RelStorage replace ZRS (Zope Replication Services)?

    A: Yes, RelStorage inherits the replication capabilities of PostgreSQL,
    MySQL, and Oracle.

Q: How do I set up an environment to run the RelStorage tests?

    A: See README.txt in the relstorage/tests directory.


Project URLs
============

* http://pypi.python.org/pypi/RelStorage       (PyPI entry and downloads)
* http://shane.willowrise.com/                 (blog)
