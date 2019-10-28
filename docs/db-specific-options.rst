===================================
 Database-Specific Adapter Options
===================================

Each adapter supports one common option:

driver
    The name of the driver to use for this database. Defaults to
    "auto", meaning to choose the best driver from among the
    possibilities. Most of the time this option should be omitted and
    RelStorage will choose the best option.

    This is handy to set when an environment might have multiple
    drivers installed, some of which might be non-optimal. For
    example, on PyPy, PyMySQL is generally faster than MySQLdb, but
    both can be installed (in the form of mysqlclient for the latter).

    This is also convenient when using zodbshootout to compare driver
    speeds.

    If you specify a driver that is not installed, an error will be raised.

    Each adapter will document the available driver names.

    .. versionadded:: 2.0b2

PostgreSQL Adapter Options
==========================

RelStorage 3.0 requires PostgreSQL 9.6 or above.

.. tip::

   PostgreSQL defaults to storing persistent object state data (pickles)
   on disk in a compressed format if they are longer than roughly
   2,000 bytes. Thus wrapper storages like ``zc.zlibstorage`` are
   unlikely to save much disk space. They may still reduce network
   traffic, however, at the cost of CPU usage in the Python process.

   If you used a compressing wrapper, `you can disable this disk
   compression
   <https://www.postgresql.org/docs/current/storage-toast.html#STORAGE-TOAST-ONDISK>`_
   with the SQL command ``ALTER TABLE object_state ALTER COLUMN STATE
   SET storage EXTERNAL``.

The PostgreSQL adapter accepts:

driver
    All of these drivers use the name of the corresponding PyPI
    package. All drivers support uploading objects using PostgreSQL's
    fast binary COPY protocol (except where noted). None of the gevent
    drivers support RelStorage's critical commit section. The possible options are:

    psycopg2
      A C-based driver that uses the C PostgreSQL client
      libraries. Optimal on CPython, but not compatible with gevent.
      For non-production, experimental usage, one can install the
      ``psycopg2-binary`` package to be able to use this driver
      without `needing a C compiler
      <http://initd.org/psycopg/docs/install.html#binary-packages>`_.

      This is the default and preferred driver everywhere except PyPy.

    gevent psycopg2
      The same as ``psycopg2``, but made to be gevent compatible
      through the use of a wait callback. If the system is
      monkey-patched by gevent, RelStorage will automatically install
      a wait callback optimized for RelStorage. If you won't be
      monkey-patching, you must install a compatible wait
      callback yourself (perhaps using `psycogreen
      <https://pypi.org/project/psycogreen/>`__).

      A wait callback is shared between all connections created by the
      psycopg2 library. There is no way to have some connections be
      blocking and some non-blocking. If you'll be using psycopg2
      connections in the same process outside of RelStorage, the wait
      callback RelStorage installs won't work for those other
      connections. You can install a more general callback at a slight
      expense to the RelStorage connections, or you could use the
      RelStorage driver module to create the other connections.

      This driver forfeits use of the COPY protocol and use of the
      C-accelerated BLOBs.

    psycopg2cffi
      A C-based driver that requires the PostgreSQL client
      libraries. Optimal on PyPy and almost indistinguishable from
      psycopg2 on CPython. Not compatible with gevent.

      This is the default and preferred driver for PyPy.

    pg8000
     A pure-Python driver suitable for use with gevent (if the system
     is monkey-patched). Works on all supported platforms, and can use
     the COPY protocol even when the system is monkey-patched.

     This driver makes use of ``SET SESSION CHARACTERISTICS`` and thus
     `may not work well
     <http://initd.org/psycopg/docs/connection.html#connection.set_session>`_
     with certain configurations of connection load balancers.

dsn
    Specifies the data source name for connecting to PostgreSQL.
    A PostgreSQL DSN is a list of parameters separated with
    whitespace.  A typical DSN looks like::

        dbname='zodb' user='username' host='localhost' password='pass'

    If dsn is omitted, the adapter will connect to a local database with
    no password.  Both the user and database name will match the
    name of the owner of the current process.

MySQL Adapter Options
=====================

RelStorage 3.0 requires MySQL 5.7.19 or above. MySQL 5.7.21 or higher, or
MySQL 8.0.16 or higher, is strongly recommended.

.. note::

   MySQL versions earlier than 8.0.16 do not support or preserve table
   ``CHECK`` constraints. Upgrading a RelStorage schema from MySQL 5.7.x to
   8.0.x will thus not have any defined or enforced table ``CHECK``
   constraints, but creating a new RelStorage schema under MySQL 8.0
   will include enforced ``CHECK`` constraints.

.. caution::

   MySQL 5.0.18 and earlier contain crashing bugs. See :pr:`287` for
   details.

driver
    The possible options are:

    MySQLdb
      A C-based driver that requires the MySQL client
      libraries.. This is best provided by the PyPI distribution
      `mysqlclient <https://pypi.python.org/pypi/mysqlclient>`_.
      This driver is *not* compatible with gevent, though alternate
      distributions exist and were used in the past.

      This is the default and preferred driver on CPython on all
      platforms except Windows.

    gevent MySQLdb
      Like ``MySQLdb``, but explicitly uses's gevent's event loop to
      avoid blocking on the socket as much as possible when
      communicating with MySQL.

      Note that this is fairly coarse-grained: When sending a query,
      we can only yield until the socket is ready to write, and then
      we must write the entire query (because that portion is
      implemented in C). Likewise, we can only yield until results are
      ready to be read, and then we must read the entire results,
      unless a server-side cursor is used.

      Supports RelStorage's critical commit section. Supports server-side
      cursors for large result sets, and if they are large, will
      periodically yield to gevent while iterating them.

    PyMySQL
      A pure-Python driver provided by the distribution of the same
      name. It works with CPython 2 and 3 and PyPy (where it is
      preferred). It is compatible with gevent if gevent's
      monkey-patching is used.

      This is the default and preferred driver on Windows and on PyPy.

    Py MySQL Connector/Python

      This is the `official client
      <https://dev.mysql.com/doc/connector-python/en/>`_ built by
      Oracle and distributed as `mysql-connector-python on PyPI
      <https://pypi.org/project/mysql-connector-python/8.0.17/#files>`_.

      It has an optional C extension. The C extension (which uses the
      MySQL client libraries) performs about as well as mysqlclient,
      but the pure-python version is somewhat slower than PyMySQL.
      However, it supports more advanced options for failover and high
      availability.

      RelStorage will only use the pure-Python implementation when
      using this name; this is compatible with gevent monkey-patching.

      Binary packages are distributed by Oracle for many platforms
      and include the necessary native libraries and C extension.
      These can be installed from PyPI or downloaded from Oracle.

      .. versionadded:: 2.1a1

    C MySQL Connector/Python
      The same as above, but RelStorage will only use the C extension.
      This is not compatible with gevent.

      .. caution::

         At least through version 8.0.16, this driver is not
         recommended.

         It fails the checks established by `CPython 3.7's development
         mode
         <https://docs.python.org/3/using/cmdline.html#envvar-PYTHONDEVMODE>`_;
         trying to use it with development mode enabled will crash the
         interpreter with "Fatal Python error: Python memory allocator
         called without holding the GIL." This signals potentially
         serious internal problems.

The MySQL adapter accepts most parameters supported by the mysqlclient
library (the maintained version of MySQL-python). If a particular
driver doesn't support a parameter, it will be ignored. The parameters
include:

host
    string, host to connect

user
    string, user to connect as

passwd
    string, password to use

db
    string, database to use

port
    integer, TCP/IP port to connect to

unix_socket
    string, location of unix_socket (UNIX-ish only)

conv
    mapping, maps MySQL FIELD_TYPE.* to Python functions which convert a
    string to the appropriate Python type

connect_timeout
    number of seconds to wait before the connection attempt fails.

compress
    if set, gzip network compression is enabled

named_pipe
    if set, connect to server via named pipe (Windows only)

init_command
    command which is run once the connection is created

read_default_file
    see the MySQL documentation for mysql_options()

read_default_group
    see the MySQL documentation for mysql_options()

client_flag
    client flags from MySQLdb.constants.CLIENT

load_infile
    int, non-zero enables LOAD LOCAL INFILE, zero disables

.. _oracle-adapter-options:

Oracle Adapter Options
======================

The Oracle adapter has been tested against Oracle 12, but likely works
with Oracle 10 as well. The only supported driver is `cx_Oracle
<https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html>`_.

The Oracle adapter accepts:

driver
    Other than "auto" the only supported value is "cx_Oracle".

    .. caution::
        (This is a historical note. Only version 6.0 and above of
        cx_Oracle is supported in RelStorage 3.)

        If you use cx_Oracle 5.2.1 or 5.3 (in general, any version >=
        5.2 but < 6.0) you must be sure that it is compiled against a
        version of the Oracle client that is compatible with the
        Oracle database to which you will be connecting.

        Specifically, if you will be connecting to Oracle database 11
        or earlier, you must *not* compile against client version 12.
        (Compiling against an older client and connecting to a newer
        database is fine.) If you use a client that is too new,
        RelStorage will fail to commit with the error ``DatabaseError:
        ORA-03115: unsupported network datatype or representation``.

        For more details, see :issue:`172`.

user
    The Oracle account name

password
    The Oracle account password

dsn
    The Oracle data source name.  The Oracle client library will
    normally expect to find the DSN in ``/etc/oratab``.

SQLite Adapter Options
======================

This adapter uses the built-in sqlite3 module provided by the Python
standard library. It is available on Python 2.7 (including PyPy) and
Python 3.6 and above (including PyPy3), as long as the underlying
version of SQLite is at least 3.11. The best performance can be
obtained by ensuring the underlying SQLite is at least 3.24.

A SQLite database can be used by multiple processes concurrently, but
because it uses shared memory, those processes *must* all be on the
same machine. The database files also should reside locally.

Using a persistent cache file is not supported with this driver and
will be automatically disabled. In some cases, it may be advantageous
to also disable RelStorage's in-memory pickle cache
altogether (``cache-local-mb 0``) and allow the operating system's
filesystem cache to serve that purpose.

This adapter supports blobs, but you still must configure a
``blob-cache-dir``, or use a ``shared-blob-dir``.

For more, see :doc:`faq`.

There is one required setting:

data-dir
    The path to a directory to hold the data.

    Choosing a dedicated directory is strongly recommended. A network
    filesystem is generally not recommended.

    Several files will be created in this directory automatically by
    RelStorage. Some are persistent while others are transient. Do not
    remove them or data corruption may result.

Optional settings include:

driver
    This can be set to "gevent sqlite3" to activate a mode that yields
    to the gevent loop periodically. Normally, sqlite3 gives up the
    GIL and allows other threads to run while executing SQLite
    queries, but for a gevent-based system, that's not particularly
    helpful. This driver will call ``gevent.sleep()`` approximately every
    ``gevent_yield_interval`` virtual machine instractions executed by
    any given connection. (It's approximate because the instruction
    count is tracked per-prepared statement. RelStorage uses a few
    different prepared statements during normal operations.)

    The gevent sqlite driver supports RelStorage's critical commit section.

gevent_yield_interval
    Only used if the driver is ``gevent sqlite``. The default is
    currently 100, but that's arbitrary and subject to change. Choosing a specific value that
    works well for your application is recommended. Note that this
    will interact with the ``cache-local-mb`` value as that's shared
    among Connections and could reduce the number of SQLite queries
    executed.


RelStorage configures SQLite to work well and be safe. For advanced
tuning, nearly the entire set of `SQLite PRAGMAs
<https://www.sqlite.org/pragma.html>`_ are available. Put them in the
``pragmas`` section of the configuration. For example, this
configuration is meant to make SQLite run as fast as possible while
ignoring safety considerations::

        <sqlite3>
            data-dir /dev/shm
            <pragmas>
                synchronous off
                checkpoint_fullfsync off
                defer_foreign_keys on
                fullfsync off
                ignore_check_constraints on
                foreign_keys off
            </pragmas>
        </sqlite3>

Particularly useful pragmas to consider adjusting include
``cache_size`` and ``mmap_size``.

Setting ``wal_autocheckpoint`` to a larger value than the default may
improve write speed at the expense of read speed and substantially
increased disk usage. (A setting of 0 is not recommended.)

Setting ``max_page_count`` can be used to enforce a crude quota
system.

The ``journal_mode`` cannot be changed.
