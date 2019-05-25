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

RelStorage 2.1 performs best with PostgreSQL 9.5 or above.

The PostgreSQL adapter accepts:

driver
    The possible options are:

    psycopg2
      A C-based driver that requires the PostgreSQL development
      libraries. Optimal on CPython, but not compatible with gevent.

    psycopg2cffi
      A C-based driver that requires the PostgreSQL development
      libraries. Optimal on PyPy and almost indistinguishable from
      psycopg2 on CPython. Not compatible with gevent.

    pg8000
     A pure-Python driver suitable for use with gevent. Works on all
     supported platforms.

     .. note:: pg8000 requires PostgreSQL 9.4 or above for BLOB support.

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

The MySQL adapter accepts most parameters supported by the mysqlclient
library (the maintained version of MySQL-python), including:

driver
    The possible options are:

    MySQLdb
      A C-based driver that requires the MySQL client development
      libraries.. This is best provided by the PyPI distribution
      `mysqlclient <https://pypi.python.org/pypi/mysqlclient>`_.
      This driver is *not* compatible with gevent.

    gevent MySQLdb
      Like ``MySQLdb``, but explicitly uses's gevent's event loop to
      avoid blocking on the socket as much as possible when
      communicating with MySQL.

      Note that this is fairly coarse-grained: When sending a query,
      we can only yield until the socket is ready to write, and then
      we must write the entire query (because that portion is
      implemented in C). Likewise, we can only yield until results are
      ready to be read, and then we must read the entire query.

    PyMySQL
      A pure-Python driver provided by the distribution of the same
      name. It works with CPython 2 and 3 and PyPy (where it is
      preferred). It is compatible with gevent if gevent's
      monkey-patching is used.

    umysqldb
      A C-based driver that builds on PyMySQL. It is compatible with
      gevent if monkey-patching is used, but only works on CPython 2.
      It does not require the MySQL client development libraries but
      uses a project called ``umysql`` to communicate with the server
      using only sockets.

      .. note:: Make sure the server has a
          ``max_allowed_packet`` setting no larger than 16MB. Also
          make sure that RelStorage's ``blob-chunk-size`` is less than
          16MB as well.

      .. note:: `This fork of umysqldb
           <https://github.com/NextThought/umysqldb.git>`_ is
           recommended. The ``full-buffer`` branch of `this ultramysql
           fork
           <https://github.com/NextThought/ultramysql/tree/full-buffer>`_
           is also recommended if you encounter strange MySQL packet
           errors.


    Py MySQL Connector/Python
      This is the `official client
      <https://dev.mysql.com/doc/connector-python/en/>`_ provided by
      Oracle. It generally cannot be installed from PyPI or by pip if
      you want the optional C extension. It has an optional C
      extension that must be built manually. The C extension (which
      requires the MySQL client development libraries) performs
      about as well as mysqlclient, but the pure-python version
      somewhat slower than PyMySQL. However, it supports more advanced
      options for failover and high availability.

      RelStorage will only use the pure-Python implementation when
      using this name; this is compatible with gevent.

      Binary packages are distributed by Oracle for many platforms
      and include the necessary native libraries and C extension.

      .. versionadded:: 2.1a1

    C MySQL Connector/Python
      The same as above, but RelStorage will only use the C extension.
      This is not compatible with gevent.

      .. note::

         At least through version 8.0.16, this is not compatible with
         `CPython 3.7's development mode
         <https://docs.python.org/3/using/cmdline.html#envvar-PYTHONDEVMODE>`_;
         trying to use it with development mode enabled will crash the
         interpreter with "Fatal Python error: Python memory allocator
         called without holding the GIL."

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
    if set, gzip compression is enabled

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

The Oracle adapter accepts:

driver
    Other than "auto" the only supported value is "cx_Oracle".

    .. caution::
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
