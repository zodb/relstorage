=======================
 MySQL Adapter Options
=======================

The ZConfig section name is ``<mysql>``.

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
