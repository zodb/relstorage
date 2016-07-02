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

The PostgreSQL adapter accepts:

driver
    Either "psycopg2" or "psycopg2cffi" for the native libpg based
    drivers. "pg8000" is a pure-python driver suitable for use with
    gevent.

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
    Either "MySQLdb" or "PyMySQL"
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

Oracle Adapter Options
======================

The Oracle adapter accepts:

driver
        Other than "auto" the only supported value is "cx_Oracle".
user
        The Oracle account name
password
        The Oracle account password
dsn
        The Oracle data source name.  The Oracle client library will
        normally expect to find the DSN in ``/etc/oratab``.
