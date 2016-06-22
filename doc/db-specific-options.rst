===================================
 Database-Specific Adapter Options
===================================

PostgreSQL Adapter Options
==========================

The PostgreSQL adapter accepts:

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

The MySQL adapter accepts most parameters supported by the MySQL-python
library, including:

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

user
        The Oracle account name
password
        The Oracle account password
dsn
        The Oracle data source name.  The Oracle client library will
        normally expect to find the DSN in ``/etc/oratab``.
