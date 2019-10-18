==================
 Use With zodburi
==================

This package also enable the use of the ``postgres://``, ``mysql://``
and ``oracle://`` URI schemes for zodburi_.
For more information about zodburi, please refer to its documentation. This
section contains information specific to the these schemes.

.. _zodburi: http://pypi.python.org/pypi/zodburi

URI schemes
===========

The ``postgres://`` , ``mysql://``, ``oracle://`` and ``sqlite://`` URI schemes can
be passed as ``zodbconn.uri`` to create a RelStorage PostgresSQL,
MySQL or Oracle database factory.  The uri should contain the user,
the password, the host, the port and the db name e.g.::

  postgres://someuser:somepass@somehost:5432/somedb?connection_cache_size=20000
  mysql://someuser:somepass@somehost:5432/somedb?connection_cache_size=20000

Because oracle connection information are most often given as dsn, the
oracle uri should not contain the same information as the other, but
only the dsn ::

  oracle://?dsn="HERE GOES THE DSN"

Likewise, SQLite only needs the file path::

  sqlite://?path=/the/path/to/database.sqlite3

Query String Arguments
======================

The URI scheme also accepts query string arguments.  The query string
arguments honored by this scheme are as follows.

RelStorage-constructor related
------------------------------

These arguments generally inform the RelStorage constructor about
values of the same names :

cache_local_mb, commit_lock_timeout, commit_lock_id,
read_only, shared_blob_dir, keep_history, pack_gc, pack_dry_run,
strict_tpc, create, blob_cache_size, blob_cache_size_check,
blob_cache_chunk_size, replica_timeout, pack_batch_timeout,
pack_duty_cycle, pack_max_delay, name, blob_dir, replica_conf,
cache_module_name, cache_prefix, cache_delta_size_limit, cache_servers

Usual zodburi arguments
-----------------------

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
-----------------

connection_timeout
  integer
ssl_mode
  string

Mysql specific
--------------

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
---------------

twophase
  integer
user
  string
password
  string
dsn
  string

Example
=======

An example that combines a path with a query string::

  postgres://someuser:somepass@somehost:5432/somedb?connection_cache_size=20000
