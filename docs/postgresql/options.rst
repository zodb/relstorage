============================
 PostgreSQL Adapter Options
============================

The name of the ZConfig section is ``<postgresql>``.

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
    fast binary COPY protocol (except where noted). Only ``gevent psycopg2``
    supports RelStorage's critical commit section. The possible options are:

    psycopg2
      A C-based driver that uses the C PostgreSQL client
      libraries. Optimal on CPython, but not compatible with gevent.
      For non-production, experimental usage, one can install the
      ``psycopg2-binary`` package to be able to use this driver
      without `needing a C compiler
      <http://initd.org/psycopg/docs/install.html#binary-packages>`_.

      This is the default and preferred driver everywhere except PyPy.

      If psycopg2 has a wait callback installed, this driver will not
      be available. If no driver is specified, the next choice for
      'auto' in this case will be gevent psycopg2.

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
      expense to the RelStorage connections (this will forfeit support
      for the critical commit section), or you could use the
      RelStorage driver module to create the other connections.

      This driver forfeits use of the COPY protocol and use of the
      C-accelerated BLOBs.

      This driver will only be available if psycopg2 has a wait
      callback installed, which, as mentioned above, happens
      automatically when gevent monkey-patches the system. No attempt
      is made to check that the wait callback is actually
      gevent-friendly in case it has been replaced.

      .. versionchanged:: 3.2.0
         Add support for the critical commit section.

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

     This driver cannot handle OID and TID parameters greater than
     nine quintillion (``2^63``).

dsn
    Specifies the data source name for connecting to PostgreSQL.
    A PostgreSQL DSN is a list of parameters separated with
    whitespace.  A typical DSN looks like::

        dbname='zodb' user='username' host='localhost' password='pass'

    If dsn is omitted, the adapter will connect to a local database with
    no password.  Both the user and database name will match the
    name of the owner of the current process.
