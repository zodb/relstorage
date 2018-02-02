==============
 Installation
==============

RelStorage 2.0 is supported on Python 2.7, Python 3.4, 3.5 and 3.6,
and PyPy2 5.4.1 or later.

You can install RelStorage using pip::

    pip install RelStorage

If you use a recent version of pip to install RelStorage on a
supported platform (OS X, Windows or "manylinx"), you can get a
pre-built binary wheel. If you install from source or on a different
platform, you will need to have a functioning C compiler and the
ability to compile `CFFI extensions
<https://cffi.readthedocs.io/en/latest/installation.html>`_.

RelStorage requires a modern version of ZODB and ZEO; it is tested
with ZODB 4.4 and 5.x and ZEO 4.3 and 5.x. If you need to use an older
version of ZODB/ZEO, install RelStorage 1.6. Likewise, if you need
Python 2.6 support, install RelStorage 1.6 (note that 1.6 *does not*
run on Python 3 or PyPy).

Database Adapter
================

You also need the Python database adapter that corresponds with your
database.

.. tip::
   The easiest way to get the recommended and tested database adapter for
   your platform and database is to install the corresponding RelStorage
   extra; RelStorage has extras for all three databases that install
   the recommended driver on all platforms::

    pip install "RelStorage[mysql]"
    pip install "RelStorage[postgresql]"
    pip install "RelStorage[oracle]"


On CPython2, install psycopg2 2.6.1+, mysqlclient 1.3.7+, or cx_Oracle
5.2+ (but use caution with 5.2.1+); PyMySQL 0.7, MySQL
Connector/Python 8.0.6 and umysql are also known to work as is pg8000.

For CPython3, install psycopg2, mysqlclient or cx_Oracle;
PyMySQL, MySQL Connector/Python  and pg8000 are also known to work.

On PyPy, install psycopg2cffi 2.7.4+ or PyMySQL 0.6.6+ (PyPy will
generally work with psycopg2 and mysqlclient, but it will be *much*
slower; in contrast, pg8000 performs nearly as well. cx_Oracle is
untested on PyPy). MySQL Connector/Python is explicitly known *not* to
work on recent versions of PyPy. [#f1]_

Here's a table of known (tested) working adapters; adapters **in
bold** are the recommended adapter.

.. table:: Tested Adapters
   :widths: auto

   +----------+---------------------+---------------------+--------------+
   | Platform |  MySQL              |   PostgreSQL        |  Oracle      |
   +==========+=====================+=====================+==============+
   | CPython2 | 1. **mysqlclient**  |  1. **psycopg2**    | **cx_Oracle**|
   |          | 2. PyMySQL          |  2. pg8000          |              |
   |          | 3. umysqldb         |                     |              |
   |          | 4. MySQL Connector  |                     |              |
   |          |                     |                     |              |
   +----------+---------------------+---------------------+--------------+
   | CPython3 | 1. **mysqlclient**  |  1. **psycopg2**    | **cx_Oracle**|
   |          | 2. PyMySQL          |  2. pg8000          |              |
   |          | 3. MySQL Connector  |                     |              |
   +----------+---------------------+---------------------+--------------+
   | PyPy     | 1. **PyMySQL**      | 1. **psycopg2cffi** |              |
   |          |                     | 2.  pg8000          |              |
   +----------+---------------------+---------------------+--------------+


mysqlclient, MySQL Connector/Python (without its C extension), pg8000
and umysql are compatible (cooperative) with gevent.

For additional details and warnings, see the "driver" section for each database in
:doc:`db-specific-options`.

Memcache Integration
====================

If you want to use Memcache servers as an external shared cache for
RelStorage clients, you'll need to install either `pylibmc
<https://pypi.python.org/pypi/pylibmc>`_ (C based, requires Memcache
development libraries and CPython) or `python-memcached
<https://pypi.python.org/pypi/python-memcached>`_ (pure-Python, works
on CPython and PyPy, compatible with gevent).


Once RelStorage is installed, it's time to :doc:`configure the database <configure-database>`
you'll be using.

.. rubric:: Footnotes

.. [#f1] Broken tests `were observed
         <https://travis-ci.org/zodb/relstorage/jobs/336589498#L912>`_
         for PyPy2 5.8.0 using both Connector 2.1.5 and 8.0.6. `Tests
         passed <https://travis-ci.org/zodb/relstorage/builds/245866051>`_
         using PyPy2 5.6.0 and Connector 2.1.5. On the other hand,
         CPython 2.7.14 and CPython 2.7.9 are tested to work with the
         C extension, and CPython 3.6.0 and 3.6.3 have been tested to
         work with the pure-Python extension (the same extension PyPy
         would use).
