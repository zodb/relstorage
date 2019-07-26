==============
 Installation
==============

RelStorage 3.0 is supported on Python 2.7.9+, Python 3.5, 3.6 and 3.7,
and PyPy2 and PyPy 3 7.1 or later.

You can install RelStorage using pip::

    pip install RelStorage

If you use a recent version of pip to install RelStorage on a
supported platform (OS X, Windows or "manylinx"), you can get a
pre-built binary wheel. If you install from source or on a different
platform, you will need to have a functioning C compiler and the
ability to compile `CFFI extensions
<https://cffi.readthedocs.io/en/latest/installation.html>`_.

RelStorage requires ZODB and ZEO 5. To use ZODB and ZEO 4 (which
supports Python 2.7.8 and earlier), install RelStorage 2.1. If you
need to use even older versions of ZODB/ZEO, install RelStorage 1.6.
Likewise, if you need Python 2.6 support, install RelStorage 1.6 (note
that 1.6 *does not* run on Python 3 or PyPy).

Database Adapter
================

You also need the Python database adapter that corresponds with your
database.

On CPython2, install psycopg2 2.8+, mysqlclient 1.4+, or cx_Oracle
5.2+ (but use caution with 5.2.1+); PyMySQL 0.7, MySQL
Connector/Python 8.0.16 is also tested to work as is pg8000.

.. note:: umysql support was removed in RelStorage 3.0. Use 'gevent
          MySQLdb' instead.

.. note:: mysqlclient 1.4 is not available on Windows for Python 2.
          PyMySQL is tested instead.

For CPython3, install psycopg2, mysqlclient 1.4+, or cx_Oracle;
PyMySQL, MySQL Connector/Python  and pg8000 are also known to work.

On PyPy, install psycopg2cffi 2.8+ or PyMySQL 0.6.6+ (PyPy will
generally work with psycopg2 and mysqlclient, but it will be *much*
slower; in contrast, pg8000 performs nearly as well. cx_Oracle is
untested on PyPy). MySQL Connector/Python is tested to work on PyPy
7.1.

.. tip::
   The easiest way to get the recommended and tested database adapter for
   your platform and database is to install the corresponding RelStorage
   extra; RelStorage has extras for all three databases that install
   the recommended driver on all platforms::

    pip install "RelStorage[mysql]"
    pip install "RelStorage[postgresql]"
    pip install "RelStorage[oracle]"

   Installing those packages may require you to have database client
   software and development libraries already installed. Some packages
   may provide binary wheels on PyPI for some platforms. In the case
   of psycopg2, that binary package (which is not recommended for
   production use) can be installed with the name ``psycopg2-binary``.
   Note that the ``postgresql`` extra in RelStorage does **not**
   install the binary but attempts to install from source.


Here's a table of known (tested) working adapters; adapters **in
bold** are the recommended adapter installed with the extra.

.. table:: Tested Adapters
   :widths: auto

   +----------+---------------------+---------------------+--------------+
   | Platform |  MySQL              |   PostgreSQL        |  Oracle      |
   +==========+=====================+=====================+==============+
   | CPython2 | 1. **mysqlclient**  |  1. **psycopg2**    | **cx_Oracle**|
   |          | 2. PyMySQL          |  2. pg8000          |              |
   |          | 3. MySQL Connector  |                     |              |
   |          |                     |                     |              |
   +----------+---------------------+---------------------+--------------+
   | CPython3 | 1. **mysqlclient**  |  1. **psycopg2**    | **cx_Oracle**|
   |          | 2. PyMySQL          |  2. pg8000          |              |
   |          | 3. MySQL Connector  |                     |              |
   +----------+---------------------+---------------------+--------------+
   | PyPy     | 1. **PyMySQL**      | 1. **psycopg2cffi** |              |
   |          | 2. MySQL Connector  | 2.  pg8000          |              |
   +----------+---------------------+---------------------+--------------+


mysqlclient can be used with gevent by explicitly choosing a
gevent-aware driver. PyMySQL, MySQL Connector/Python (without its C
extension), and pg8000 are compatible (cooperative) with gevent
when the system is monkey-patched.

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
