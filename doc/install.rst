==============
 Installation
==============

RelStorage 1.7 is supported on Python 2.7, Python 3.4 and 3.5, and PyPy2.

You can install RelStorage using pip::

    pip install RelStorage

Installing CFFI (and a functioning compiler) is also recommended::

    pip install cffi

RelStorage requires a modern version of ZODB; it is tested with ZODB
4.3 but *might* work with ZODB as old as 3.10. If you need to use an
older version of ZODB, install RelStorage 1.6.0. Likewise, if you need
Python 2.6 support, install RelStorage 1.6.0 (note that 1.6.0 *does
not* run on Python 3 or PyPy).

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
5.2+; PyMySQL 0.7 and umysql are also known to work as is pg8000. For
CPython3, install psycopg2, mysqlclient 1.3.7+ or cx_Oracle; PyMySQL
and pg8000 are also known to work. On PyPy, install psycopg2cffi
2.7.4+ or PyMySQL 0.6.6+ (PyPy will generally work with psycopg2 and
mysqlclient, but it will be *much* slower; in contrast, pg8000
performs nearly as well. cx_Oracle is untested on PyPy).

Here's a table of known working adapters; adapters **in bold** are the recommended
adapter; adapters in *italic* are also tested:

========   ================      =================     ======
Platform   MySQL                 PostgreSQL            Oracle
========   ================      =================     ======
CPython2   MySQL-python;         **psycopg2**;         **cx_Oracle**
           **mysqlclient**;      psycopg2cffi;
           *PyMySQL*;            *pg8000*
           umysql
CPython3   **mysqlclient**;      **psycopg2**;         **cx_Oracle**
           *PyMySQL*             *pg8000*
PyPy       **PyMySQL**           **psycopg2cffi**;
                                 *pg8000*
========   ================      =================     ======

Memcache Integration
====================

If you want to use Memcache servers as an external shared cache for
RelStorage clients, you'll need to install either `pylibmc
<https://pypi.python.org/pypi/pylibmc>`_ (C based, requires Memcache
development libraries and CPython) or `python-memcached
<https://pypi.python.org/pypi/python-memcached>`_ (pure-Python, works
on CPython and PyPy).


Once RelStorage is installed, it's time to :doc:`configure the database <configure-database>`
you'll be using.
