==============
 Installation
==============

RelStorage is supported on Python 2.7, Python 3.4 and 3.5, and PyPy2.

You can install RelStorage using pip::

    pip install RelStorage

RelStorage requires a modern version of ZODB; it is tested with ZODB
4.3 but *might* work with ZODB as old as 3.10. If you need to use an
older version of ZODB, install RelStorage 1.6.0.

Database Adapter
================

You also need the Python database adapter that corresponds with your
database. On CPython2, install psycopg2, MySQL-python 1.2.2+, or
cx_Oracle 4.3+; umysql is also known to work. For CPython3, install psycopg2, mysqlclient or
cx_Oracle. On PyPy, install psycopg2cffi or PyMySQL (PyPy will
generally work with psycopg2 and MySQLdb, but it will be *much*
slower; cx_Oracle is untested on PyPy).

The easiest way to get the recommended and tested database adapter for
your platform and database is to install the corresponding RelStorage
extra; RelStorage has extras for all three databases::

  pip install "RelStorage[mysql]"
  pip install "RelStorage[postgresql]"
  pip install "RelStorage[oracle]"



Once RelStorage is installed, it's time to :doc:`configure the database <configure-database>`
you'll be using.
