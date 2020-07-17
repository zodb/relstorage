============================================
 Installing Database Drivers for PostgreSQL
============================================

.. tip:: See :doc:`../install` for the easiest way to get the best
         drivers.

On CPython2, install psycopg2 2.8+. pg8000 is also tested to work.

For CPython3, install psycopg2. pg8000 is also tested to work.


On PyPy, install psycopg2cffi 2.8+ (PyPy will generally work with
psycopg2 but it will be *much* slower; in contrast, pg8000 performs
nearly as well.)

Here's a table of known (tested) working adapters; adapters **in
bold** are the recommended adapter installed with the extra.

.. table:: Tested Adapters
   :widths: auto

   +----------+---------------------+
   | Platform |   PostgreSQL        |
   +==========+=====================+
   | CPython2 |  1. **psycopg2**    |
   |          |  2. pg8000          |
   |          |                     |
   |          |                     |
   +----------+---------------------+
   | CPython3 |  1. **psycopg2**    |
   |          |  2. pg8000          |
   |          |                     |
   +----------+---------------------+
   | PyPy     | 1. **psycopg2cffi** |
   |          | 2.  pg8000          |
   +----------+---------------------+

gevent
======

psycopg2 can be used with gevent by explicitly choosing a gevent-aware
driver if a wait callback (such as ``psycogreen``) is installed;
RelStorage will install an optimal one automatically if the system is
monkey-patched. pg8000 is compatible (cooperative) with gevent when
the system is monkey-patched.

For additional details and warnings, see the "driver" section in
:doc:`options`.
