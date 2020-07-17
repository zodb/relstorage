=======================================
 Installing Database Drivers for MySQL
=======================================

.. tip:: See :doc:`../install` for the easiest way to get the best
         drivers.

.. note:: umysql support was removed in RelStorage 3.0. Use 'gevent
          MySQLdb' instead.

.. note:: mysqlclient 1.4 is not available on Windows for Python 2.
          PyMySQL is tested instead.

On CPython2, mysqlclient 1.4+. PyMySQL 0.7+ and MySQL
Connector/Python 8.0.16 are also tested to work.

For CPython3, install mysqlclient 1.4+; PyMySQL, and MySQL
Connector/Python are also known to work.

On PyPy, install PyMySQL 0.6.6+ (PyPy will generally work with
mysqlclient, but it will be *much* slower). MySQL Connector/Python is
tested to work on PyPy 7.1.

Here's a table of known (tested) working adapters; adapters **in
bold** are the recommended adapter installed with the extra.

.. table:: Tested Adapters
   :widths: auto

   +----------+---------------------+
   | Platform |  MySQL              |
   +==========+=====================+
   | CPython2 | 1. **mysqlclient**  |
   |          | 2. PyMySQL          |
   |          | 3. MySQL Connector  |
   |          |                     |
   +----------+---------------------+
   | CPython3 | 1. **mysqlclient**  |
   |          | 2. PyMySQL          |
   |          | 3. MySQL Connector  |
   +----------+---------------------+
   | PyPy     | 1. **PyMySQL**      |
   |          | 2. MySQL Connector  |
   +----------+---------------------+

gevent
------

mysqlclient can be used with gevent by explicitly choosing a
gevent-aware driver. PyMySQL and MySQL Connector/Python (without its C
extension) are compatible (cooperative) with gevent when the system is
monkey-patched.

For additional details and warnings, see the "driver" section in
:doc:`options`.
