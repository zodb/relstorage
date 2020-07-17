===========================
 Configuring Your Database
===========================

You need to configure a database (schema) and user account for RelStorage.
RelStorage will populate the database with its schema the first time it
connects. Once you have the database configured, you can
:doc:`configure your application <configure-application>` to use RelStorage.

.. note:: If you'll be developing on RelStorage itself, see :ref:`how
          to set up databases to run tests <test-databases>`.

.. toctree::

   postgresql/setup
   mysql/setup
   oracle/setup
   sqlite3/setup
