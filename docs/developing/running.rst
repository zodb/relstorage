===============
 Running Tests
===============

RelStorage ships with an extensive test suite. It can be run from a
local checkout using tox::

  tox -e py27-mysql

There are environments for each database and each Python version.

.. note:: This may not always be in a working state; the core
          developers tend to work in multiple independent virtual
          environments rather than using tox.

You can also run tests manually (be sure to have installed
relstorage's test dependencies; for example, ``pip install -e
.[test]``). Many test files contain a ``__main__`` block that can be
used with Python's ``-m``::

  python -m relstorage.tests.test_zodbconvert

Running all tests can be done with zope.testrunner::

  zope-testrunner --test-path=src

For each database driver installed, tests will be run. Use the
``--unit`` argument to only run the (small) subset of tests that do
not require access to an operational database. Use ``--layer
<driver>`` to run tests for a particular database driver; for example,
``--layer PG8000``. The ``-t`` and ``-m`` arguments are also useful
for filtering tests; the ``--repeat`` argument can help find
intermittent issues.

Shell aliases make this more convenient::

    alias ztest='zope-testrunner --test-path=src --auto-color --auto-progress'
    alias ztestv='zope-testrunner --test-path=src --auto-color -vvv --slow-test=3'

.. note:: zope-testrunner is how the core developers test. It's
          advanced features (layers) are required to run the complete
          test suite.

The ``--list`` argument can be used to discover tests. To find the
available layers, look for ``Listing``:

.. code:: console

   $ ztest --list | grep Listing | sort
   Listing .MySQLCMySQLConnector_Python_SharedHistoryFreeBlobTests tests:
   ...
   Listing .Oraclecx_Oracle_SharedHistoryFreeBlobTests tests:
   Listing .Oraclecx_Oracle_SharedHistoryPreservingBlobTests tests:
   ...
   Listing .PostgreSQLgeventPsycopg2Driver_SharedHistoryFreeBlobTests tests:
   ...
   Listing .Sqlite3sqlite3_SharedHistoryFreeBlobTests tests:
   ...

.. _test-databases:

Databases
=========

Many of RelStorage's test require access to a database. You will have
to have the appropriate database adapter (driver) installed to run
these tests; tests for which you don't have an adapter installed will
be skipped.

For example, to get started testing RelStorage against MySQL in a
virtual environment you could write::

  pip install -e ".[test,mysql]"

from the root of the checkout. This sets up an editable installation
of relstorage complete with the correct MySQL driver.

For all databases except SQLite, before actually running the tests,
you need to create a test user account and several databases. Use or
adapt the SQL statements below to create the databases. (You can also
see example scripts that are used to set up the continuous integration
test environment in the `.travis
<https://github.com/zodb/relstorage/tree/master/.travis>`_ directory.)

If the environment variable ``RS_SMALL_BLOB`` is set when running
the tests, certain blob tests will use a much smaller size, making the
test run much faster.

.. tip:: See ``relstorage.tests.util`` for other environment variables
         RelStorage's tests look at. ``RS_DB_HOST`` is useful if you
         want to have the database servers running on a different machine.

.. highlight:: sql

PostgreSQL
----------

Execute the following using the ``psql`` command:

.. literalinclude:: ../../.travis/postgres.sh
   :language: shell

Also, add the following lines to the top of pg_hba.conf (if you put
them at the bottom, they may be overridden by other parameters)::

    local   relstoragetest     relstoragetest   md5
    local   relstoragetest2    relstoragetest   md5
    local   relstoragetest_hf  relstoragetest   md5
    local   relstoragetest2_hf relstoragetest   md5
    host    relstoragetest     relstoragetest   127.0.0.1/32 md5
    host    relstoragetest_hf  relstoragetest   127.0.0.1/32 md5


PostgreSQL specific tests can be run with the PostgreSQL layer::

  ztest --layer PostgreSQL

MySQL
-----

Execute the following using the ``mysql`` command:

.. literalinclude:: ../../.travis/mysql.sh
   :language: shell

MySQL specific tests can be run with the MySQL layer::

  ztest --layer MySQL

.. note:: For some MySQL tests to pass (check16MObject), it may be
          necessary to increase the server's ``max_allowed_packet``
          setting. See the `MySQL Documentation
          <http://dev.mysql.com/doc/refman/5.5/en/packet-too-large.html>`_
          for more information.

Oracle
------

.. highlight:: shell

Initial setup will require ``SYS`` privileges. Using Oracle 10g XE, you
can start a ``SYS`` session with the following shell commands::

    $ su - oracle
    $ sqlplus / as sysdba

.. highlight:: sql

Using ``sqlplus`` with ``SYS`` privileges, execute the
following::

    CREATE USER relstoragetest IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO relstoragetest;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest;
    CREATE USER relstoragetest2 IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO relstoragetest2;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest2;
    CREATE USER relstoragetest_hf IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO relstoragetest_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest_hf;
    CREATE USER relstoragetest2_hf IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO relstoragetest2_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest2_hf;

You may need to grant tablespace privileges if you get "no privileges
on tablespace" errors::

    grant unlimited tablespace to relstoragetest;
    grant unlimited tablespace to relstoragetest2;
    grant unlimited tablespace to relstoragetest_hf;
    grant unlimited tablespace to relstoragetest2_hf;

Oracle specific tests can be run with the Oracle layer::

  ztest --layer Oracle

When running the tests, you can use the environment variable
ORACLE_TEST_DSN to override the data source name, which defaults to
"XE" (for Oracle 10g XE). For example, using Oracle's Developer Days
Virtual Box VM with an IP of 192.168.1.131, you might set
ORACLE_TEST_DSN to ``192.168.1.131/orcl``. (And you would connect as
sysdba with ``sqlplus 'sys/oracle@192.168.1.131/orcl' as sysdba``.)

If the environment variable ``RS_ORCL_SMALL_BLOB`` is set when running
the tests, certain blob tests will use a much smaller size, making the
test run much faster.

Docs:
    http://www.oracle.com/pls/db102/homepage

Excellent setup instructions:
    http://www.davidpashley.com/articles/oracle-install.html

Work around session limit (fixes ORA-12520)::

    ALTER SYSTEM SET PROCESSES=150 SCOPE=SPFILE;
    ALTER SYSTEM SET SESSIONS=150 SCOPE=SPFILE;
    (then restart Oracle)

Manually rollback an in-dispute transaction::

    select local_tran_id, state from DBA_2PC_PENDING;
    rollback force '$local_tran_id';
