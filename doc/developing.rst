=======================
 Developing RelStorage
=======================

Hacking on RelStorage should generally be done in a virtual
environment. Buildout may also be used.

Running Tests
=============

RelStorage ships with an extensive test suite. It can be run from a
local checkout using tox::

  tox -e py27-mysql

There are environments for each database and each Python version.

You can also run tests manually. Most test file contain a ``__main__``
block that can be used with Python's ``-m``::

  python -m relstorage.tests.test_zodbconvert

Databases
---------

Many of RelStorage's test require access to a database. You will have
to have the appropriate database adapter (driver) installed to run
these tests; tests for which you don't have an adapter installed will
be skipped.

For example, to get started testing RelStorage against MySQL in a
virtual environment you could write::

  pip install -e ".[mysql]"

from the root of the checkout. This sets up an editable installation
of relstorage complete with the correct MySQL driver.


Before actually running the tests, you need to create a test user
account and several databases. Use or adapt the SQL statements below
to create the databases.

.. highlight:: sql

PostgreSQL
~~~~~~~~~~

Execute the following using the ``psql`` command::

    CREATE USER relstoragetest WITH PASSWORD 'relstoragetest';
    CREATE DATABASE relstoragetest OWNER relstoragetest;
    CREATE DATABASE relstoragetest2 OWNER relstoragetest;
    CREATE DATABASE relstoragetest_hf OWNER relstoragetest;
    CREATE DATABASE relstoragetest2_hf OWNER relstoragetest;

Also, add the following lines to the top of pg_hba.conf (if you put
them at the bottom, they may be overridden by other parameters)::

    local   relstoragetest     relstoragetest   md5
    local   relstoragetest2    relstoragetest   md5
    local   relstoragetest_hf  relstoragetest   md5
    local   relstoragetest2_hf relstoragetest   md5
    host    relstoragetest     relstoragetest   127.0.0.1/32 md5
    host    relstoragetest_hf  relstoragetest   127.0.0.1/32 md5


PostgreSQL specific tests can be run by the testposgresql module::

  python -m relstorage.tests.testpostgresql

If the environment variable ``RS_PG_SMALL_BLOB`` is set when running
the tests, certain blob tests will use a much smaller size, making the
test run much faster.

MySQL
~~~~~

Execute the following using the ``mysql`` command::

    CREATE USER 'relstoragetest'@'localhost' IDENTIFIED BY 'relstoragetest';
    CREATE DATABASE relstoragetest;
    GRANT ALL ON relstoragetest.* TO 'relstoragetest'@'localhost';
    CREATE DATABASE relstoragetest2;
    GRANT ALL ON relstoragetest2.* TO 'relstoragetest'@'localhost';
    CREATE DATABASE relstoragetest_hf;
    GRANT ALL ON relstoragetest_hf.* TO 'relstoragetest'@'localhost';
    CREATE DATABASE relstoragetest2_hf;
    GRANT ALL ON relstoragetest2_hf.* TO 'relstoragetest'@'localhost';
    FLUSH PRIVILEGES;


MySQL specific tests can be run by the testmysql module::

  python -m relstorage.tests.testmysql

.. note:: For some MySQL tests to pass (check16MObject), it may be
          necessary to increase the server's ``max_allowed_packet``
          setting. See the `MySQL Documentation
          <http://dev.mysql.com/doc/refman/5.5/en/packet-too-large.html>`_
          for more information.

Oracle
~~~~~~

.. highlight:: shell

Initial setup will require ``SYS`` privileges. Using Oracle 10g XE, you
can start a ``SYS`` session with the following shell commands::

    $ su - oracle
    $ sqlplus / as sysdba

.. highlight:: sql

Using ``sqlplus`` with ``SYS`` privileges, execute the
following::

    CREATE USER relstoragetest IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest;
    CREATE USER relstoragetest2 IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest2;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest2;
    CREATE USER relstoragetest_hf IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest_hf;
    CREATE USER relstoragetest2_hf IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest2_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest2_hf;

You may need to grant tablespace privileges if you get "no privileges
on tablespace" errors::

    grant unlimited tablespace to relstoragetest;
    grant unlimited tablespace to relstoragetest2;
    grant unlimited tablespace to relstoragetest_hf;
    grant unlimited tablespace to relstoragetest2_hf;

Oracle specific tests can be run by the testoracle module::

  python -m relstorage.tests.testoracle

When running the tests, you can use the environment variable
ORACLE_TEST_DSN to override the data source name, which defaults to
"XE" (for Oracle 10g XE). For example, using Oracle's Developer Days
Virtual Box VM with an IP of 192.168.1.131, you might set
ORACLE_TEST_DSN to ``192.168.1.131/orcl``. (And you would connect as
sysdba with ``sqlplus 'sys/oracle@192.168.1.131/orcl' as sysdba``.)

If the environment variable ``RS_ORCL_SMALL_BLOB`` is set when running
the tests, certain blob tests will use a much smaller size, making the
test run much faster.
