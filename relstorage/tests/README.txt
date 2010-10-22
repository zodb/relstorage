
Running Tests
=============

To run these tests, you need to create a test user account and several
databases. Use or adapt the SQL statements below to create the
databases.


PostgreSQL
----------

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


MySQL
-----

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


Oracle
------

Initial setup will require ``SYS`` privileges. Using Oracle 10g XE, you
can start a ``SYS`` session with the following shell commands::

    $ su - oracle
    $ sqlplus / as sysdba

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

When running the tests, you can use the environment variable
ORACLE_TEST_DSN to override the data source name, which defaults to
"XE" (for Oracle 10g XE).

