
Running Tests
=============

To run tests of both RelStorage 1.4 and 1.5 on the same box, you need
a set of databases for each version, since they have different schemas.
Do this after following the instructions in README.txt:


PostgreSQL
----------

Execute the following using the ``psql`` command::

    CREATE DATABASE relstorage15test OWNER relstoragetest;
    CREATE DATABASE relstorage15test2 OWNER relstoragetest;
    CREATE DATABASE relstorage15test_hf OWNER relstoragetest;
    CREATE DATABASE relstorage15test2_hf OWNER relstoragetest;

Also, add the following lines to the top of pg_hba.conf (if you put
them at the bottom, they may be overridden by other parameters)::

    local   relstorage15test     relstoragetest   md5
    local   relstorage15test2    relstoragetest   md5
    local   relstorage15test_hf  relstoragetest   md5
    local   relstorage15test2_hf relstoragetest   md5
    host    relstorage15test     relstoragetest   127.0.0.1/32 md5
    host    relstorage15test_hf  relstoragetest   127.0.0.1/32 md5



MySQL
-----

    CREATE DATABASE relstorage15test;
    GRANT ALL ON relstorage15test.* TO 'relstoragetest'@'localhost';
    CREATE DATABASE relstorage15test2;
    GRANT ALL ON relstorage15test2.* TO 'relstoragetest'@'localhost';
    CREATE DATABASE relstorage15test_hf;
    GRANT ALL ON relstorage15test_hf.* TO 'relstoragetest'@'localhost';
    CREATE DATABASE relstorage15test2_hf;
    GRANT ALL ON relstorage15test2_hf.* TO 'relstoragetest'@'localhost';
    FLUSH PRIVILEGES;


Oracle
------

Using ``sqlplus`` with ``SYS`` privileges, execute the
following::

    CREATE USER relstorage15test IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstorage15test;
    GRANT EXECUTE ON DBMS_LOCK TO relstorage15test;
    CREATE USER relstoragetest2 IDENTIFIED BY relstorage15test;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstorage15test2;
    GRANT EXECUTE ON DBMS_LOCK TO relstorage15test2;
    CREATE USER relstoragetest_hf IDENTIFIED BY relstorage15test;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstorage15test_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstorage15test_hf;
    CREATE USER relstoragetest2_hf IDENTIFIED BY relstorage15test;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstorage15test2_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstorage15test2_hf;
