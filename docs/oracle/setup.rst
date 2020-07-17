===================
 Setting Up Oracle
===================

.. caution::

   Oracle is not used in production by the RelStorage maintainers and
   tends to lag behind in feature development; it also does not fully
   support parallel commit. If possible, choose PostgreSQL. Oracle
   support may be deprecated and eventually removed.

.. highlight:: shell

Initial setup will require ``SYS`` privileges. Using Oracle 10g XE, you
can start a ``SYS`` session with the following shell commands::

    $ su - oracle
    $ sqlplus / as sysdba

.. highlight:: sql

You need to create a database user and grant execute privileges on
the DBMS_LOCK package to that user.
Here are some sample SQL statements for creating the database user
and granting the required permissions::

    CREATE USER zodb IDENTIFIED BY mypassword;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO zodb;
    GRANT EXECUTE ON DBMS_LOCK TO zodb;
