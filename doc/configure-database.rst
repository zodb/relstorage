===========================
 Configuring Your Database
===========================

You need to configure a database and user account for RelStorage.
RelStorage will populate the database with its schema the first time it
connects. Once you have the database configured, you can
:doc:`configure your application <configure-application>` to use RelStorage.

PostgreSQL
==========

If you installed PostgreSQL from a binary package, you probably have a
user account named ``postgres``. Since PostgreSQL respects the name of
the logged-in user by default, switch to the ``postgres`` account to
create the RelStorage user and database. Even ``root`` does not have
the PostgreSQL privileges that the ``postgres`` account has. For
example::

    $ sudo su - postgres
    $ createuser --pwprompt zodbuser
    $ createdb -O zodbuser zodb

New PostgreSQL accounts often require modifications to ``pg_hba.conf``,
which contains host-based access control rules. The location of
``pg_hba.conf`` varies, but ``/etc/postgresql/8.4/main/pg_hba.conf`` is
common. PostgreSQL processes the rules in order, so add new rules
before the default rules rather than after. Here is a sample rule that
allows only local connections by ``zodbuser`` to the ``zodb``
database::

    local  zodb  zodbuser  md5

PostgreSQL re-reads ``pg_hba.conf`` when you ask it to reload its
configuration file::

    /etc/init.d/postgresql reload

MySQL
=====

Use the ``mysql`` utility to create the database and user account. Note
that the ``-p`` option is usually required. You must use the ``-p``
option if the account you are accessing requires a password, but you
should not use the ``-p`` option if the account you are accessing does
not require a password. If you do not provide the ``-p`` option, yet
the account requires a password, the ``mysql`` utility will not prompt
for a password and will fail to authenticate.

Most users can start the ``mysql`` utility with the following shell
command, using any login account::

    $ mysql -u root -p

Here are some sample SQL statements for creating the user and database::

    CREATE USER 'zodbuser'@'localhost' IDENTIFIED BY 'mypassword';
    CREATE DATABASE zodb;
    GRANT ALL ON zodb.* TO 'zodbuser'@'localhost';
    FLUSH PRIVILEGES;

Oracle
======

Initial setup will require ``SYS`` privileges. Using Oracle 10g XE, you
can start a ``SYS`` session with the following shell commands::

    $ su - oracle
    $ sqlplus / as sysdba

You need to create a database user and grant execute privileges on
the DBMS_LOCK package to that user.
Here are some sample SQL statements for creating the database user
and granting the required permissions::

    CREATE USER zodb IDENTIFIED BY mypassword;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO zodb;
    GRANT EXECUTE ON DBMS_LOCK TO zodb;
