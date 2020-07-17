==================
 Setting Up MySQL
==================

.. highlight:: shell

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

.. highlight:: sql

Here are some sample SQL statements for creating the user and database::

    CREATE USER 'zodbuser'@'localhost' IDENTIFIED BY 'mypassword';
    CREATE DATABASE zodb;
    GRANT ALL ON zodb.* TO 'zodbuser'@'localhost';
    FLUSH PRIVILEGES;

See the RelStorage option ``blob-chunk-size`` for information on
configuring the server's ``max_allowed_packet`` value for optimal
performance.

.. note::

   Granting ``SELECT`` access to the ``performance_schema`` to this
   user is highly recommended. This will allow RelStorage to present
   helpful information when it detects a lock failure in MySQL 8. In
   earlier versions, access to ``sys.innodb_lock_waits`` is suggested.
