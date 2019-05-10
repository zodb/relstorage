.. _migrate-to-1.4:


=======================================
 Migrating to RelStorage version 1.4.2
=======================================

.. highlight:: sql

If you are using a history-free storage, you need to drop and re-create
the object_refs_added table.  It contains only temporary state used during
packing, so it is safe to drop and create the table at any time except while
packing.

Do not make these changes to history-preserving databases.

PostgreSQL::

    DROP TABLE object_refs_added;
    CREATE TABLE object_refs_added (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        tid         BIGINT NOT NULL
    );

MySQL::

    DROP TABLE object_refs_added;
    CREATE TABLE object_refs_added (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        tid         BIGINT NOT NULL
    ) ENGINE = MyISAM;

Oracle::

    DROP TABLE object_refs_added;
    CREATE TABLE object_refs_added (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        tid         NUMBER(20) NOT NULL
    );


=====================================
 Migrating to RelStorage version 1.4
=====================================

Before following these directions, first upgrade to the schema of
RelStorage version 1.1.2 by following the directions in :ref:`migrate-to-1.1.2`.

Only Oracle needs a change for this release.  The Oracle adapter
now requires the EXECUTE permission on the DBMS_LOCK package.
In the example below, "zodb" is the name of the account::

    GRANT EXECUTE ON DBMS_LOCK TO zodb;

Also, the Oracle adapter in RelStorage no longer uses the commit_lock
table, so you can drop it.  It contains no data.

    DROP TABLE commit_lock;
