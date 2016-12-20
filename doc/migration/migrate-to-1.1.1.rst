.. _migrate-to-1.1.1:

========================================================
 Migrating from RelStorage version 1.1 to version 1.1.1
========================================================

.. highlight:: sql


Before following these directions, first upgrade to the schema of
RelStorage version 1.1 by following the directions in :ref:`migrate-to-1.1`.

PostgreSQL::

    DROP TABLE pack_object;
    CREATE TABLE pack_object (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        keep        BOOLEAN NOT NULL,
        keep_tid    BIGINT NOT NULL,
        visited     BOOLEAN NOT NULL DEFAULT FALSE
    );
    CREATE INDEX pack_object_keep_false ON pack_object (zoid)
        WHERE keep = false;
    CREATE INDEX pack_object_keep_true ON pack_object (visited)
        WHERE keep = true;


MySQL::

    DROP TABLE pack_object;
    CREATE TABLE pack_object (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        keep        BOOLEAN NOT NULL,
        keep_tid    BIGINT NOT NULL,
        visited     BOOLEAN NOT NULL DEFAULT FALSE
    ) ENGINE = MyISAM;
    CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);


Oracle::

    DROP TABLE pack_object;
    CREATE TABLE pack_object (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
        keep_tid    NUMBER(20) NOT NULL,
        visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
    );
    CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);
