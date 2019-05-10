.. _migrate-to-1.1:

===============================================================
 Migrating from RelStorage version 1.0 or 1.0.1 to version 1.1
===============================================================

.. highlight:: sql

PostgreSQL::

    CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    DROP INDEX pack_object_keep_zoid;
    CREATE INDEX pack_object_keep_false ON pack_object (zoid)
        WHERE keep = false;
    CREATE INDEX pack_object_keep_true ON pack_object (zoid, keep_tid)
        WHERE keep = true;

    ALTER TABLE transaction ADD COLUMN empty BOOLEAN NOT NULL DEFAULT FALSE;

    CREATE INDEX current_object_tid ON current_object (tid);

    ALTER TABLE object_ref ADD PRIMARY KEY (tid, zoid, to_zoid);
    DROP INDEX object_ref_from;
    DROP INDEX object_ref_tid;
    DROP INDEX object_ref_to;

    CREATE TABLE pack_state (
        tid         BIGINT NOT NULL,
        zoid        BIGINT NOT NULL,
        PRIMARY KEY (tid, zoid)
    );

    CREATE TABLE pack_state_tid (
        tid         BIGINT NOT NULL PRIMARY KEY
    );

Users of PostgreSQL 8.2 and above should also drop the pack_lock table since
it has been replaced with an advisory lock::

    DROP TABLE pack_lock;

Users of PostgreSQL 8.1 and below still need the pack_lock table.  If you
have deleted it, please create it again with the following statement::

    CREATE TABLE pack_lock ();


MySQL::

    CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    ALTER TABLE transaction ADD COLUMN empty BOOLEAN NOT NULL DEFAULT FALSE;

    CREATE INDEX current_object_tid ON current_object (tid);

    ALTER TABLE object_ref ADD PRIMARY KEY (tid, zoid, to_zoid);
    ALTER TABLE object_ref DROP INDEX object_ref_from;
    ALTER TABLE object_ref DROP INDEX object_ref_tid;
    ALTER TABLE object_ref DROP INDEX object_ref_to;

    CREATE TABLE pack_state (
        tid         BIGINT NOT NULL,
        zoid        BIGINT NOT NULL,
        PRIMARY KEY (tid, zoid)
    ) ENGINE = MyISAM;

    CREATE TABLE pack_state_tid (
        tid         BIGINT NOT NULL PRIMARY KEY
    ) ENGINE = MyISAM;


Oracle::

    CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    ALTER TABLE transaction ADD empty CHAR DEFAULT 'N' CHECK (empty IN ('N', 'Y'));

    CREATE INDEX current_object_tid ON current_object (tid);

    ALTER TABLE object_ref ADD PRIMARY KEY (tid, zoid, to_zoid);
    DROP INDEX object_ref_from;
    DROP INDEX object_ref_tid;
    DROP INDEX object_ref_to;

    CREATE TABLE pack_state (
        tid         NUMBER(20) NOT NULL,
        zoid        NUMBER(20) NOT NULL,
        PRIMARY KEY (tid, zoid)
    );

    CREATE TABLE pack_state_tid (
        tid         NUMBER(20) NOT NULL PRIMARY KEY
    );
