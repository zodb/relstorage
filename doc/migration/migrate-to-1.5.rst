.. _migrate-to-1.5:

=====================================
 Migrating to RelStorage version 1.5
=====================================

All databases need a schema migration for this release.  This release
adds a state_size column to the object_state table, making it possible
to query the size of objects without loading the state.  The new column
is intended for gathering statistics.

Please note that if you are using the history-free schema, you need to
first migrate to RelStorage 1.4.2 by following the instructions in
:ref:`migrate-to-1.4`.

.. highlight:: sql


PostgreSQL
==========

1. Migrate the object_state table::

    BEGIN;
    ALTER TABLE object_state ADD COLUMN state_size BIGINT;
    UPDATE object_state SET state_size = COALESCE(LENGTH(state), 0);
    ALTER TABLE object_state ALTER COLUMN state_size SET NOT NULL;
    COMMIT;

2. The "plpgsql" language is now required and must be enabled in
   your database.  Depending on your version of PostgreSQL, you may have
   to execute the following psql command as the database superuser::

    CREATE LANGUAGE plpgsql;

3. If you used a beta version of RelStorage 1.5.0, you need to migrate
   your blob_chunk table schema::

    CREATE OR REPLACE FUNCTION blob_write(data bytea) RETURNS oid
    AS $blob_write$
        DECLARE
            new_oid OID;
            fd INTEGER;
            bytes INTEGER;
        BEGIN
            new_oid := lo_create(0);
            fd := lo_open(new_oid, 131072);
            bytes := lowrite(fd, data);
            IF (bytes != LENGTH(data)) THEN
                RAISE EXCEPTION 'Not all data copied to blob';
            END IF;
            PERFORM lo_close(fd);
            RETURN new_oid;
        END;
    $blob_write$ LANGUAGE plpgsql;
    BEGIN;
    ALTER TABLE blob_chunk RENAME COLUMN chunk TO oldbytea;
    ALTER TABLE blob_chunk ADD COLUMN chunk OID;
    UPDATE blob_chunk SET chunk = blob_write(oldbytea);
    ALTER TABLE blob_chunk
        ALTER COLUMN chunk SET NOT NULL,
        DROP COLUMN oldbytea;
    COMMIT;

If the conversion succeeded, the psql prompt will respond with "COMMIT".  If
something went wrong, psql will respond with "ROLLBACK".

3A. The script in step 3 does not reclaim the space occupied by the oldbytea
column. If there is a large amount of data in the blob_chunk table, you may
want to re-initialize the whole table by moving the data to a temporary table
and then copying it back::

    BEGIN;
    ALTER TABLE blob_chunk DISABLE TRIGGER USER;
    CREATE TEMP TABLE blob_chunk_copy (LIKE blob_chunk) ON COMMIT DROP;
    INSERT INTO blob_chunk_copy SELECT * FROM blob_chunk;
    TRUNCATE blob_chunk;
    INSERT INTO blob_chunk SELECT * FROM blob_chunk_copy;
    ALTER TABLE blob_chunk ENABLE TRIGGER USER;
    COMMIT;


MySQL history-preserving
========================

Execute::

    ALTER TABLE object_state ADD COLUMN state_size BIGINT AFTER md5;
    UPDATE object_state SET state_size = COALESCE(LENGTH(state), 0);
    ALTER TABLE object_state MODIFY state_size BIGINT NOT NULL AFTER md5;

MySQL history-free
==================

Execute::

    ALTER TABLE object_state ADD COLUMN state_size BIGINT AFTER tid;
    UPDATE object_state SET state_size = COALESCE(LENGTH(state), 0);
    ALTER TABLE object_state MODIFY state_size BIGINT NOT NULL AFTER tid;


Oracle
======

Execute::

    ALTER TABLE object_state ADD state_size NUMBER(20);
    UPDATE object_state SET state_size = COALESCE(LENGTH(state), 0);
    ALTER TABLE object_state MODIFY state_size NOT NULL;
