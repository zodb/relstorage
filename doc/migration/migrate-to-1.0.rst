====================
 1.0 Beta Migration
====================

.. highlight:: sql


Use one of the following scripts to migrate from RelStorage 1.0 beta to
RelStorage 1.0.  Alter the scripts to match the Python default encoding.
For example, if 'import sys; print sys.getdefaultencoding()' says the
encoding is "iso-8859-1", change all occurrences of 'UTF-8' or 'UTF8'
to 'ISO-8859-1'.


PostgreSQL 8.3 (using the psql command)::

    ALTER TABLE transaction
        ALTER username TYPE BYTEA USING (convert_to(username, 'UTF-8')),
        ALTER description TYPE BYTEA USING (convert_to(description, 'UTF-8'));

PostgreSQL 8.2 and below (using the psql command)::

    ALTER TABLE transaction
        ALTER username TYPE BYTEA USING
            (decode(replace(convert(username, 'UTF-8'), '\\', '\\\\'), 'escape')),
        ALTER description TYPE BYTEA USING
            (decode(replace(convert(description, 'UTF-8'), '\\', '\\\\'), 'escape'));

MySQL (using the mysql command)::

    ALTER TABLE transaction
        MODIFY username BLOB NOT NULL,
        MODIFY description BLOB NOT NULL;

Oracle (using the sqlplus command)::

    ALTER TABLE transaction ADD (
        new_username    RAW(500),
        new_description RAW(2000),
        new_extension   RAW(2000));

    UPDATE transaction
        SET new_username = UTL_I18N.STRING_TO_RAW(username, 'UTF8'),
            new_description = UTL_I18N.STRING_TO_RAW(description, 'UTF8'),
            new_extension = extension;

    ALTER TABLE transaction DROP (username, description, extension);
    ALTER TABLE transaction RENAME COLUMN new_username TO username;
    ALTER TABLE transaction RENAME COLUMN new_description TO description;
    ALTER TABLE transaction RENAME COLUMN new_extension TO extension;

Migration From PGStorage to RelStorage
======================================

PostgreSQL::

  -- Migration from PGStorage to RelStorage

  -- Do all the work in a transaction
  BEGIN;

  -- Remove the commit_order information (RelStorage has a better solution).
  DROP SEQUENCE commit_seq;
  ALTER TABLE transaction DROP commit_order;

  -- Make the special transaction 0 match RelStorage
  UPDATE transaction SET username='system',
    description='special transaction for object creation'
    WHERE tid = 0;

  -- Add the MD5 column and some more constraints.
  ALTER TABLE object_state
    ADD CONSTRAINT object_state_tid_check CHECK (tid > 0),
    ADD CONSTRAINT object_state_prev_tid_fkey FOREIGN KEY (prev_tid)
        REFERENCES transaction,
    ADD COLUMN md5 CHAR(32);
  UPDATE object_state SET md5=md5(state) WHERE state IS NOT NULL;

  -- Replace the temporary tables used for packing.
  DROP TABLE pack_operation;
  DROP TABLE pack_transaction;
  DROP TABLE pack_keep;
  DROP TABLE pack_garbage;
  CREATE TABLE pack_lock ();
  CREATE TABLE object_ref (
      zoid        BIGINT NOT NULL,
      tid         BIGINT NOT NULL,
      to_zoid     BIGINT NOT NULL
  );
  CREATE INDEX object_ref_from ON object_ref (zoid);
  CREATE INDEX object_ref_tid ON object_ref (tid);
  CREATE INDEX object_ref_to ON object_ref (to_zoid);
  CREATE TABLE object_refs_added (
      tid         BIGINT NOT NULL PRIMARY KEY
  );
  CREATE TABLE pack_object (
      zoid        BIGINT NOT NULL PRIMARY KEY,
      keep        BOOLEAN NOT NULL,
      keep_tid    BIGINT
  );
  CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

  -- Now commit everything
  COMMIT;
