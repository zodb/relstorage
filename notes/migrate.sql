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
