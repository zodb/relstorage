##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Database schema installers
"""
import logging
from relstorage.adapters.interfaces import ISchemaInstaller
from ZODB.POSException import StorageError
from zope.interface import implements
import re

# Versions of the installed stored procedures. Change these when
# the corresponding code changes.
oracle_package_version = '1.5A'
postgresql_proc_version = '1.5B'

log = logging.getLogger("relstorage")

history_preserving_schema = """

# commit_lock: Held during commit.  Another kind of lock is used for MySQL
# and Oracle.

    postgresql:
        CREATE TABLE commit_lock ();

# pack_lock: Held during pack.  Another kind of lock is used for MySQL.
# Another kind of lock is used for PostgreSQL >= 8.2.

    oracle:
        CREATE TABLE pack_lock (dummy CHAR);

# transaction: The list of all transactions in the database.

    postgresql:
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            empty       BOOLEAN NOT NULL DEFAULT FALSE,
            username    BYTEA NOT NULL,
            description BYTEA NOT NULL,
            extension   BYTEA
        );

    mysql:
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            empty       BOOLEAN NOT NULL DEFAULT FALSE,
            username    BLOB NOT NULL,
            description BLOB NOT NULL,
            extension   BLOB
        ) ENGINE = InnoDB;

    oracle:
        CREATE TABLE transaction (
            tid         NUMBER(20) NOT NULL PRIMARY KEY,
            packed      CHAR DEFAULT 'N' CHECK (packed IN ('N', 'Y')),
            empty       CHAR DEFAULT 'N' CHECK (empty IN ('N', 'Y')),
            username    RAW(500),
            description RAW(2000),
            extension   RAW(2000)
        );

# OID allocation

    postgresql:
        CREATE SEQUENCE zoid_seq;

    mysql:
        CREATE TABLE new_oid (
            zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) ENGINE = MyISAM;

    oracle:
        CREATE SEQUENCE zoid_seq;

# object_state and blob_chunk: All object and blob states in all
# transactions. Note that md5 and state can be null to represent object
# uncreation.

    postgresql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL REFERENCES transaction
                        CHECK (tid > 0),
                        PRIMARY KEY (zoid, tid),
            prev_tid    BIGINT NOT NULL REFERENCES transaction,
            md5         CHAR(32),
            state_size  BIGINT NOT NULL CHECK (state_size >= 0),
            state       BYTEA
        );
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

        CREATE TABLE blob_chunk (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            chunk_num   BIGINT NOT NULL,
                        PRIMARY KEY (zoid, tid, chunk_num),
            chunk       OID NOT NULL
        );
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
        CREATE INDEX blob_chunk_loid ON blob_chunk (chunk);
        ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
            FOREIGN KEY (zoid, tid)
            REFERENCES object_state (zoid, tid)
            ON DELETE CASCADE;

    mysql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL REFERENCES transaction,
                        PRIMARY KEY (zoid, tid),
                        CHECK (tid > 0),
            prev_tid    BIGINT NOT NULL REFERENCES transaction,
            md5         CHAR(32) CHARACTER SET ascii,
            state_size  BIGINT NOT NULL,
            state       LONGBLOB
        ) ENGINE = InnoDB;
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

        CREATE TABLE blob_chunk (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            chunk_num   BIGINT NOT NULL,
                        PRIMARY KEY (zoid, tid, chunk_num),
            chunk       LONGBLOB NOT NULL
        ) ENGINE = InnoDB;
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
        ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
            FOREIGN KEY (zoid, tid)
            REFERENCES object_state (zoid, tid)
            ON DELETE CASCADE;

    oracle:
        CREATE TABLE object_state (
            zoid        NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL REFERENCES transaction,
                        PRIMARY KEY (zoid, tid),
                        CONSTRAINT tid_min CHECK (tid > 0),
            prev_tid    NUMBER(20) NOT NULL REFERENCES transaction,
            md5         CHAR(32),
            state_size  NUMBER(20) NOT NULL,
                        CONSTRAINT state_size_min CHECK (state_size >= 0),
            state       BLOB
        );
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

        CREATE TABLE blob_chunk (
            zoid        NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL,
            chunk_num   NUMBER(20) NOT NULL,
                        PRIMARY KEY (zoid, tid, chunk_num),
            chunk       BLOB
        );
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
        ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
            FOREIGN KEY (zoid, tid)
            REFERENCES object_state (zoid, tid)
            ON DELETE CASCADE;

# current_object: Pointers to the current object state

    postgresql:
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
                        FOREIGN KEY (zoid, tid)
                            REFERENCES object_state (zoid, tid)
        );
        CREATE INDEX current_object_tid ON current_object (tid);

    mysql:
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
                        FOREIGN KEY (zoid, tid)
                            REFERENCES object_state (zoid, tid)
        ) ENGINE = InnoDB;
        CREATE INDEX current_object_tid ON current_object (tid);

    oracle:
        CREATE TABLE current_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL,
                        FOREIGN KEY (zoid, tid)
                            REFERENCES object_state (zoid, tid)
        );
        CREATE INDEX current_object_tid ON current_object (tid);

# object_ref: A list of referenced OIDs from each object_state. This
# table is populated as needed during packing. To prevent unnecessary
# table locking, it does not use foreign keys, which is safe because
# rows in object_state are never modified once committed, and rows are
# removed from object_state only by packing.

    postgresql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        );

    mysql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_ref (
            zoid        NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL,
            to_zoid     NUMBER(20) NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        );

# The object_refs_added table tracks whether object_refs has been
# populated for all states in a given transaction. An entry is added
# only when the work is finished. To prevent unnecessary table locking,
# it does not use foreign keys, which is safe because object states are
# never added to a transaction once committed, and rows are removed
# from the transaction table only by packing.

    postgresql:
        CREATE TABLE object_refs_added (
            tid         BIGINT NOT NULL PRIMARY KEY
        );

    mysql:
        CREATE TABLE object_refs_added (
            tid         BIGINT NOT NULL PRIMARY KEY
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_refs_added (
            tid         NUMBER(20) NOT NULL PRIMARY KEY
        );

# pack_object: Temporary state during packing: The list of objects to
# pack. If keep is false, the object and all its revisions will be
# removed. If keep is true, instead of removing the object, the pack
# operation will cut the object's history. The keep_tid field specifies
# the oldest revision of the object to keep. The visited flag is set
# when pre_pack is visiting an object's references, and remains set.

    postgresql:
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

    mysql:
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        ) ENGINE = MyISAM;
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

    oracle:
        CREATE TABLE pack_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
            keep_tid    NUMBER(20) NOT NULL,
            visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

# pack_state: Temporary state during packing: the list of object states
# to pack.

    postgresql:
        CREATE TABLE pack_state (
            tid         BIGINT NOT NULL,
            zoid        BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid)
        );

    mysql:
        CREATE TABLE pack_state (
            tid         BIGINT NOT NULL,
            zoid        BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid)
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE pack_state (
            tid         NUMBER(20) NOT NULL,
            zoid        NUMBER(20) NOT NULL,
            PRIMARY KEY (tid, zoid)
        );

# pack_state_tid: Temporary state during packing: the list of
# transactions that have at least one object state to pack.

    postgresql:
        CREATE TABLE pack_state_tid (
            tid         BIGINT NOT NULL PRIMARY KEY
        );

    mysql:
        CREATE TABLE pack_state_tid (
            tid         BIGINT NOT NULL PRIMARY KEY
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE pack_state_tid (
            tid         NUMBER(20) NOT NULL PRIMARY KEY
        );

# Oracle expects temporary tables to be created ahead of time, while
# MySQL and PostgreSQL expect them to be created in the session.

    oracle:
        # States that will soon be stored
        CREATE GLOBAL TEMPORARY TABLE temp_store (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            prev_tid    NUMBER(20) NOT NULL,
            md5         CHAR(32),
            state       BLOB,
            blobdata    BLOB
        ) ON COMMIT DELETE ROWS;

        CREATE GLOBAL TEMPORARY TABLE temp_blob_chunk (
            zoid        NUMBER(20) NOT NULL,
            chunk_num   NUMBER(20) NOT NULL,
            chunk       BLOB
        ) ON COMMIT DELETE ROWS;

        # Temporary state during packing: a list of objects
        # whose references need to be examined.
        CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep_tid    NUMBER(20) NOT NULL
        );

        # Temporary state during undo: a list of objects
        # to be undone and the tid of the undone state.
        CREATE GLOBAL TEMPORARY TABLE temp_undo (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            prev_tid    NUMBER(20) NOT NULL
        );
"""

history_preserving_init = """
# Create a special '0' transaction to represent object creation. The
# '0' transaction is often referenced by object_state.prev_tid, but
# never by object_state.tid.

    postgresql:
        INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');

    mysql:
        INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');

    oracle:
        INSERT INTO transaction (tid, username, description)
            VALUES (0,
            UTL_I18N.STRING_TO_RAW('system', 'US7ASCII'),
            UTL_I18N.STRING_TO_RAW(
                'special transaction for object creation', 'US7ASCII'));
"""

history_preserving_reset_oid = """
    postgresql:
        ALTER SEQUENCE zoid_seq RESTART WITH 1;

    mysql:
        TRUNCATE new_oid;

    oracle:
        DROP SEQUENCE zoid_seq;
        CREATE SEQUENCE zoid_seq;
"""

postgresql_procedures = """
CREATE OR REPLACE FUNCTION blob_chunk_delete_trigger() RETURNS TRIGGER
AS $blob_chunk_delete_trigger$
    -- Version: %(postgresql_proc_version)s
    -- Unlink large object data file after blob_chunk row deletion
    DECLARE
        cnt integer;
    BEGIN
        SELECT count(*) into cnt FROM blob_chunk WHERE chunk=OLD.chunk;
        IF (cnt = 1) THEN
            -- Last reference to this oid, unlink
            PERFORM lo_unlink(OLD.chunk);
        END IF;
        RETURN OLD;
    END;
$blob_chunk_delete_trigger$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION temp_blob_chunk_delete_trigger() RETURNS TRIGGER
AS $temp_blob_chunk_delete_trigger$
    -- Version: %(postgresql_proc_version)s
    -- Unlink large object data file after temp_blob_chunk row deletion
    DECLARE
        cnt integer;
    BEGIN
        SELECT count(*) into cnt FROM blob_chunk WHERE chunk=OLD.chunk;
        IF (cnt = 0) THEN
            -- No more references to this oid, unlink
            PERFORM lo_unlink(OLD.chunk);
        END IF;
        RETURN OLD;
    END;
$temp_blob_chunk_delete_trigger$ LANGUAGE plpgsql;
""" % globals()

oracle_history_preserving_package = """
CREATE OR REPLACE PACKAGE relstorage_op AS
    TYPE numlist IS TABLE OF NUMBER(20) INDEX BY BINARY_INTEGER;
    TYPE md5list IS TABLE OF VARCHAR2(32) INDEX BY BINARY_INTEGER;
    TYPE statelist IS TABLE OF RAW(2000) INDEX BY BINARY_INTEGER;
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist);
    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        md5s IN md5list,
        states IN statelist);
END relstorage_op;
/

CREATE OR REPLACE PACKAGE BODY relstorage_op AS
/* Version: %(oracle_package_version)s */
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM temp_store WHERE zoid = zoids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO temp_store (zoid, prev_tid, md5, state) VALUES
            (zoids(indx), prev_tids(indx), md5s(indx), states(indx));
    END store_temp;

    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        md5s IN md5list,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM object_state
            WHERE zoid = zoids(indx)
                AND tid = tids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO object_state
                (zoid, tid, prev_tid, md5, state_size, state)
                VALUES (zoids(indx), tids(indx),
                    COALESCE((SELECT tid
                        FROM current_object
                        WHERE zoid = zoids(indx)), 0),
                md5s(indx), COALESCE(LENGTH(states(indx)), 0), states(indx));
    END restore;
END relstorage_op;
/
""" % globals()


history_free_schema = """

# commit_lock: Held during commit.  Another kind of lock is used for MySQL
# and Oracle.

    postgresql:
        CREATE TABLE commit_lock ();

# pack_lock: Held during pack.  Another kind of lock is used for MySQL.
# Another kind of lock is used for PostgreSQL >= 8.2.

    oracle:
        CREATE TABLE pack_lock (dummy CHAR);

# OID allocation

    postgresql:
        CREATE SEQUENCE zoid_seq;

    mysql:
        CREATE TABLE new_oid (
            zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) ENGINE = MyISAM;

    oracle:
        CREATE SEQUENCE zoid_seq;

# object_state and blob_chunk: All object states in all transactions.

    postgresql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL CHECK (tid > 0),
            state_size  BIGINT NOT NULL CHECK (state_size >= 0),
            state       BYTEA
        );
        CREATE INDEX object_state_tid ON object_state (tid);

        CREATE TABLE blob_chunk (
            zoid        BIGINT NOT NULL,
            chunk_num   BIGINT NOT NULL,
                        PRIMARY KEY (zoid, chunk_num),
            tid         BIGINT NOT NULL,
            chunk       OID NOT NULL
        );
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
        CREATE INDEX blob_chunk_loid ON blob_chunk (chunk);
        ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
            FOREIGN KEY (zoid)
            REFERENCES object_state (zoid)
            ON DELETE CASCADE;

    mysql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
                        CHECK (tid > 0),
            state_size  BIGINT NOT NULL,
            state       LONGBLOB
        ) ENGINE = InnoDB;
        CREATE INDEX object_state_tid ON object_state (tid);

        CREATE TABLE blob_chunk (
            zoid        BIGINT NOT NULL,
            chunk_num   BIGINT NOT NULL,
                        PRIMARY KEY (zoid, chunk_num),
            tid         BIGINT NOT NULL,
            chunk       LONGBLOB NOT NULL
        ) ENGINE = InnoDB;
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
        ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
            FOREIGN KEY (zoid)
            REFERENCES object_state (zoid)
            ON DELETE CASCADE;

    oracle:
        CREATE TABLE object_state (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL,
                        CONSTRAINT tid_min CHECK (tid > 0),
            state_size  NUMBER(20) NOT NULL,
                        CONSTRAINT state_size_min CHECK (state_size >= 0),
            state       BLOB
        );
        CREATE INDEX object_state_tid ON object_state (tid);

        CREATE TABLE blob_chunk (
            zoid        NUMBER(20) NOT NULL,
            chunk_num   NUMBER(20) NOT NULL,
                        PRIMARY KEY (zoid, chunk_num),
            tid         NUMBER(20) NOT NULL,
            chunk       BLOB
        );
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
        ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
            FOREIGN KEY (zoid)
            REFERENCES object_state (zoid)
            ON DELETE CASCADE;

# object_ref: A list of referenced OIDs from each object_state. This
# table is populated as needed during packing.

    postgresql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );

    mysql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_ref (
            zoid        NUMBER(20) NOT NULL,
            to_zoid     NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );

# The object_refs_added table tracks which state of each object
# has been analyzed for references to other objects.

    postgresql:
        CREATE TABLE object_refs_added (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL
        );

    mysql:
        CREATE TABLE object_refs_added (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_refs_added (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL
        );

# pack_object contains temporary state during garbage collection: The
# list of all objects, a flag signifying whether the object should be
# kept, and a flag signifying whether the object's references have been
# visited. The keep_tid field specifies the current revision of the
# object.

    postgresql:
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

    mysql:
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        ) ENGINE = MyISAM;
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

    oracle:
        CREATE TABLE pack_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
            keep_tid    NUMBER(20) NOT NULL,
            visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

# Oracle expects temporary tables to be created ahead of time, while
# MySQL and PostgreSQL expect them to be created in the session.
# Note that the md5 column is not used in a history-free storage.

    oracle:
        # States that will soon be stored
        CREATE GLOBAL TEMPORARY TABLE temp_store (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            prev_tid    NUMBER(20) NOT NULL,
            md5         CHAR(32),
            state       BLOB
        ) ON COMMIT DELETE ROWS;

        CREATE GLOBAL TEMPORARY TABLE temp_blob_chunk (
            zoid        NUMBER(20) NOT NULL,
            chunk_num   NUMBER(20) NOT NULL,
            chunk       BLOB
        ) ON COMMIT DELETE ROWS;

        # Temporary state during packing: a list of objects
        # whose references need to be examined.
        CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep_tid    NUMBER(20) NOT NULL
        );
"""

history_free_reset_oid = """
    postgresql:
        ALTER SEQUENCE zoid_seq RESTART WITH 1;

    mysql:
        TRUNCATE new_oid;

    oracle:
        DROP SEQUENCE zoid_seq;
        CREATE SEQUENCE zoid_seq;
"""

oracle_history_free_package = """
CREATE OR REPLACE PACKAGE relstorage_op AS
    TYPE numlist IS TABLE OF NUMBER(20) INDEX BY BINARY_INTEGER;
    TYPE md5list IS TABLE OF VARCHAR2(32) INDEX BY BINARY_INTEGER;
    TYPE statelist IS TABLE OF RAW(2000) INDEX BY BINARY_INTEGER;
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist);
    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        states IN statelist);
END relstorage_op;
/

CREATE OR REPLACE PACKAGE BODY relstorage_op AS
/* Version: %(oracle_package_version)s */
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM temp_store WHERE zoid = zoids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO temp_store (zoid, prev_tid, md5, state) VALUES
            (zoids(indx), prev_tids(indx), md5s(indx), states(indx));
    END store_temp;

    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM object_state WHERE zoid = zoids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO object_state (zoid, tid, state_size, state) VALUES (
                zoids(indx),
                tids(indx),
                COALESCE(LENGTH(states(indx)), 0),
                states(indx));
    END restore;
END relstorage_op;
/
""" % globals()


def filter_script(script, database_type):
    res = []
    match = False
    for line in script.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.endswith(':'):
            match = (database_type in line[:-1].split())
            continue
        if match:
            res.append(line)
    return '\n'.join(res)


def filter_statements(script, expr):
    res = []
    match = False
    for line in script.splitlines():
        line = line.strip()
        if not match and expr.search(line) is not None:
            match = True
        if match:
            res.append(line)
            if line.rstrip().endswith(';'):
                match = False
    return '\n'.join(res)


class AbstractSchemaInstaller(object):

    # Keep this list in the same order as the schema scripts
    all_tables = (
        'commit_lock',
        'pack_lock',
        'transaction',
        'new_oid',
        'object_state',
        'blob_chunk',
        'current_object',
        'object_ref',
        'object_refs_added',
        'pack_object',
        'pack_state',
        'pack_state_tid',
        'temp_store',
        'temp_blob_chunk',
        'temp_pack_visit',
        'temp_undo',
    )

    database_type = None  # provided by a subclass

    def __init__(self, connmanager, runner, keep_history):
        self.connmanager = connmanager
        self.runner = runner
        self.keep_history = keep_history
        if keep_history:
            self.schema_script = history_preserving_schema
            self.init_script = history_preserving_init
            self.reset_oid_script = history_preserving_reset_oid
        else:
            self.schema_script = history_free_schema
            self.init_script = ''
            self.reset_oid_script = history_free_reset_oid

    def list_tables(self, cursor):
        raise NotImplementedError()

    def list_sequences(self, cursor):
        raise NotImplementedError()

    def get_database_name(self, cursor):
        raise NotImplementedError()

    def create(self, cursor):
        """Create the database tables."""
        script = filter_script(self.schema_script, self.database_type)
        self.runner.run_script(cursor, script)
        script = filter_script(self.init_script, self.database_type)
        self.runner.run_script(cursor, script)
        tables = self.list_tables(cursor)
        self.check_compatibility(cursor, tables)

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            tables = self.list_tables(cursor)
            if not 'object_state' in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)
        self.connmanager.open_and_call(callback)

    def check_compatibility(self, cursor, tables):
        if self.keep_history:
            if 'transaction' not in tables and 'current_object' not in tables:
                raise StorageError(
                    "Schema mismatch: a history-preserving adapter "
                    "can not connect to a history-free database. "
                    "If you need to convert, use the zodbconvert utility."
                )
        else:
            if 'transaction' in tables and 'current_object' in tables:
                raise StorageError(
                    "Schema mismatch: a history-free adapter "
                    "can not connect to a history-preserving database. "
                    "If you need to convert, use the zodbconvert utility."
                )

    def update_schema(self, cursor, tables):
        if not 'blob_chunk' in tables:
            # Add the blob_chunk table (RelStorage 1.5+)
            script = filter_script(
                self.schema_script, self.database_type)
            expr = (r'(CREATE|ALTER)\s+(GLOBAL TEMPORARY\s+)?(TABLE|INDEX)'
                '\s+(temp_)?blob_chunk')
            script = filter_statements(script, re.compile(expr, re.I))
            self.runner.run_script(cursor, script)

    def zap_all(self, reset_oid=True):
        """Clear all data out of the database."""
        def callback(conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = list(self.all_tables)
            todo.reverse()
            log.debug("Checking tables: %r", todo)
            for table in todo:
                log.debug("Considering table %s", table)
                if table.startswith('temp_'):
                    continue
                if table in existent:
                    log.debug("Deleting from table %s...", table)
                    cursor.execute("DELETE FROM %s" % table)
            log.debug("Done deleting from tables.")
            script = filter_script(self.init_script, self.database_type)

            if script:
                log.debug("Running init script.")
                self.runner.run_script(cursor, script)
                log.debug("Done running init script.")

            if reset_oid:
                script = filter_script(self.reset_oid_script,
                                       self.database_type)
                if script:
                    log.debug("Running OID reset script.")
                    self.runner.run_script(cursor, script)
                    log.debug("Done running OID reset script.")

        self.connmanager.open_and_call(callback)

    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = list(self.all_tables)
            todo.reverse()
            for table in todo:
                if table in existent:
                    cursor.execute("DROP TABLE %s" % table)
            for sequence in self.list_sequences(cursor):
                cursor.execute("DROP SEQUENCE %s" % sequence)
        self.connmanager.open_and_call(callback)


class PostgreSQLSchemaInstaller(AbstractSchemaInstaller):
    implements(ISchemaInstaller)

    database_type = 'postgresql'

    def __init__(self, connmanager, runner, locker, keep_history):
        super(PostgreSQLSchemaInstaller, self).__init__(
            connmanager, runner, keep_history)
        self.locker = locker

    def get_database_name(self, cursor):
        cursor.execute("SELECT current_database()")
        for (name,) in cursor:
            return name

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            tables = self.list_tables(cursor)
            if not 'object_state' in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)

            if not self.all_procedures_installed(cursor):
                self.install_procedures(cursor)
                if not self.all_procedures_installed(cursor):
                    raise AssertionError(
                        "Could not get version information after "
                        "installing the stored procedures.")

            triggers = self.list_triggers(cursor)
            if not 'blob_chunk_delete' in triggers:
                self.install_triggers(cursor)

        self.connmanager.open_and_call(callback)

    def create(self, cursor):
        """Create the database tables."""
        super(PostgreSQLSchemaInstaller, self).create(cursor)
        # Create the pack_lock table only on PostgreSQL 8.1 (not 8.2+)
        self.locker.create_pack_lock(cursor)

    def list_tables(self, cursor):
        cursor.execute("SELECT tablename FROM pg_tables")
        return [name for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        cursor.execute("SELECT relname FROM pg_class WHERE relkind = 'S'")
        return [name for (name,) in cursor.fetchall()]

    def list_languages(self, cursor):
        cursor.execute("SELECT lanname FROM pg_catalog.pg_language")
        return [name for (name,) in cursor.fetchall()]

    def install_languages(self, cursor):
        if 'plpgsql' not in self.list_languages(cursor):
            cursor.execute("CREATE LANGUAGE plpgsql")

    def list_procedures(self, cursor):
        """Returns {procedure name: version}.  version may be None."""
        stmt = """
        SELECT proname, prosrc
        FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_proc p ON pronamespace = n.oid
        JOIN pg_catalog.pg_type t ON prorettype = t.oid
        WHERE nspname = 'public'
        """
        cursor.execute(stmt)
        res = {}
        for (name, text) in cursor.fetchall():
            version = None
            match = re.search(r'Version:\s*([0-9a-zA-Z.]+)', text)
            if match is not None:
                version = match.group(1)
            res[name.lower()] = version
        return res

    def all_procedures_installed(self, cursor):
        """Check whether all required stored procedures are installed.

        Returns True only if all required procedures are installed and
        up to date.
        """
        expect = [
            'blob_chunk_delete_trigger',
            'temp_blob_chunk_delete_trigger',
        ]
        current_procs = self.list_procedures(cursor)
        for proc in expect:
            if current_procs.get(proc) != postgresql_proc_version:
                return False
        return True

    def install_procedures(self, cursor):
        """Install the stored procedures"""
        self.install_languages(cursor)
        cursor.execute(postgresql_procedures)

    def list_triggers(self, cursor):
        cursor.execute("SELECT tgname FROM pg_trigger")
        return [name for (name,) in cursor]

    def install_triggers(self, cursor):
        stmt = """
        CREATE TRIGGER blob_chunk_delete
            BEFORE DELETE ON blob_chunk
            FOR EACH ROW
            EXECUTE PROCEDURE blob_chunk_delete_trigger()
        """
        cursor.execute(stmt)

    def drop_all(self):
        def callback(conn, cursor):
            if 'blob_chunk' in self.list_tables(cursor):
                # Trigger deletion of blob OIDs.
                cursor.execute("DELETE FROM blob_chunk")
        self.connmanager.open_and_call(callback)
        super(PostgreSQLSchemaInstaller, self).drop_all()


class MySQLSchemaInstaller(AbstractSchemaInstaller):
    implements(ISchemaInstaller)

    database_type = 'mysql'

    def get_database_name(self, cursor):
        cursor.execute("SELECT DATABASE()")
        for (name,) in cursor:
            return name

    def list_tables(self, cursor):
        cursor.execute("SHOW TABLES")
        return [name for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        return []

    def check_compatibility(self, cursor, tables):
        super(MySQLSchemaInstaller, self).check_compatibility(cursor, tables)
        stmt = "SHOW TABLE STATUS LIKE 'object_state'"
        cursor.execute(stmt)
        for row in cursor:
            for col_index, col in enumerate(cursor.description):
                if col[0].lower() == 'engine':
                    engine = row[col_index]
                    if engine.lower() != 'innodb':
                        raise StorageError(
                            "The object_state table must use the InnoDB "
                            "engine, but it is using the %s engine." % engine)


class OracleSchemaInstaller(AbstractSchemaInstaller):
    implements(ISchemaInstaller)

    database_type = 'oracle'

    def get_database_name(self, cursor):
        cursor.execute("SELECT ora_database_name FROM DUAL")
        for (name,) in cursor:
            return name

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            tables = self.list_tables(cursor)
            if not 'object_state' in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)
            packages = self.list_packages(cursor)
            package_name = 'relstorage_op'
            if packages.get(package_name) != oracle_package_version:
                self.install_package(cursor)
                packages = self.list_packages(cursor)
                if packages.get(package_name) != oracle_package_version:
                    raise AssertionError(
                        "Could not get version information after "
                        "installing the %s package." % package_name)
        self.connmanager.open_and_call(callback)

    def install_package(self, cursor):
        """Install the package containing stored procedures"""
        if self.keep_history:
            code = oracle_history_preserving_package
        else:
            code = oracle_history_free_package

        for stmt in code.split('\n/\n'):
            if stmt.strip():
                cursor.execute(stmt)

    def list_tables(self, cursor):
        cursor.execute("SELECT table_name FROM user_tables")
        return [name.lower() for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        cursor.execute("SELECT sequence_name FROM user_sequences")
        return [name.lower() for (name,) in cursor.fetchall()]

    def list_packages(self, cursor):
        """List installed stored procedure packages.

        Returns {package name: version}.  version may be None.
        """
        stmt = """
        SELECT object_name
        FROM user_objects
        WHERE object_type = 'PACKAGE'
        """
        cursor.execute(stmt)
        names = [name for (name,) in cursor.fetchall()]

        res = {}
        for name in names:
            version = None
            stmt = """
            SELECT TEXT FROM USER_SOURCE
            WHERE TYPE='PACKAGE BODY'
                AND NAME=:1
            """
            cursor.execute(stmt, (name,))
            for (text,) in cursor:
                match = re.search(r'Version:\s*([0-9a-zA-Z.]+)', text)
                if match is not None:
                    version = match.group(1)
                    break
            res[name.lower()] = version
        return res
