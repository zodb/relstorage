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
from relstorage.adapters.interfaces import ISchemaInstaller
from zope.interface import implements
import time

class HistoryPreservingPostgreSQLSchema(object):
    implements(ISchemaInstaller)

    script = """
    CREATE TABLE commit_lock ();

    -- The list of all transactions in the database
    CREATE TABLE transaction (
        tid         BIGINT NOT NULL PRIMARY KEY,
        packed      BOOLEAN NOT NULL DEFAULT FALSE,
        empty       BOOLEAN NOT NULL DEFAULT FALSE,
        username    BYTEA NOT NULL,
        description BYTEA NOT NULL,
        extension   BYTEA
    );

    -- Create a special transaction to represent object creation.  This
    -- row is often referenced by object_state.prev_tid, but never by
    -- object_state.tid.
    INSERT INTO transaction (tid, username, description)
        VALUES (0, 'system', 'special transaction for object creation');

    CREATE SEQUENCE zoid_seq;

    -- All object states in all transactions.  Note that md5 and state
    -- can be null to represent object uncreation.
    CREATE TABLE object_state (
        zoid        BIGINT NOT NULL,
        tid         BIGINT NOT NULL REFERENCES transaction
                    CHECK (tid > 0),
        PRIMARY KEY (zoid, tid),
        prev_tid    BIGINT NOT NULL REFERENCES transaction,
        md5         CHAR(32),
        state       BYTEA
    );
    CREATE INDEX object_state_tid ON object_state (tid);
    CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    -- Pointers to the current object state
    CREATE TABLE current_object (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        tid         BIGINT NOT NULL,
        FOREIGN KEY (zoid, tid) REFERENCES object_state
    );
    CREATE INDEX current_object_tid ON current_object (tid);

    -- A list of referenced OIDs from each object_state.
    -- This table is populated as needed during packing.
    -- To prevent unnecessary table locking, it does not use
    -- foreign keys, which is safe because rows in object_state
    -- are never modified once committed, and rows are removed
    -- from object_state only by packing.
    CREATE TABLE object_ref (
        zoid        BIGINT NOT NULL,
        tid         BIGINT NOT NULL,
        to_zoid     BIGINT NOT NULL,
        PRIMARY KEY (tid, zoid, to_zoid)
    );

    -- The object_refs_added table tracks whether object_refs has
    -- been populated for all states in a given transaction.
    -- An entry is added only when the work is finished.
    -- To prevent unnecessary table locking, it does not use
    -- foreign keys, which is safe because object states
    -- are never added to a transaction once committed, and
    -- rows are removed from the transaction table only by
    -- packing.
    CREATE TABLE object_refs_added (
        tid         BIGINT NOT NULL PRIMARY KEY
    );

    -- Temporary state during packing:
    -- The list of objects to pack.  If keep is false,
    -- the object and all its revisions will be removed.
    -- If keep is true, instead of removing the object,
    -- the pack operation will cut the object's history.
    -- The keep_tid field specifies the oldest revision
    -- of the object to keep.
    -- The visited flag is set when pre_pack is visiting an object's
    -- references, and remains set.
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

    -- Temporary state during packing: the list of object states to pack.
    CREATE TABLE pack_state (
        tid         BIGINT NOT NULL,
        zoid        BIGINT NOT NULL,
        PRIMARY KEY (tid, zoid)
    );

    -- Temporary state during packing: the list of transactions that
    -- have at least one object state to pack.
    CREATE TABLE pack_state_tid (
        tid         BIGINT NOT NULL PRIMARY KEY
    );
    """

    def __init__(self, locker, connmanager):
        self.locker = locker
        self.connmanager = connmanager

    def create(self, cursor):
        """Create the database tables."""
        cursor.execute(self.script)
        self.locker.create_pack_lock(cursor)

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            cursor.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE tablename = 'object_state'
            """)
            if not cursor.rowcount:
                self.create(cursor)
        self.connmanager.open_and_call(callback)

    def zap_all(self):
        """Clear all data out of the database."""
        def callback(conn, cursor):
            cursor.execute("""
            DELETE FROM object_refs_added;
            DELETE FROM object_ref;
            DELETE FROM current_object;
            DELETE FROM object_state;
            DELETE FROM transaction;
            -- Create a special transaction to represent object creation.
            INSERT INTO transaction (tid, username, description) VALUES
                (0, 'system', 'special transaction for object creation');
            ALTER SEQUENCE zoid_seq RESTART WITH 1;
            """)
        self.connmanager.open_and_call(callback)

    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(conn, cursor):
            cursor.execute("SELECT tablename FROM pg_tables")
            existent = set([name for (name,) in cursor])
            for tablename in ('pack_state_tid', 'pack_state',
                    'pack_object', 'object_refs_added', 'object_ref',
                    'current_object', 'object_state', 'transaction',
                    'commit_lock', 'pack_lock'):
                if tablename in existent:
                    cursor.execute("DROP TABLE %s" % tablename)
            cursor.execute("DROP SEQUENCE zoid_seq")
        self.connmanager.open_and_call(callback)


class HistoryPreservingMySQLSchema(object):
    implements(ISchemaInstaller)

    script = """
    -- The list of all transactions in the database
    CREATE TABLE transaction (
        tid         BIGINT NOT NULL PRIMARY KEY,
        packed      BOOLEAN NOT NULL DEFAULT FALSE,
        empty       BOOLEAN NOT NULL DEFAULT FALSE,
        username    BLOB NOT NULL,
        description BLOB NOT NULL,
        extension   BLOB
    ) ENGINE = InnoDB;

    -- Create a special transaction to represent object creation.  This
    -- row is often referenced by object_state.prev_tid, but never by
    -- object_state.tid.
    INSERT INTO transaction (tid, username, description)
        VALUES (0, 'system', 'special transaction for object creation');

    -- All OIDs allocated in the database.  Note that this table
    -- is purposely non-transactional.
    CREATE TABLE new_oid (
        zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
    ) ENGINE = MyISAM;

    -- All object states in all transactions.  Note that md5 and state
    -- can be null to represent object uncreation.
    CREATE TABLE object_state (
        zoid        BIGINT NOT NULL,
        tid         BIGINT NOT NULL REFERENCES transaction,
        PRIMARY KEY (zoid, tid),
        prev_tid    BIGINT NOT NULL REFERENCES transaction,
        md5         CHAR(32) CHARACTER SET ascii,
        state       LONGBLOB,
        CHECK (tid > 0)
    ) ENGINE = InnoDB;
    CREATE INDEX object_state_tid ON object_state (tid);
    CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    -- Pointers to the current object state
    CREATE TABLE current_object (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        tid         BIGINT NOT NULL,
        FOREIGN KEY (zoid, tid) REFERENCES object_state (zoid, tid)
    ) ENGINE = InnoDB;
    CREATE INDEX current_object_tid ON current_object (tid);

    -- A list of referenced OIDs from each object_state.
    -- This table is populated as needed during packing.
    -- To prevent unnecessary table locking, it does not use
    -- foreign keys, which is safe because rows in object_state
    -- are never modified once committed, and rows are removed
    -- from object_state only by packing.
    CREATE TABLE object_ref (
        zoid        BIGINT NOT NULL,
        tid         BIGINT NOT NULL,
        to_zoid     BIGINT NOT NULL,
        PRIMARY KEY (tid, zoid, to_zoid)
    ) ENGINE = MyISAM;

    -- The object_refs_added table tracks whether object_refs has
    -- been populated for all states in a given transaction.
    -- An entry is added only when the work is finished.
    -- To prevent unnecessary table locking, it does not use
    -- foreign keys, which is safe because object states
    -- are never added to a transaction once committed, and
    -- rows are removed from the transaction table only by
    -- packing.
    CREATE TABLE object_refs_added (
        tid         BIGINT NOT NULL PRIMARY KEY
    ) ENGINE = MyISAM;

    -- Temporary state during packing:
    -- The list of objects to pack.  If keep is false,
    -- the object and all its revisions will be removed.
    -- If keep is true, instead of removing the object,
    -- the pack operation will cut the object's history.
    -- The keep_tid field specifies the oldest revision
    -- of the object to keep.
    -- The visited flag is set when pre_pack is visiting an object's
    -- references, and remains set.
    CREATE TABLE pack_object (
        zoid        BIGINT NOT NULL PRIMARY KEY,
        keep        BOOLEAN NOT NULL,
        keep_tid    BIGINT NOT NULL,
        visited     BOOLEAN NOT NULL DEFAULT FALSE
    ) ENGINE = MyISAM;
    CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

    -- Temporary state during packing: the list of object states to pack.
    CREATE TABLE pack_state (
        tid         BIGINT NOT NULL,
        zoid        BIGINT NOT NULL,
        PRIMARY KEY (tid, zoid)
    ) ENGINE = MyISAM;

    -- Temporary state during packing: the list of transactions that
    -- have at least one object state to pack.
    CREATE TABLE pack_state_tid (
        tid         BIGINT NOT NULL PRIMARY KEY
    ) ENGINE = MyISAM;
    """

    def __init__(self, connmanager, runner):
        self.connmanager = connmanager
        self.runner = runner

    def create(self, cursor):
        """Create the database tables."""
        self.runner.run_script(cursor, self.script)

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            cursor.execute("SHOW TABLES LIKE 'object_state'")
            if not cursor.rowcount:
                self.create(cursor)
        self.connmanager.open_and_call(callback)

    def zap_all(self):
        """Clear all data out of the database."""
        def callback(conn, cursor):
            stmt = """
            DELETE FROM object_refs_added;
            DELETE FROM object_ref;
            DELETE FROM current_object;
            DELETE FROM object_state;
            TRUNCATE new_oid;
            DELETE FROM transaction;
            -- Create a transaction to represent object creation.
            INSERT INTO transaction (tid, username, description) VALUES
                (0, 'system', 'special transaction for object creation');
            """
            self.runner.run_script(cursor, stmt)
        self.connmanager.open_and_call(callback)

    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(conn, cursor):
            for tablename in ('pack_state_tid', 'pack_state',
                    'pack_object', 'object_refs_added', 'object_ref',
                    'current_object', 'object_state', 'new_oid',
                    'transaction'):
                cursor.execute("DROP TABLE IF EXISTS %s" % tablename)
        self.connmanager.open_and_call(callback)


class HistoryPreservingOracleSchema(object):
    implements(ISchemaInstaller)

    script = """
    CREATE TABLE commit_lock (dummy CHAR);

    -- The list of all transactions in the database
    CREATE TABLE transaction (
        tid         NUMBER(20) NOT NULL PRIMARY KEY,
        packed      CHAR DEFAULT 'N' CHECK (packed IN ('N', 'Y')),
        empty       CHAR DEFAULT 'N' CHECK (empty IN ('N', 'Y')),
        username    RAW(500),
        description RAW(2000),
        extension   RAW(2000)
    );

    -- Create a special transaction to represent object creation.  This
    -- row is often referenced by object_state.prev_tid, but never by
    -- object_state.tid.
    INSERT INTO transaction (tid, username, description)
        VALUES (0,
        UTL_I18N.STRING_TO_RAW('system', 'US7ASCII'),
        UTL_I18N.STRING_TO_RAW(
            'special transaction for object creation', 'US7ASCII'));

    CREATE SEQUENCE zoid_seq;

    -- All object states in all transactions.
    -- md5 and state can be null to represent object uncreation.
    CREATE TABLE object_state (
        zoid        NUMBER(20) NOT NULL,
        tid         NUMBER(20) NOT NULL REFERENCES transaction
                    CHECK (tid > 0),
        PRIMARY KEY (zoid, tid),
        prev_tid    NUMBER(20) NOT NULL REFERENCES transaction,
        md5         CHAR(32),
        state       BLOB
    );
    CREATE INDEX object_state_tid ON object_state (tid);
    CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    -- Pointers to the current object state
    CREATE TABLE current_object (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        tid         NUMBER(20) NOT NULL,
        FOREIGN KEY (zoid, tid) REFERENCES object_state
    );
    CREATE INDEX current_object_tid ON current_object (tid);

    -- States that will soon be stored
    CREATE GLOBAL TEMPORARY TABLE temp_store (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        prev_tid    NUMBER(20) NOT NULL,
        md5         CHAR(32),
        state       BLOB
    ) ON COMMIT DELETE ROWS;

    -- During packing, an exclusive lock is held on pack_lock.
    CREATE TABLE pack_lock (dummy CHAR);

    -- A list of referenced OIDs from each object_state.
    -- This table is populated as needed during packing.
    -- To prevent unnecessary table locking, it does not use
    -- foreign keys, which is safe because rows in object_state
    -- are never modified once committed, and rows are removed
    -- from object_state only by packing.
    CREATE TABLE object_ref (
        zoid        NUMBER(20) NOT NULL,
        tid         NUMBER(20) NOT NULL,
        to_zoid     NUMBER(20) NOT NULL,
        PRIMARY KEY (tid, zoid, to_zoid)
    );

    -- The object_refs_added table tracks whether object_refs has
    -- been populated for all states in a given transaction.
    -- An entry is added only when the work is finished.
    -- To prevent unnecessary table locking, it does not use
    -- foreign keys, which is safe because object states
    -- are never added to a transaction once committed, and
    -- rows are removed from the transaction table only by
    -- packing.
    CREATE TABLE object_refs_added (
        tid         NUMBER(20) NOT NULL PRIMARY KEY
    );

    -- Temporary state during packing:
    -- The list of objects to pack.  If keep is 'N',
    -- the object and all its revisions will be removed.
    -- If keep is 'Y', instead of removing the object,
    -- the pack operation will cut the object's history.
    -- The keep_tid field specifies the oldest revision
    -- of the object to keep.
    -- The visited flag is set when pre_pack is visiting an object's
    -- references, and remains set.
    CREATE TABLE pack_object (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
        keep_tid    NUMBER(20) NOT NULL,
        visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
    );
    CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

    -- Temporary state during packing: the list of object states to pack.
    CREATE TABLE pack_state (
        tid         NUMBER(20) NOT NULL,
        zoid        NUMBER(20) NOT NULL,
        PRIMARY KEY (tid, zoid)
    );

    -- Temporary state during packing: the list of transactions that
    -- have at least one object state to pack.
    CREATE TABLE pack_state_tid (
        tid         NUMBER(20) NOT NULL PRIMARY KEY
    );

    -- Temporary state during packing: a list of objects
    -- whose references need to be examined.
    CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        keep_tid    NUMBER(20)
    );

    -- Temporary state during undo: a list of objects
    -- to be undone and the tid of the undone state.
    CREATE GLOBAL TEMPORARY TABLE temp_undo (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        prev_tid    NUMBER(20) NOT NULL
    );
    """

    def __init__(self, connmanager, runner):
        self.connmanager = connmanager
        self.runner = runner

    def create(self, cursor):
        """Create the database tables."""
        self.runner.run_script(cursor, self.script)
        # Let Oracle catch up with the new data definitions by sleeping.
        # This reduces the likelihood of spurious ORA-01466 errors.
        time.sleep(5)

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            cursor.execute("""
            SELECT 1 FROM USER_TABLES WHERE TABLE_NAME = 'OBJECT_STATE'
            """)
            if not cursor.fetchall():
                self.create(cursor)
        self.connmanager.open_and_call(callback)

    def zap_all(self):
        """Clear all data out of the database."""
        def callback(conn, cursor):
            stmt = """
            DELETE FROM object_refs_added;
            DELETE FROM object_ref;
            DELETE FROM current_object;
            DELETE FROM object_state;
            DELETE FROM transaction;
            -- Create a transaction to represent object creation.
            INSERT INTO transaction (tid, username, description) VALUES
                (0, UTL_I18N.STRING_TO_RAW('system', 'US7ASCII'),
                UTL_I18N.STRING_TO_RAW(
                'special transaction for object creation', 'US7ASCII'));
            DROP SEQUENCE zoid_seq;
            CREATE SEQUENCE zoid_seq;
            """
            self.runner.run_script(cursor, stmt)
        self.connmanager.open_and_call(callback)

    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(conn, cursor):
            for tablename in ('pack_state_tid', 'pack_state',
                    'pack_object', 'object_refs_added', 'object_ref',
                    'current_object', 'object_state', 'transaction',
                    'commit_lock', 'pack_lock',
                    'temp_store', 'temp_undo', 'temp_pack_visit'):
                cursor.execute("DROP TABLE %s" % tablename)
            cursor.execute("DROP SEQUENCE zoid_seq")
        self.connmanager.open_and_call(callback)
