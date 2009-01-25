##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""PostgreSQL adapter for RelStorage."""

from base64 import decodestring, encodestring
import logging
import psycopg2, psycopg2.extensions
import re
from ZODB.POSException import StorageError

from common import Adapter

log = logging.getLogger("relstorage.adapters.postgresql")


class PostgreSQLAdapter(Adapter):
    """PostgreSQL adapter for RelStorage."""

    def __init__(self, dsn='', twophase=False):
        self._dsn = dsn
        self._twophase = twophase

    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
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
        cursor.execute(stmt)

        if not self._pg_has_advisory_locks(cursor):
            cursor.execute("CREATE TABLE pack_lock ()")


    def prepare_schema(self):
        """Create the database schema if it does not already exist."""
        conn, cursor = self.open()
        try:
            try:
                cursor.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE tablename = 'object_state'
                """)
                if not cursor.rowcount:
                    self.create_schema(cursor)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def zap_all(self):
        """Clear all data out of the database."""
        conn, cursor = self.open()
        try:
            try:
                cursor.execute("""
                DELETE FROM object_refs_added;
                DELETE FROM object_ref;
                DELETE FROM current_object;
                DELETE FROM object_state;
                DELETE FROM transaction;
                -- Create a special transaction to represent object creation.
                INSERT INTO transaction (tid, username, description) VALUES
                    (0, 'system', 'special transaction for object creation');
                ALTER SEQUENCE zoid_seq START WITH 1;
                """)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def drop_all(self):
        """Drop all tables and sequences."""
        conn, cursor = self.open()
        try:
            try:
                cursor.execute("SELECT tablename FROM pg_tables")
                existent = set([name for (name,) in cursor])
                for tablename in ('pack_state_tid', 'pack_state',
                        'pack_object', 'object_refs_added', 'object_ref',
                        'current_object', 'object_state', 'transaction',
                        'commit_lock', 'pack_lock'):
                    if tablename in existent:
                        cursor.execute("DROP TABLE %s" % tablename)
                cursor.execute("DROP SEQUENCE zoid_seq")
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def open(self,
            isolation=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED):
        """Open a database connection and return (conn, cursor)."""
        try:
            conn = psycopg2.connect(self._dsn)
            conn.set_isolation_level(isolation)
            cursor = conn.cursor()
            cursor.arraysize = 64
        except psycopg2.OperationalError, e:
            log.warning("Unable to connect: %s", e)
            raise
        return conn, cursor

    def close(self, conn, cursor):
        """Close a connection and cursor, ignoring certain errors.
        """
        for obj in (cursor, conn):
            if obj is not None:
                try:
                    obj.close()
                except (psycopg2.InterfaceError,
                        psycopg2.OperationalError):
                    pass

    def _pg_version(self, cursor):
        """Return the (major, minor) version of PostgreSQL"""
        cursor.execute("SELECT version()")
        v = cursor.fetchone()[0]
        m = re.search(r"([0-9]+)[.]([0-9]+)", v)
        if m is None:
            raise AssertionError("Unable to detect PostgreSQL version: " + v)
        else:
            return int(m.group(1)), int(m.group(2))

    def _pg_has_advisory_locks(self, cursor):
        """Return true if this version of PostgreSQL supports advisory locks"""
        return self._pg_version(cursor) >= (8, 2)

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open(
            psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        stmt = """
        PREPARE get_latest_tid AS
        SELECT tid
        FROM transaction
        ORDER BY tid DESC
        LIMIT 1
        """
        cursor.execute(stmt)
        return conn, cursor

    def restart_load(self, cursor):
        """Reinitialize a connection for loading objects."""
        try:
            cursor.connection.rollback()
        except (psycopg2.OperationalError, psycopg2.InterfaceError), e:
            raise StorageError(e)

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # do later
        return 0

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        conn, cursor = self.open()
        try:
            cursor.execute("SELECT pg_database_size(current_database())")
            return cursor.fetchone()[0]
        finally:
            self.close(conn, cursor)

    def get_current_tid(self, cursor, oid):
        """Returns the current integer tid for an object.

        oid is an integer.  Returns None if object does not exist.
        """
        cursor.execute("""
        SELECT tid
        FROM current_object
        WHERE zoid = %s
        """, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()[0]
        return None

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        cursor.execute("""
        SELECT encode(state, 'base64'), tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = %s
        """, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            state64, tid = cursor.fetchone()
            if state64 is not None:
                state = decodestring(state64)
            else:
                # This object's creation has been undone
                state = None
            return state, tid
        else:
            return None, None

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        cursor.execute("""
        SELECT encode(state, 'base64')
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state64,) = cursor.fetchone()
            if state64 is not None:
                return decodestring(state64)
        return None

    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        cursor.execute("SELECT 1 FROM current_object WHERE zoid = %s", (oid,))
        return cursor.rowcount

    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        cursor.execute("""
        SELECT encode(state, 'base64'), tid
        FROM object_state
        WHERE zoid = %s
            AND tid < %s
        ORDER BY tid DESC
        LIMIT 1
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            state64, tid = cursor.fetchone()
            if state64 is not None:
                state = decodestring(state64)
            else:
                # The object's creation has been undone
                state = None
            return state, tid
        else:
            return None, None

    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT tid
        FROM object_state
        WHERE zoid = %s
            AND tid > %s
        ORDER BY tid
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()[0]
        else:
            return None

    def _make_temp_table(self, cursor):
        """Create the temporary table for storing objects"""
        if self._twophase:
            # PostgreSQL does not allow two phase transactions
            # to use temporary tables. :-(
            stmt = """
            CREATE TABLE temp_store (
                zoid        BIGINT NOT NULL,
                prev_tid    BIGINT NOT NULL,
                md5         CHAR(32),
                state       BYTEA
            );
            CREATE UNIQUE INDEX temp_store_zoid ON temp_store (zoid)
            """
        else:
            stmt = """
            CREATE TEMPORARY TABLE temp_store (
                zoid        BIGINT NOT NULL,
                prev_tid    BIGINT NOT NULL,
                md5         CHAR(32),
                state       BYTEA
            ) ON COMMIT DROP;
            CREATE UNIQUE INDEX temp_store_zoid ON temp_store (zoid)
            """
        cursor.execute(stmt)

    def open_for_store(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open()
        try:
            self._make_temp_table(cursor)
            return conn, cursor
        except:
            self.close(conn, cursor)
            raise

    def restart_store(self, cursor):
        """Reuse a store connection."""
        try:
            cursor.connection.rollback()
            if self._twophase:
                cursor.connection.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            self._make_temp_table(cursor)
        except (psycopg2.OperationalError, psycopg2.InterfaceError), e:
            raise StorageError(e)

    def store_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Store an object in the temporary table."""
        stmt = """
        INSERT INTO temp_store (zoid, prev_tid, md5, state)
        VALUES (%s, %s, %s, decode(%s, 'base64'))
        """
        cursor.execute(stmt, (oid, prev_tid, md5sum, encodestring(data)))

    def replace_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Replace an object in the temporary table."""
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = decode(%s, 'base64')
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, encodestring(data), oid))

    def restore(self, cursor, oid, tid, md5sum, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (%s, %s,
            COALESCE((SELECT tid FROM current_object WHERE zoid = %s), 0),
            %s, decode(%s, 'base64'))
        """
        if data is not None:
            data = encodestring(data)
        cursor.execute(stmt, (oid, tid, oid, md5sum, data))

    def start_commit(self, cursor):
        """Prepare to commit."""
        # Hold commit_lock to prevent concurrent commits
        # (for as short a time as possible).
        # Lock transaction and current_object in share mode to ensure
        # conflict detection has the most current data.
        cursor.execute("""
        LOCK TABLE commit_lock IN EXCLUSIVE MODE;
        LOCK TABLE transaction IN SHARE MODE;
        LOCK TABLE current_object IN SHARE MODE
        """)

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        cursor.execute("""
        SELECT tid, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
        FROM transaction
        ORDER BY tid DESC
        LIMIT 1
        """)
        assert cursor.rowcount == 1
        return cursor.fetchone()

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        stmt = """
        INSERT INTO transaction
            (tid, packed, username, description, extension)
        VALUES (%s, %s,
            decode(%s, 'base64'), decode(%s, 'base64'), decode(%s, 'base64'))
        """
        cursor.execute(stmt, (tid, packed,
            encodestring(username), encodestring(description),
            encodestring(extension)))

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        stmt = """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid,
            encode(temp_store.state, 'base64')
        FROM temp_store
            JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        LIMIT 1
        """
        cursor.execute(stmt)
        if cursor.rowcount:
            oid, prev_tid, attempted_prev_tid, data = cursor.fetchone()
            return oid, prev_tid, attempted_prev_tid, decodestring(data)
        return None

    def move_from_temp(self, cursor, tid):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT zoid, %s, prev_tid, md5, state
        FROM temp_store
        """
        cursor.execute(stmt, (tid,))

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in cursor]

    def update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        cursor.execute("""
        -- Insert objects created in this transaction into current_object.
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %(tid)s
            AND prev_tid = 0;

        -- Change existing objects.  To avoid deadlocks,
        -- update in OID order.
        UPDATE current_object SET tid = %(tid)s
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = %(tid)s
                AND prev_tid != 0
            ORDER BY zoid
        )
        """, {'tid': tid})

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        cursor.execute("""
        SELECT CASE WHEN %s > nextval('zoid_seq')
            THEN setval('zoid_seq', %s)
            ELSE 0
            END
        """, (oid, oid))

    def commit_phase1(self, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        if self._twophase:
            txn = 'T%d' % tid
            stmt = """
            DROP TABLE temp_store;
            PREPARE TRANSACTION %s
            """
            cursor.execute(stmt, (txn,))
            cursor.connection.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            return txn
        else:
            return '-'

    def commit_phase2(self, cursor, txn):
        """Final transaction commit."""
        if self._twophase:
            cursor.execute('COMMIT PREPARED %s', (txn,))
        else:
            cursor.connection.commit()

    def abort(self, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        if self._twophase:
            if txn is not None:
                cursor.execute('ROLLBACK PREPARED %s', (txn,))
        else:
            cursor.connection.rollback()

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "SELECT NEXTVAL('zoid_seq')"
        cursor.execute(stmt)
        return cursor.fetchone()[0]

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_try_advisory_lock(1)")
            locked = cursor.fetchone()[0]
            if not locked:
                raise StorageError('A pack or undo operation is in progress')
        else:
            # b/w compat
            try:
                cursor.execute("LOCK pack_lock IN EXCLUSIVE MODE NOWAIT")
            except psycopg2.DatabaseError:
                raise StorageError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_advisory_unlock(1)")
        # else no action needed since the lock will be released at txn commit

    _poll_query = "EXECUTE get_latest_tid"
