##############################################################################
#
# Copyright (c) 2008-2009 Zope Foundation and Contributors.
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

from relstorage.adapters.packless.common import PacklessAdapter

log = logging.getLogger("relstorage.adapters.postgresql")

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (psycopg2.OperationalError, psycopg2.InterfaceError)


class PacklessPostgreSQLAdapter(PacklessAdapter):
    """Packless PostgreSQL adapter for RelStorage."""

    def __init__(self, dsn=''):
        self._dsn = dsn

    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
        CREATE TABLE commit_lock ();

        CREATE SEQUENCE zoid_seq;

        -- All object states in all transactions.
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL CHECK (tid > 0),
            state       BYTEA
        );
        CREATE INDEX object_state_tid ON object_state (tid);

        -- A list of referenced OIDs from each object_state.
        -- This table is populated as needed during garbage collection.
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );

        -- The object_refs_added table tracks whether object_refs has
        -- been populated for all states in a given transaction.
        -- An entry is added only when the work is finished.
        CREATE TABLE object_refs_added (
            tid         BIGINT NOT NULL PRIMARY KEY
        );

        -- Temporary state during garbage collection:
        -- The list of all objects, a flag signifying whether
        -- the object should be kept, and a flag signifying whether
        -- the object's references have been visited.
        CREATE TABLE gc_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL DEFAULT FALSE,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        );
        CREATE INDEX gc_object_keep_false ON gc_object (zoid)
            WHERE keep = false;
        CREATE INDEX gc_object_keep_true ON gc_object (visited)
            WHERE keep = true;
        """
        cursor.execute(stmt)

        if not self._pg_has_advisory_locks(cursor):
            cursor.execute("CREATE TABLE gc_lock ()")


    def prepare_schema(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            cursor.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE tablename = 'object_state'
            """)
            if not cursor.rowcount:
                self.create_schema(cursor)
        self._open_and_call(callback)

    def zap_all(self):
        """Clear all data out of the database."""
        def callback(conn, cursor):
            cursor.execute("""
            DELETE FROM object_refs_added;
            DELETE FROM object_ref;
            DELETE FROM object_state;
            ALTER SEQUENCE zoid_seq START WITH 1;
            """)
        self._open_and_call(callback)

    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(conn, cursor):
            cursor.execute("SELECT tablename FROM pg_tables")
            existent = set([name for (name,) in cursor])
            for tablename in ('gc_object', 'object_refs_added',
                    'object_ref', 'object_state', 'commit_lock', 'pack_lock'):
                if tablename in existent:
                    cursor.execute("DROP TABLE %s" % tablename)
            cursor.execute("DROP SEQUENCE zoid_seq")
        self._open_and_call(callback)

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
                except disconnected_exceptions:
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
        FROM object_state
        ORDER BY tid DESC
        LIMIT 1
        """
        cursor.execute(stmt)
        return conn, cursor

    def restart_load(self, cursor):
        """Reinitialize a connection for loading objects."""
        try:
            cursor.connection.rollback()
        except disconnected_exceptions, e:
            raise StorageError(e)

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # do later
        return 0

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        def callback(conn, cursor):
            cursor.execute("SELECT pg_database_size(current_database())")
            return cursor.fetchone()[0]
        return self._open_and_call(callback)

    def get_current_tid(self, cursor, oid):
        """Returns the current integer tid for an object.

        oid is an integer.  Returns None if object does not exist.
        """
        cursor.execute("""
        SELECT tid
        FROM object_state
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
        FROM object_state
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
        stmt = """
        CREATE TEMPORARY TABLE temp_store (
            zoid        BIGINT NOT NULL,
            prev_tid    BIGINT NOT NULL,
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
            self._make_temp_table(cursor)
        except disconnected_exceptions, e:
            raise StorageError(e)

    def store_temp(self, cursor, oid, prev_tid, data):
        """Store an object in the temporary table."""
        stmt = """
        DELETE FROM temp_store WHERE zoid = %s;
        INSERT INTO temp_store (zoid, prev_tid, state)
        VALUES (%s, %s, decode(%s, 'base64'))
        """
        cursor.execute(stmt, (oid, oid, prev_tid, encodestring(data)))

    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table."""
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            state = decode(%s, 'base64')
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, encodestring(data), oid))

    def restore(self, cursor, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        stmt = """
        DELETE FROM object_state WHERE zoid = %s;
        INSERT INTO object_state (zoid, tid, state)
        VALUES (%s, %s, decode(%s, 'base64'))
        """
        if data is not None:
            data = encodestring(data)
        cursor.execute(stmt, (oid, oid, tid, data))

    def start_commit(self, cursor):
        """Prepare to commit."""
        # Hold commit_lock to prevent concurrent commits
        # (for as short a time as possible).
        # Lock object_state in share mode to ensure
        # conflict detection has the most current data.
        cursor.execute("""
        LOCK TABLE commit_lock IN EXCLUSIVE MODE;
        LOCK TABLE object_state IN SHARE MODE
        """)

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        cursor.execute("""
        SELECT tid, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
        FROM object_state
        ORDER BY tid DESC
        LIMIT 1
        """)
        if cursor.rowcount:
            return cursor.fetchone()
        else:
            return 0, 0

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        pass

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        stmt = """
        SELECT temp_store.zoid, object_state.tid, temp_store.prev_tid,
            encode(temp_store.state, 'base64')
        FROM temp_store
            JOIN object_state ON (temp_store.zoid = object_state.zoid)
        WHERE temp_store.prev_tid != object_state.tid
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
        DELETE FROM object_state
        WHERE zoid IN (SELECT zoid FROM temp_store);

        INSERT INTO object_state (zoid, tid, state)
        SELECT zoid, %s, state
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
        pass

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
        return '-'

    def commit_phase2(self, cursor, txn):
        """Final transaction commit."""
        cursor.connection.commit()

    def abort(self, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        cursor.connection.rollback()

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "SELECT NEXTVAL('zoid_seq')"
        cursor.execute(stmt)
        return cursor.fetchone()[0]

    def hold_pack_lock(self, cursor):
        """Try to acquire the garbage collection lock.

        Raise an exception if gc is already in progress.
        """
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_try_advisory_lock(1)")
            locked = cursor.fetchone()[0]
            if not locked:
                raise StorageError(
                    'A garbage collection operation is in progress')
        else:
            # b/w compat with PostgreSQL 8.1
            try:
                cursor.execute("LOCK gc_lock IN EXCLUSIVE MODE NOWAIT")
            except psycopg2.DatabaseError:
                raise StorageError(
                    'A garbage collection operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the garbage collection lock."""
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_advisory_unlock(1)")
        # else no action needed since the lock will be released at txn commit

    _poll_query = "EXECUTE get_latest_tid"
