##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""MySQL adapter for RelStorage.

Connection parameters supported by MySQLdb:

host
    string, host to connect
user
    string, user to connect as
passwd
    string, password to use
db
    string, database to use
port
    integer, TCP/IP port to connect to
unix_socket
    string, location of unix_socket (UNIX-ish only)
conv
    mapping, maps MySQL FIELD_TYPE.* to Python functions which convert a
    string to the appropriate Python type
connect_timeout
    number of seconds to wait before the connection attempt fails.
compress
    if set, gzip compression is enabled
named_pipe
    if set, connect to server via named pipe (Windows only)
init_command
    command which is run once the connection is created
read_default_file
    see the MySQL documentation for mysql_options()
read_default_group
    see the MySQL documentation for mysql_options()
client_flag
    client flags from MySQLdb.constants.CLIENT
load_infile
    int, non-zero enables LOAD LOCAL INFILE, zero disables
"""

import logging
import MySQLdb
import time
from ZODB.POSException import ConflictError, StorageError, UndoError

from common import Adapter

log = logging.getLogger("relstorage.adapters.mysql")

commit_lock_timeout = 30


class MySQLAdapter(Adapter):
    """MySQL adapter for RelStorage."""

    def __init__(self, **params):
        self._params = params

    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
        -- The list of all transactions in the database
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            username    VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
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
            md5         CHAR(32),
            state       LONGBLOB,
            CHECK (tid > 0)
        ) ENGINE = InnoDB;
        CREATE INDEX object_state_tid ON object_state (tid);

        -- Pointers to the current object state
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state (zoid, tid)
        ) ENGINE = InnoDB;

        -- A list of referenced OIDs from each object_state.
        -- This table is populated as needed during packing.
        -- To prevent unnecessary table locking, it does not use
        -- foreign keys, which is safe because rows in object_state
        -- are never modified once committed, and rows are removed
        -- from object_state only by packing.
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL
        ) ENGINE = MyISAM;
        CREATE INDEX object_ref_from ON object_ref (zoid);
        CREATE INDEX object_ref_tid ON object_ref (tid);
        CREATE INDEX object_ref_to ON object_ref (to_zoid);

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
        -- The list of objects to pack.  If keep is 'N',
        -- the object and all its revisions will be removed.
        -- If keep is 'Y', instead of removing the object,
        -- the pack operation will cut the object's history.
        -- If keep is 'Y' then the keep_tid field must also be set.
        -- The keep_tid field specifies which revision to keep within
        -- the list of packable transactions.
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT
        ) ENGINE = MyISAM;
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);
        """
        self._run_script(cursor, stmt)


    def prepare_schema(self):
        """Create the database schema if it does not already exist."""
        conn, cursor = self.open()
        try:
            try:
                cursor.execute("SHOW TABLES LIKE 'object_state'")
                if not cursor.rowcount:
                    self.create_schema(cursor)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def zap(self):
        """Clear all data out of the database.

        Used by the test suite.
        """
        conn, cursor = self.open()
        try:
            try:
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
                self._run_script(cursor, stmt)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED"):
        """Open a database connection and return (conn, cursor)."""
        try:
            conn = MySQLdb.connect(**self._params)
            cursor = conn.cursor()
            cursor.arraysize = 64
            if transaction_mode:
                conn.autocommit(True)
                cursor.execute("SET SESSION TRANSACTION %s" % transaction_mode)
                conn.autocommit(False)
            return conn, cursor
        except MySQLdb.OperationalError:
            log.warning("Unable to connect in %s", repr(self))
            raise

    def close(self, conn, cursor):
        """Close a connection and cursor, ignoring certain errors.
        """
        for obj in (cursor, conn):
            if obj is not None:
                try:
                    obj.close()
                except (MySQLdb.InterfaceError,
                        MySQLdb.OperationalError):
                    pass

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open("ISOLATION LEVEL REPEATABLE READ")

    def restart_load(self, cursor):
        """After a rollback, reinitialize a connection for loading objects."""
        # No re-init necessary
        pass

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # do later
        return 0

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        # do later
        return 0

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        cursor.execute("""
        SELECT state, tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = %s
        """, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()
        else:
            return None, None

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        cursor.execute("""
        SELECT state
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state,) = cursor.fetchone()
            return state
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
        SELECT state, tid
        FROM object_state
        WHERE zoid = %s
            AND tid < %s
        ORDER BY tid DESC
        LIMIT 1
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()
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
            zoid        BIGINT NOT NULL PRIMARY KEY,
            prev_tid    BIGINT NOT NULL,
            md5         CHAR(32),
            state       LONGBLOB
        ) ENGINE MyISAM
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
            cursor.execute("TRUNCATE temp_store")
        except (MySQLdb.OperationalError, MySQLdb.InterfaceError):
            raise StorageError("database disconnected")

    def store_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Store an object in the temporary table."""
        stmt = """
        INSERT INTO temp_store (zoid, prev_tid, md5, state)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(stmt, (oid, prev_tid, md5sum, MySQLdb.Binary(data)))

    def replace_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Replace an object in the temporary table."""
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = %s
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, MySQLdb.Binary(data), oid))

    def start_commit(self, cursor):
        """Prepare to commit."""
        # Hold commit_lock to prevent concurrent commits.
        cursor.execute("SELECT GET_LOCK('relstorage.commit', %s)",
            (commit_lock_timeout,))
        locked = cursor.fetchone()[0]
        if not locked:
            raise StorageError("Unable to acquire commit lock")

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        # Lock in share mode to ensure the data being read is up to date.
        cursor.execute("""
        SELECT tid, UNIX_TIMESTAMP()
        FROM transaction
        ORDER BY tid DESC
        LIMIT 1
        LOCK IN SHARE MODE
        """)
        assert cursor.rowcount == 1
        tid, timestamp = cursor.fetchone()
        # MySQL does not provide timestamps with more than one second
        # precision.  To provide more precision, if the system time is
        # within one minute of the MySQL time, use the system time instead.
        now = time.time()
        if abs(now - timestamp) <= 60.0:
            timestamp = now
        return tid, timestamp

    def add_transaction(self, cursor, tid, username, description, extension):
        """Add a transaction.

        Raises ConflictError if the given tid has already been used.
        """
        stmt = """
        INSERT INTO transaction
            (tid, username, description, extension)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(stmt, (
            tid, username, description, MySQLdb.Binary(extension)))

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        # Lock in share mode to ensure the data being read is up to date.
        stmt = """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid,
            temp_store.state
        FROM temp_store
            JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        LIMIT 1
        LOCK IN SHARE MODE
        """
        cursor.execute(stmt)
        if cursor.rowcount:
            return cursor.fetchone()
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
        REPLACE INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %s
        """, (tid,))

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
        cursor.execute("SELECT RELEASE_LOCK('relstorage.commit')")

    def abort(self, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        cursor.connection.rollback()
        cursor.execute("SELECT RELEASE_LOCK('relstorage.commit')")

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "INSERT INTO new_oid VALUES ()"
        cursor.execute(stmt)
        oid = cursor.connection.insert_id()
        if oid % 100 == 0:
            # Clean out previously generated OIDs.
            stmt = "DELETE FROM new_oid WHERE zoid < %s"
            cursor.execute(stmt, (oid,))
        return oid


    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        stmt = "SELECT GET_LOCK('relstorage.pack', 0)"
        cursor.execute(stmt)
        res = cursor.fetchone()[0]
        if not res:
            raise StorageError('A pack or undo operation is in progress')


    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        stmt = "SELECT RELEASE_LOCK('relstorage.pack')"
        cursor.execute(stmt)


    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        stmt = """
        SELECT 1 FROM transaction WHERE tid = %s AND packed = FALSE
        """
        cursor.execute(stmt, (undo_tid,))
        if not cursor.rowcount:
            raise UndoError("Transaction not found or packed")

        # Rule: we can undo an object if the object's state in the
        # transaction to undo matches the object's current state.
        # If any object in the transaction does not fit that rule,
        # refuse to undo.
        stmt = """
        SELECT prev_os.zoid, current_object.tid
        FROM object_state prev_os
            JOIN object_state cur_os ON (prev_os.zoid = cur_os.zoid)
            JOIN current_object ON (cur_os.zoid = current_object.zoid
                AND cur_os.tid = current_object.tid)
        WHERE prev_os.tid = %s
            AND cur_os.md5 != prev_os.md5
        LIMIT 1
        """
        cursor.execute(stmt, (undo_tid,))
        if cursor.rowcount:
            raise UndoError(
                "Some data were modified by a later transaction")

        # Rule: don't allow the creation of the root object to
        # be undone.  It's hard to get it back.
        stmt = """
        SELECT 1
        FROM object_state
        WHERE tid = %s
            AND zoid = 0
            AND prev_tid = 0
        """
        cursor.execute(stmt, (undo_tid,))
        if cursor.rowcount:
            raise UndoError("Can't undo the creation of the root object")


    def undo(self, cursor, undo_tid, self_tid):
        """Undo a transaction.

        Parameters: "undo_tid", the integer tid of the transaction to undo,
        and "self_tid", the integer tid of the current transaction.

        Returns the list of OIDs undone.
        """
        stmt = """
        CREATE TEMPORARY TABLE temp_undo_state (
            zoid BIGINT NOT NULL PRIMARY KEY,
            md5 CHAR(32),
            state LONGBLOB
        );

        -- Copy the states to revert to into temp_undo_state.
        -- Some of the states can be null, indicating object uncreation.
        INSERT INTO temp_undo_state
        SELECT undoing.zoid, prev.md5, prev.state
        FROM object_state undoing
            LEFT JOIN object_state prev
                ON (prev.zoid = undoing.zoid
                    AND prev.tid = undoing.prev_tid)
        WHERE undoing.tid = %(undo_tid)s;

        -- Update records produced by earlier undo operations
        -- within this transaction.  Change the state, but not
        -- prev_tid, since prev_tid is still correct.
        UPDATE object_state
        JOIN temp_undo_state USING (zoid)
        SET object_state.md5 = temp_undo_state.md5,
            object_state.state = temp_undo_state.state
        WHERE tid = %(self_tid)s;

        -- Add new undo records.
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT zoid, %(self_tid)s, tid, md5, state
        FROM temp_undo_state
            JOIN current_object USING (zoid)
        WHERE zoid NOT IN (
            SELECT zoid FROM object_state WHERE tid = %(self_tid)s);

        DROP TABLE temp_undo_state;
        """
        self._run_script(cursor, stmt,
            {'undo_tid': undo_tid, 'self_tid': self_tid})

        # List the changed OIDs.
        stmt = "SELECT zoid FROM object_state WHERE tid = %s"
        cursor.execute(stmt, (undo_tid,))
        return [oid_int for (oid_int,) in cursor]


    def pre_pack(self, pack_tid, get_references, gc=True):
        """Decide what to pack.

        This overrides the method by the same name in common.Adapter.
        """
        conn, cursor = self.open(transaction_mode=None)
        try:
            # This phase of packing works best with transactions
            # disabled.  It changes no user-facing data.
            conn.autocommit(True)
            if gc:
                self._pre_pack_with_gc(cursor, pack_tid, get_references)
            else:
                self._pre_pack_without_gc(cursor, pack_tid)
        finally:
            self.close(conn, cursor)


    def _create_temp_pack_visit(self, cursor):
        """Create a workspace for listing objects to visit.

        This overrides the method by the same name in common.Adapter.
        """
        stmt = """
        CREATE TEMPORARY TABLE temp_pack_visit (
            zoid BIGINT NOT NULL PRIMARY KEY
        )
        """
        cursor.execute(stmt)


    def _hold_commit_lock(self, cursor):
        """Hold the commit lock for packing.

        This overrides the method by the same name in common.Adapter.
        """
        cursor.execute("SELECT GET_LOCK('relstorage.commit', %s)",
            (commit_lock_timeout,))
        locked = cursor.fetchone()[0]
        if not locked:
            raise StorageError("Unable to acquire commit lock")


    def poll_invalidations(self, conn, cursor, prev_polled_tid, ignore_tid):
        """Polls for new transactions.

        conn and cursor must have been created previously by open_for_load().
        prev_polled_tid is the tid returned at the last poll, or None
        if this is the first poll.  If ignore_tid is not None, changes
        committed in that transaction will not be included in the list
        of changed OIDs.

        Returns (changed_oids, new_polled_tid).  Raises StorageError
        if the database has disconnected.
        """
        try:
            # find out the tid of the most recent transaction.
            stmt = "SELECT tid FROM transaction ORDER BY tid DESC LIMIT 1"
            cursor.execute(stmt)
            # Expect the transaction table to always have at least one row.
            assert cursor.rowcount == 1
            new_polled_tid = cursor.fetchone()[0]

            if prev_polled_tid is None:
                # This is the first time the connection has polled.
                return None, new_polled_tid

            if new_polled_tid == prev_polled_tid:
                # No transactions have been committed since prev_polled_tid.
                return (), new_polled_tid

            stmt = "SELECT 1 FROM transaction WHERE tid = %s"
            cursor.execute(stmt, (prev_polled_tid,))
            if not cursor.rowcount:
                # Transaction not found; perhaps it has been packed.
                # The connection cache needs to be cleared.
                return None, new_polled_tid

            # Get the list of changed OIDs and return it.
            stmt = """
            SELECT DISTINCT zoid
            FROM object_state
                JOIN transaction USING (tid)
            WHERE tid > %s
            """
            if ignore_tid is not None:
                stmt += " AND tid != %d" % ignore_tid
            cursor.execute(stmt, (prev_polled_tid,))
            oids = [oid for (oid,) in cursor]

            return oids, new_polled_tid

        except (MySQLdb.OperationalError, MySQLdb.InterfaceError):
            raise StorageError("database disconnected")

