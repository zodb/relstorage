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
from ZODB.POSException import StorageError

from common import Adapter

log = logging.getLogger("relstorage.adapters.mysql")

commit_lock_timeout = 30


class MySQLAdapter(Adapter):
    """MySQL adapter for RelStorage."""

    _scripts = Adapter._scripts.copy()
    # Work around a MySQL performance bug by avoiding an expensive subquery.
    # See: http://mail.zope.org/pipermail/zodb-dev/2008-May/011880.html
    #      http://bugs.mysql.com/bug.php?id=28257
    _scripts.update({
        'create_temp_pack_visit': """
            CREATE TEMPORARY TABLE temp_pack_visit (
                zoid BIGINT NOT NULL,
                keep_tid BIGINT
            );
            CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid);
            CREATE TEMPORARY TABLE temp_pack_child (
                zoid BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX temp_pack_child_zoid ON temp_pack_child (zoid);
            """,

        # Note: UPDATE must be the last statement in the script
        # because it returns a value.
        'prepack_follow_child_refs': """
            %(TRUNCATE)s temp_pack_child;

            INSERT INTO temp_pack_child
            SELECT DISTINCT to_zoid
            FROM object_ref
                JOIN temp_pack_visit USING (zoid)
            WHERE object_ref.tid >= temp_pack_visit.keep_tid;

            -- MySQL-specific syntax for table join in update
            UPDATE pack_object, temp_pack_child SET keep = %(TRUE)s
            WHERE keep = %(FALSE)s
                AND pack_object.zoid = temp_pack_child.zoid;
            """,

        # MySQL optimizes deletion far better when using a join syntax.
        'pack_current_object': """
            DELETE FROM current_object
            USING current_object
                JOIN pack_state USING (zoid, tid)
            WHERE current_object.tid = %(tid)s
            """,

        'pack_object_state': """
            DELETE FROM object_state
            USING object_state
                JOIN pack_state USING (zoid, tid)
            WHERE object_state.tid = %(tid)s
            """,

        'pack_object_ref': """
            DELETE FROM object_refs_added
            USING object_refs_added
                JOIN transaction USING (tid)
            WHERE transaction.empty = true;

            DELETE FROM object_ref
            USING object_ref
                JOIN transaction USING (tid)
            WHERE transaction.empty = true
            """,
        })

    def __init__(self, **params):
        self._params = params.copy()

    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
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


    def zap_all(self):
        """Clear all data out of the database."""
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


    def drop_all(self):
        """Drop all tables and sequences."""
        conn, cursor = self.open()
        try:
            try:
                for tablename in ('pack_state_tid', 'pack_state',
                        'pack_object', 'object_refs_added', 'object_ref',
                        'current_object', 'object_state', 'new_oid',
                        'transaction'):
                    cursor.execute("DROP TABLE IF EXISTS %s" % tablename)
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
        except MySQLdb.OperationalError, e:
            log.warning("Unable to connect: %s", e)
            raise

    def close(self, conn, cursor):
        """Close a connection and cursor, ignoring certain errors.
        """
        for obj in (cursor, conn):
            if obj is not None:
                try:
                    obj.close()
                except (MySQLdb.InterfaceError,
                        MySQLdb.OperationalError,
                        MySQLdb.ProgrammingError):
                    pass

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open("ISOLATION LEVEL REPEATABLE READ")

    def restart_load(self, cursor):
        """Reinitialize a connection for loading objects."""
        try:
            cursor.connection.rollback()
        except (MySQLdb.OperationalError, MySQLdb.InterfaceError), e:
            raise StorageError(e)

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # do later
        return 0

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        conn, cursor = self.open()
        try:
            cursor.execute("SHOW TABLE STATUS")
            description = [i[0] for i in cursor.description]
            rows = list(cursor)
        finally:
            self.close(conn, cursor)
        data_column = description.index('Data_length')
        index_column = description.index('Index_length')
        return sum([row[data_column] + row[index_column] for row in rows], 0)

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

    def _restart_temp_table(self, cursor):
        """Restart the temporary table for storing objects"""
        stmt = """
        DROP TEMPORARY TABLE IF EXISTS temp_store
        """
        cursor.execute(stmt)
        self._make_temp_table(cursor)

    def restart_store(self, cursor):
        """Reuse a store connection."""
        try:
            cursor.connection.rollback()
            self._restart_temp_table(cursor)
        except (MySQLdb.OperationalError, MySQLdb.InterfaceError), e:
            raise StorageError(e)

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

    def restore(self, cursor, oid, tid, md5sum, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (%s, %s,
            COALESCE((SELECT tid FROM current_object WHERE zoid = %s), 0),
            %s, %s)
        """
        if data is not None:
            data = MySQLdb.Binary(data)
        cursor.execute(stmt, (oid, tid, oid, md5sum, data))

    def start_commit(self, cursor):
        """Prepare to commit."""
        self._hold_commit_lock(cursor)

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

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        stmt = """
        INSERT INTO transaction
            (tid, packed, username, description, extension)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(stmt, (
            tid, packed, MySQLdb.Binary(username),
            MySQLdb.Binary(description), MySQLdb.Binary(extension)))

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

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        cursor.execute("REPLACE INTO new_oid VALUES(%s)", (oid,))

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
        self._release_commit_lock(cursor)

    def abort(self, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        cursor.connection.rollback()
        self._release_commit_lock(cursor)

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
        stmt = "SELECT GET_LOCK(CONCAT(DATABASE(), '.pack'), 0)"
        cursor.execute(stmt)
        res = cursor.fetchone()[0]
        if not res:
            raise StorageError('A pack or undo operation is in progress')


    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        stmt = "SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.pack'))"
        cursor.execute(stmt)


    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).

        This overrides the method by the same name in common.Adapter.
        """
        conn, cursor = self.open(transaction_mode=None)
        try:
            # This phase of packing works best with transactions
            # disabled.  It changes no user-facing data.
            conn.autocommit(True)
            return conn, cursor
        except:
            self.close(conn, cursor)
            raise


    def _hold_commit_lock(self, cursor):
        """Hold the commit lock.

        This overrides the method by the same name in common.Adapter.
        """
        cursor.execute("SELECT GET_LOCK(CONCAT(DATABASE(), '.commit'), %s)",
            (commit_lock_timeout,))
        locked = cursor.fetchone()[0]
        if not locked:
            raise StorageError("Unable to acquire commit lock")


    def _release_commit_lock(self, cursor):
        """Release the commit lock.

        This overrides the method by the same name in common.Adapter.
        """
        cursor.execute("SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.commit'))")


    _poll_query = "SELECT tid FROM transaction ORDER BY tid DESC LIMIT 1"
