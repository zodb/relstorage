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
"""Oracle adapter for RelStorage."""

import logging
import re
import thread
import time
import cx_Oracle
from ZODB.POSException import ConflictError, StorageError, UndoError

log = logging.getLogger("relstorage.oracle")


class OracleAdapter(object):
    """Oracle adapter for RelStorage."""

    def __init__(self, user, password, dsn, twophase=False, arraysize=64):
        self._params = (user, password, dsn)
        self._twophase = twophase
        self._arraysize = arraysize

    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
        CREATE TABLE commit_lock (dummy CHAR);

        -- The list of all transactions in the database
        CREATE TABLE transaction (
            tid         NUMBER(20) NOT NULL PRIMARY KEY,
            packed      CHAR DEFAULT 'N' CHECK (packed IN ('N', 'Y')),
            username    VARCHAR2(255) NOT NULL,
            description VARCHAR2(4000) NOT NULL,
            extension   RAW(2000)
        );

        -- Create a special transaction to represent object creation.  This
        -- row is often referenced by object_state.prev_tid, but never by
        -- object_state.tid.
        INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');

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

        -- Pointers to the current object state
        CREATE TABLE current_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state
        );

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
            to_zoid     NUMBER(20) NOT NULL
        );
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
            tid         NUMBER(20) NOT NULL PRIMARY KEY
        );

        -- Temporary state during packing:
        -- The list of objects to pack.  If keep is 'N',
        -- the object and all its revisions will be removed.
        -- If keep is 'Y', instead of removing the object,
        -- the pack operation will cut the object's history.
        -- If keep is 'Y' then the keep_tid field must also be set.
        -- The keep_tid field specifies which revision to keep within
        -- the list of packable transactions.
        CREATE TABLE pack_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
            keep_tid    NUMBER(20)
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);
        """
        self._run_script(cursor, stmt)


    def _run_script(self, cursor, script, params=()):
        """Execute a series of statements in the database."""
        lines = []
        for line in script.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            if line.endswith(';'):
                line = line[:-1]
                lines.append(line)
                stmt = '\n'.join(lines)
                try:
                    cursor.execute(stmt, params)
                except:
                    log.warning("script statement failed: %s", stmt)
                    raise
                lines = []
            else:
                lines.append(line)
        if lines:
            try:
                stmt = '\n'.join(lines)
                cursor.execute(stmt, params)
            except:
                log.warning("script statement failed: %s", stmt)
                raise


    def prepare_schema(self):
        """Create the database schema if it does not already exist."""
        conn, cursor = self.open()
        try:
            try:
                cursor.execute("""
                SELECT 1 FROM USER_TABLES WHERE TABLE_NAME = 'OBJECT_STATE'
                """)
                if not cursor.fetchall():
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
                DELETE FROM transaction;
                -- Create a transaction to represent object creation.
                INSERT INTO transaction (tid, username, description) VALUES
                    (0, 'system', 'special transaction for object creation');
                DROP SEQUENCE zoid_seq;
                CREATE SEQUENCE zoid_seq;
                """
                self._run_script(cursor, stmt)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED",
            twophase=False):
        """Open a database connection and return (conn, cursor)."""
        try:
            kw = {'twophase': twophase}  #, 'threaded': True}
            conn = cx_Oracle.connect(*self._params, **kw)
            cursor = conn.cursor()
            cursor.arraysize = self._arraysize
            if transaction_mode:
                cursor.execute("SET TRANSACTION %s" % transaction_mode)
            return conn, cursor

        except cx_Oracle.OperationalError:
            log.error("Unable to connect to DSN %s", self._params[2])
            raise

    def close(self, conn, cursor):
        """Close both a cursor and connection, ignoring certain errors."""
        for obj in (cursor, conn):
            if obj is not None:
                try:
                    obj.close()
                except (cx_Oracle.InterfaceError,
                        cx_Oracle.OperationalError):
                    pass

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open('READ ONLY')

    def restart_load(self, cursor):
        """After a rollback, reinitialize a connection for loading objects."""
        cursor.execute("SET TRANSACTION READ ONLY")

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # The tests expect an exact number, but the code below generates
        # an estimate, so this is disabled for now.
        if True:
            return 0
        else:
            conn, cursor = self.open('READ ONLY')
            try:
                cursor.execute("""
                SELECT NUM_ROWS
                FROM USER_TABLES
                WHERE TABLE_NAME = 'CURRENT_OBJECT'
                """)
                res = cursor.fetchone()[0]
                if res is None:
                    res = 0
                else:
                    res = int(res)
                return res
            finally:
                self.close(conn, cursor)

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        # May not be possible without access to the dba_* objects
        return 0

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        cursor.execute("""
        SELECT state, tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = :1
        """, (oid,))
        for state, tid in cursor:
            if state is not None:
                state = state.read()
            # else this object's creation has been undone
            return state, tid
        return None, None

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        cursor.execute("""
        SELECT state
        FROM object_state
        WHERE zoid = :1
            AND tid = :2
        """, (oid, tid))
        for (state,) in cursor:
            if state is not None:
                return state.read()
        return None

    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        cursor.execute("SELECT 1 FROM current_object WHERE zoid = :1", (oid,))
        return len(list(cursor))

    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        stmt = """
        SELECT state, tid
        FROM object_state
        WHERE zoid = :oid
            AND tid = (
                SELECT MAX(tid)
                FROM object_state
                WHERE zoid = :oid
                    AND tid < :tid
            )
        """
        cursor.execute(stmt, {'oid': oid, 'tid': tid})
        for state, tid in cursor:
            if state is not None:
                state = state.read()
            # else this object's creation has been undone
            return state, tid
        return None, None

    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT MIN(tid)
        FROM object_state
        WHERE zoid = :1
            AND tid > :2
        """
        cursor.execute(stmt, (oid, tid))
        rows = cursor.fetchall()
        if rows:
            assert len(rows) == 1
            return rows[0][0]
        else:
            return None

    def open_for_store(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        if self._twophase:
            conn, cursor = self.open(transaction_mode=None, twophase=True)
            try:
                stmt = """
                SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL
                """
                cursor.execute(stmt)
                xid = str(cursor.fetchone()[0])
                conn.begin(0, xid, '0')
            except:
                self.close(conn, cursor)
                raise
        else:
            conn, cursor = self.open()
        return conn, cursor

    def store_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Store an object in the temporary table."""
        cursor.setinputsizes(data=cx_Oracle.BLOB)
        stmt = """
        INSERT INTO temp_store (zoid, prev_tid, md5, state)
        VALUES (:oid, :prev_tid, :md5sum, :data)
        """
        cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
            md5sum=md5sum, data=cx_Oracle.Binary(data))

    def replace_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Replace an object in the temporary table."""
        cursor.setinputsizes(data=cx_Oracle.BLOB)
        stmt = """
        UPDATE temp_store SET
            prev_tid = :prev_tid,
            md5 = :md5sum,
            state = :data
        WHERE zoid = :oid
        """
        cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
            md5sum=md5sum, data=cx_Oracle.Binary(data))

    def start_commit(self, cursor):
        """Prepare to commit."""
        cursor.execute("SAVEPOINT start_commit")
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")

    def restart_commit(self, cursor):
        """Rollback the attempt to commit and start over."""
        cursor.execute("ROLLBACK TO SAVEPOINT start_commit")
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")

    def _parse_dsinterval(self, s):
        """Convert an Oracle dsinterval (as a string) to a float."""
        mo = re.match(r'([+-]\d+) (\d+):(\d+):([0-9.]+)', s)
        if not mo:
            raise ValueError(s)
        day, hour, min, sec = [float(v) for v in mo.groups()]
        return day * 86400 + hour * 3600 + min * 60 + sec

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        cursor.execute("""
        SELECT MAX(tid), TO_CHAR(TO_DSINTERVAL(SYSTIMESTAMP - TO_TIMESTAMP_TZ(
            '1970-01-01 00:00:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')))
        FROM transaction
        """)
        tid, now = cursor.fetchone()
        return tid, self._parse_dsinterval(now)

    def add_transaction(self, cursor, tid, username, description, extension):
        """Add a transaction.

        Raises ConflictError if the given tid has already been used.
        """
        try:
            stmt = """
            INSERT INTO transaction
                (tid, username, description, extension)
            VALUES (:1, :2, :3, :4)
            """
            cursor.execute(stmt, (
                tid, username or '-', description or '-',
                cx_Oracle.Binary(extension)))
        except cx_Oracle.IntegrityError:
            raise ConflictError

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        stmt = """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid,
            temp_store.state
        FROM temp_store
            JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        """
        cursor.execute(stmt)
        for oid, prev_tid, attempted_prev_tid, data in cursor:
            return oid, prev_tid, attempted_prev_tid, data.read()
        return None

    def move_from_temp(self, cursor, tid):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT zoid, :tid, prev_tid, md5, state
        FROM temp_store
        """
        cursor.execute(stmt, tid=tid)

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in cursor]

    def update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        # Insert objects created in this transaction into current_object.
        stmt = """
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = :1
            AND prev_tid = 0
        """
        cursor.execute(stmt, (tid,))

        # Change existing objects.
        stmt = """
        UPDATE current_object SET tid = :1
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = :1
                AND prev_tid != 0
        )
        """
        cursor.execute(stmt, (tid,))

    def commit_phase1(self, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        if self._twophase:
            cursor.connection.prepare()
        return '-'

    def commit_phase2(self, cursor, txn):
        """Final transaction commit."""
        cursor.connection.commit()

    def abort(self, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        cursor.connection.rollback()

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "SELECT zoid_seq.nextval FROM DUAL"
        cursor.execute(stmt)
        return cursor.fetchone()[0]


    def iter_transactions(self, cursor):
        """Iterate over the transaction log.

        Yields (tid, username, description, extension) for each transaction.
        """
        stmt = """
        SELECT tid, username, description, extension
        FROM transaction
        WHERE packed = 'N'
            AND tid != 0
        ORDER BY tid desc
        """
        cursor.execute(stmt)
        return iter(cursor)


    def iter_object_history(self, cursor, oid):
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """
        stmt = """
        SELECT 1 FROM current_object WHERE zoid = :1
        """
        cursor.execute(stmt, (oid,))
        if not cursor.fetchall():
            raise KeyError(oid)

        stmt = """
        SELECT tid, username, description, extension, LENGTH(state)
        FROM transaction
            JOIN object_state USING (tid)
        WHERE zoid = :1
            AND packed = 'N'
        ORDER BY tid desc
        """
        cursor.execute(stmt, (oid,))
        return iter(cursor)


    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        stmt = """
        LOCK TABLE pack_lock IN EXCLUSIVE MODE NOWAIT
        """
        try:
            cursor.execute(stmt)
        except cx_Oracle.DatabaseError:
            raise StorageError('A pack or undo operation is in progress')


    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        stmt = """
        SELECT 1 FROM transaction WHERE tid = :1 AND packed = 'N'
        """
        cursor.execute(stmt, (undo_tid,))
        if not cursor.fetchall():
            raise UndoError("Transaction not found or packed")

        self.hold_pack_lock(cursor)

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
        WHERE prev_os.tid = :1
            AND cur_os.md5 != prev_os.md5
        """
        cursor.execute(stmt, (undo_tid,))
        if cursor.fetchmany():
            raise UndoError(
                "Some data were modified by a later transaction")

        # Rule: don't allow the creation of the root object to
        # be undone.  It's hard to get it back.
        stmt = """
        SELECT 1
        FROM object_state
        WHERE tid = :1
            AND zoid = 0
            AND prev_tid = 0
        """
        cursor.execute(stmt, (undo_tid,))
        if cursor.fetchall():
            raise UndoError("Can't undo the creation of the root object")


    def undo(self, cursor, undo_tid, self_tid):
        """Undo a transaction.

        Parameters: "undo_tid", the integer tid of the transaction to undo,
        and "self_tid", the integer tid of the current transaction.

        Returns the list of OIDs undone.
        """
        # Update records produced by earlier undo operations
        # within this transaction.  Change the state, but not
        # prev_tid, since prev_tid is still correct.
        # Table names: 'undoing' refers to the transaction being
        # undone and 'prev' refers to the object state identified
        # by undoing.prev_tid.
        stmt = """
        UPDATE object_state SET (md5, state) = (
            SELECT prev.md5, prev.state
            FROM object_state undoing
                LEFT JOIN object_state prev
                ON (prev.zoid = undoing.zoid
                    AND prev.tid = undoing.prev_tid)
            WHERE undoing.tid = :undo_tid
                AND undoing.zoid = object_state.zoid
        )
        WHERE tid = :self_tid
            AND zoid IN (
                SELECT zoid FROM object_state WHERE tid = :undo_tid);

        -- Add new undo records.

        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT undoing.zoid, :self_tid, current_object.tid,
            prev.md5, prev.state
        FROM object_state undoing
            JOIN current_object ON (current_object.zoid = undoing.zoid)
            LEFT JOIN object_state prev
                ON (prev.zoid = undoing.zoid
                    AND prev.tid = undoing.prev_tid)
        WHERE undoing.tid = :undo_tid
            AND undoing.zoid NOT IN (
                SELECT zoid FROM object_state WHERE tid = :self_tid);
        """
        self._run_script(cursor, stmt,
            {'undo_tid': undo_tid, 'self_tid': self_tid})

        # List the changed OIDs.
        stmt = "SELECT zoid FROM object_state WHERE tid = :1"
        cursor.execute(stmt, (undo_tid,))
        return [oid_int for (oid_int,) in cursor]


    def choose_pack_transaction(self, cursor, pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """
        stmt = """
        SELECT MAX(tid)
        FROM transaction
        WHERE tid > 0
            AND tid <= :1
            AND packed = 'N'
        """
        cursor.execute(stmt, (pack_point,))
        rows = cursor.fetchall()
        if not rows:
            # Nothing needs to be packed.
            return None
        return rows[0][0]


    def pre_pack(self, cursor, pack_tid, get_references):
        """Decide what to pack.

        tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.
        """
        # Fill object_ref with references from object states
        # in transactions that will not be packed.
        self._fill_nonpacked_refs(cursor, pack_tid, get_references)

        args = {'pack_tid': pack_tid}

        # Ensure the temporary pack_object table is clear.
        cursor.execute("DELETE FROM pack_object")

        # Fill the pack_object table with OIDs that either will be
        # removed (if nothing references the OID) or whose history will
        # be cut.
        stmt = """
        INSERT INTO pack_object (zoid, keep)
        SELECT DISTINCT zoid, 'N'
        FROM object_state
        WHERE tid <= :pack_tid
        """
        cursor.execute(stmt, args)

        # If the root object is in pack_object, keep it.
        stmt = """
        UPDATE pack_object SET keep = 'Y'
        WHERE zoid = 0
        """
        cursor.execute(stmt)

        # Keep objects that have been revised since pack_tid.
        stmt = """
        UPDATE pack_object SET keep = 'Y'
        WHERE keep = 'N'
            AND zoid IN (
                SELECT zoid
                FROM current_object
                WHERE tid > :pack_tid
            )
        """
        cursor.execute(stmt, args)

        # Keep objects that are still referenced by object states in
        # transactions that will not be packed.
        stmt = """
        UPDATE pack_object SET keep = 'Y'
        WHERE keep = 'N'
            AND zoid IN (
                SELECT to_zoid
                FROM object_ref
                WHERE tid > :pack_tid
            )
        """
        cursor.execute(stmt, args)

        # Each of the packable objects to be kept might
        # refer to other objects.  If some of those references
        # include objects currently set to be removed, keep
        # those objects as well.  Do this
        # repeatedly until all references have been satisfied.
        while True:

            # Set keep_tid for all pack_object rows with keep = 'Y'.
            # This must be done before _fill_pack_object_refs examines
            # references.
            stmt = """
            UPDATE pack_object SET keep_tid = (
                    SELECT MAX(tid)
                    FROM object_state
                    WHERE zoid = pack_object.zoid
                        AND tid > 0
                        AND tid <= :pack_tid
                )
            WHERE keep = 'Y' AND keep_tid IS NULL
            """
            cursor.execute(stmt, args)

            self._fill_pack_object_refs(cursor, get_references)

            stmt = """
            UPDATE pack_object SET keep = 'Y'
            WHERE keep = 'N'
                AND zoid IN (
                    SELECT DISTINCT to_zoid
                    FROM object_ref
                        JOIN pack_object parent ON (
                            object_ref.zoid = parent.zoid)
                    WHERE parent.keep = 'Y'
                )
            """
            cursor.execute(stmt)
            if not cursor.rowcount:
                # No new references detected.
                break


    def _fill_nonpacked_refs(self, cursor, pack_tid, get_references):
        """Fill object_ref for all transactions that will not be packed."""
        stmt = """
        SELECT DISTINCT tid
        FROM object_state
        WHERE tid > :1
            AND NOT EXISTS (
                SELECT 1
                FROM object_refs_added
                WHERE tid = object_state.tid
            )
        """
        cursor.execute(stmt, (pack_tid,))
        for (tid,) in cursor.fetchall():
            self._add_refs_for_tid(cursor, tid, get_references)


    def _fill_pack_object_refs(self, cursor, get_references):
        """Fill object_ref for all pack_object rows that have keep_tid."""
        stmt = """
        SELECT DISTINCT keep_tid
        FROM pack_object
        WHERE keep_tid IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM object_refs_added
                WHERE tid = keep_tid
            )
        """
        cursor.execute(stmt)
        for (tid,) in cursor.fetchall():
            self._add_refs_for_tid(cursor, tid, get_references)


    def _add_refs_for_tid(self, cursor, tid, get_references):
        """Fills object_refs with all states for a transaction.
        """
        stmt = """
        SELECT zoid, state
        FROM object_state
        WHERE tid = :1
        """
        cursor.execute(stmt, (tid,))

        to_add = []  # [(from_oid, tid, to_oid)]
        for from_oid, state_file in cursor:
            if state_file is not None:
                state = state_file.read()
                if state is not None:
                    to_oids = get_references(state)
                    for to_oid in to_oids:
                        to_add.append((from_oid, tid, to_oid))

        if to_add:
            stmt = """
            INSERT INTO object_ref (zoid, tid, to_zoid)
            VALUES (:1, :2, :3)
            """
            cursor.executemany(stmt, to_add)

        # The references have been computed for this transaction.
        stmt = """
        INSERT INTO object_refs_added (tid)
        VALUES (:1)
        """
        cursor.execute(stmt, (tid,))


    def pack(self, pack_tid):
        """Pack.  Requires populated pack tables."""

        # Read committed mode is sufficient.
        conn, cursor = self.open()
        try:
            try:

                for table in ('object_ref', 'current_object', 'object_state'):

                    # Remove objects that are in pack_object and have keep
                    # set to 'N'.
                    stmt = """
                    DELETE FROM %s
                    WHERE zoid IN (
                            SELECT zoid
                            FROM pack_object
                            WHERE keep = 'N'
                        )
                    """ % table
                    cursor.execute(stmt)

                    if table != 'current_object':
                        # Cut the history of objects in pack_object that
                        # have keep set to 'Y'.
                        stmt = """
                        DELETE FROM %s
                        WHERE zoid IN (
                                SELECT zoid
                                FROM pack_object
                                WHERE keep = 'Y'
                            )
                            AND tid < (
                                SELECT keep_tid
                                FROM pack_object
                                WHERE zoid = %s.zoid
                            )
                        """ % (table, table)
                        cursor.execute(stmt)

                stmt = """
                -- Terminate prev_tid chains
                UPDATE object_state SET prev_tid = 0
                WHERE tid <= :tid
                    AND prev_tid != 0;

                -- For each tid to be removed, delete the corresponding row in
                -- object_refs_added.
                DELETE FROM object_refs_added
                WHERE tid > 0
                    AND tid <= :tid
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = object_refs_added.tid
                    );

                -- Delete transactions no longer used.
                DELETE FROM transaction
                WHERE tid > 0
                    AND tid <= :tid
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = transaction.tid
                    );

                -- Mark the remaining packable transactions as packed
                UPDATE transaction SET packed = 'Y'
                WHERE tid > 0
                    AND tid <= :tid
                    AND packed = 'N'
                """
                self._run_script(cursor, stmt, {'tid': pack_tid})

                # Clean up
                cursor.execute("DELETE FROM pack_object")

            except:
                conn.rollback()
                raise

            else:
                conn.commit()

        finally:
            self.close(conn, cursor)


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
            stmt = "SELECT MAX(tid) FROM transaction"
            cursor.execute(stmt)
            new_polled_tid = list(cursor)[0][0]

            if prev_polled_tid is None:
                # This is the first time the connection has polled.
                return None, new_polled_tid

            if new_polled_tid == prev_polled_tid:
                # No transactions have been committed since prev_polled_tid.
                return (), new_polled_tid

            stmt = "SELECT 1 FROM transaction WHERE tid = :1"
            cursor.execute(stmt, (prev_polled_tid,))
            rows = cursor.fetchall()
            if not rows:
                # Transaction not found; perhaps it has been packed.
                # The connection cache needs to be cleared.
                return None, new_polled_tid

            # Get the list of changed OIDs and return it.
            stmt = """
            SELECT DISTINCT zoid
            FROM object_state
                JOIN transaction USING (tid)
            WHERE tid > :1
            """
            if ignore_tid is not None:
                stmt += " AND tid != %d" % ignore_tid
            cursor.execute(stmt, (prev_polled_tid,))
            oids = [oid for (oid,) in cursor]

            return oids, new_polled_tid

        except (cx_Oracle.OperationalError, cx_Oracle.InterfaceError):
            raise StorageError("database disconnected")

