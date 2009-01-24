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
import cx_Oracle
from ZODB.POSException import StorageError

from common import Adapter

log = logging.getLogger("relstorage.adapters.oracle")

def lob_handler(cursor, name, defaultType, size, precision, scale):
    """cx_Oracle outputtypehandler that causes Oracle to send BLOBs inline.

    Note that if a BLOB in the result is too large, Oracle generates an
    error indicating truncation.  The execute_lob_stmt() method works
    around this.
    """
    if defaultType == cx_Oracle.BLOB:
        # Default size for BLOB is 4, we want the whole blob inline.
        # Typical chunk size is 8132, we choose a multiple - 32528
        return cursor.var(cx_Oracle.LONG_BINARY, 32528, cursor.arraysize)

def read_lob(value):
    """Handle an Oracle LOB by returning its byte stream.

    Returns other objects unchanged.
    """
    if isinstance(value, cx_Oracle.LOB):
        return value.read()
    return value


class OracleAdapter(Adapter):
    """Oracle adapter for RelStorage."""

    _script_vars = {
        'TRUE':         "'Y'",
        'FALSE':        "'N'",
        'OCTET_LENGTH': 'LENGTH',
        'TRUNCATE':     'TRUNCATE TABLE',
        'oid':          ':oid',
        'tid':          ':tid',
        'pack_tid':     ':pack_tid',
        'undo_tid':     ':undo_tid',
        'self_tid':     ':self_tid',
        'min_tid':      ':min_tid',
        'max_tid':      ':max_tid',
    }

    _scripts = Adapter._scripts.copy()
    _scripts.update({
        'choose_pack_transaction': """
            SELECT MAX(tid)
            FROM transaction
            WHERE tid > 0
                AND tid <= %(tid)s
                AND packed = 'N'
            """,

        'create_temp_pack_visit': None,
        'create_temp_undo': None,
        'reset_temp_undo': "DELETE FROM temp_undo",

        'transaction_has_data': """
            SELECT DISTINCT tid
            FROM object_state
            WHERE tid = %(tid)s
            """,
    })

    def __init__(self, user, password, dsn, twophase=False, arraysize=64,
            use_inline_lobs=None):
        """Create an Oracle adapter.

        The user, password, and dsn parameters are provided to cx_Oracle
        at connection time.

        If twophase is true, all commits go through an Oracle-level two-phase
        commit process.  This is disabled by default.  Even when this option
        is disabled, the ZODB two-phase commit is still in effect.

        arraysize sets the number of rows to buffer in cx_Oracle.  The default
        is 64.

        use_inline_lobs enables Oracle to send BLOBs inline in response to
        queries.  It depends on features in cx_Oracle 5.  The default is None,
        telling the adapter to auto-detect the presence of cx_Oracle 5.
        """
        self._params = (user, password, dsn)
        self._twophase = bool(twophase)
        self._arraysize = arraysize
        if use_inline_lobs is None:
            use_inline_lobs = (cx_Oracle.version >= '5.0')
        self._use_inline_lobs = bool(use_inline_lobs)

    def _run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        This overrides the method by the same name in common.Adapter.
        """
        if generic_params:
            # Oracle raises ORA-01036 if the parameter map contains extra keys,
            # so filter out any unused parameters.
            tracker = TrackingMap(self._script_vars)
            stmt = generic_stmt % tracker
            used = tracker.used
            params = {}
            for k, v in generic_params.iteritems():
                if k in used:
                    params[k] = v
        else:
            stmt = generic_stmt % self._script_vars
            params = ()

        try:
            cursor.execute(stmt, params)
        except:
            log.warning("script statement failed: %r; parameters: %r",
                stmt, params)
            raise


    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
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
        self._run_script(cursor, stmt)


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
                DELETE FROM transaction;
                -- Create a transaction to represent object creation.
                INSERT INTO transaction (tid, username, description) VALUES
                    (0, UTL_I18N.STRING_TO_RAW('system', 'US7ASCII'),
                    UTL_I18N.STRING_TO_RAW(
                    'special transaction for object creation', 'US7ASCII'));
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

        except cx_Oracle.OperationalError, e:
            log.warning("Unable to connect: %s", e)
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
        """Reinitialize a connection for loading objects."""
        try:
            cursor.connection.rollback()
            cursor.execute("SET TRANSACTION READ ONLY")
        except (cx_Oracle.OperationalError, cx_Oracle.InterfaceError), e:
            raise StorageError(e)

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

    def get_current_tid(self, cursor, oid):
        """Returns the current integer tid for an object.

        oid is an integer.  Returns None if object does not exist.
        """
        cursor.execute("""
        SELECT tid
        FROM current_object
        WHERE zoid = :1
        """, (oid,))
        for (tid,) in cursor:
            return tid
        return None

    def execute_lob_stmt(self, cursor, stmt, args=(), default=None):
        """Execute a statement and return one row with all LOBs inline.

        Returns the value of the default parameter if the result was empty.
        """
        if self._use_inline_lobs:
            try:
                cursor.outputtypehandler = lob_handler
                try:
                    cursor.execute(stmt, args)
                    for row in cursor:
                        return row
                finally:
                    del cursor.outputtypehandler
            except cx_Oracle.DatabaseError, e:
                # ORA-01406: fetched column value was truncated
                error, = e
                if ((isinstance(error, str) and not error.endswith(' 1406'))
                        or error.code != 1406):
                    raise
                # Execute the query, but alter it slightly without
                # changing its meaning, so that the query cache
                # will see it as a statement that has to be compiled
                # with different output type parameters.
                cursor.execute(stmt + ' ', args)
                for row in cursor:
                    return tuple(map(read_lob, row))
        else:
            cursor.execute(stmt, args)
            for row in cursor:
                return tuple(map(read_lob, row))
        return default

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        stmt = """
        SELECT state, tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = :1
        """
        return self.execute_lob_stmt(
            cursor, stmt, (oid,), default=(None, None))

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = """
        SELECT state
        FROM object_state
        WHERE zoid = :1
            AND tid = :2
        """
        (state,) = self.execute_lob_stmt(
            cursor, stmt, (oid, tid), default=(None,))
        return state

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
        return self.execute_lob_stmt(cursor, stmt, {'oid': oid, 'tid': tid},
             default=(None, None))

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

    def _set_xid(self, cursor):
        """Set up a distributed transaction"""
        stmt = """
        SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL
        """
        cursor.execute(stmt)
        xid = str(cursor.fetchone()[0])
        cursor.connection.begin(0, xid, '0')

    def open_for_store(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        if self._twophase:
            conn, cursor = self.open(transaction_mode=None, twophase=True)
            try:
                self._set_xid(cursor)
            except:
                self.close(conn, cursor)
                raise
        else:
            conn, cursor = self.open()
        return conn, cursor

    def restart_store(self, cursor):
        """Reuse a store connection."""
        try:
            cursor.connection.rollback()
            if self._twophase:
                self._set_xid(cursor)
        except (cx_Oracle.OperationalError, cx_Oracle.InterfaceError), e:
            raise StorageError(e)

    def store_temp(self, cursor, oid, prev_tid, md5sum, data):
        """Store an object in the temporary table."""
        if len(data) <= 2000:
            # Send data inline for speed.  Oracle docs say maximum size
            # of a RAW is 2000 bytes.  cx_Oracle.BINARY corresponds with RAW.
            cursor.setinputsizes(rawdata=cx_Oracle.BINARY)
            stmt = """
            INSERT INTO temp_store (zoid, prev_tid, md5, state)
            VALUES (:oid, :prev_tid, :md5sum, :rawdata)
            """
            cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
                md5sum=md5sum, rawdata=data)
        else:
            # Send data as a BLOB
            cursor.setinputsizes(blobdata=cx_Oracle.BLOB)
            stmt = """
            INSERT INTO temp_store (zoid, prev_tid, md5, state)
            VALUES (:oid, :prev_tid, :md5sum, :blobdata)
            """
            cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
                md5sum=md5sum, blobdata=data)

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

    def restore(self, cursor, oid, tid, md5sum, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        cursor.setinputsizes(data=cx_Oracle.BLOB)
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (:oid, :tid,
            COALESCE((SELECT tid FROM current_object WHERE zoid = :oid), 0),
            :md5sum, :data)
        """
        if data is not None:
            data = cx_Oracle.Binary(data)
        cursor.execute(stmt, oid=oid, tid=tid, md5sum=md5sum, data=data)

    def start_commit(self, cursor):
        """Prepare to commit."""
        # Hold commit_lock to prevent concurrent commits
        # (for as short a time as possible).
        # Lock transaction and current_object in share mode to ensure
        # conflict detection has the most current data.
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")
        cursor.execute("LOCK TABLE transaction IN SHARE MODE")
        cursor.execute("LOCK TABLE current_object IN SHARE MODE")

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

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        stmt = """
        INSERT INTO transaction
            (tid, packed, username, description, extension)
        VALUES (:1, :2, :3, :4, :5)
        """
        encoding = cursor.connection.encoding
        cursor.execute(stmt, (
            tid, packed and 'Y' or 'N', cx_Oracle.Binary(username),
            cx_Oracle.Binary(description), cx_Oracle.Binary(extension)))

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
        return self.execute_lob_stmt(cursor, stmt)

    def move_from_temp(self, cursor, tid):
        """Move the temporarily stored objects to permanent storage.

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

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        next_oid = self.new_oid(cursor)
        if next_oid < oid:
            # Oracle provides no way modify the sequence value
            # except through alter sequence or drop/create sequence,
            # but either statement kills the current transaction.
            # Therefore, open a temporary connection to make the
            # alteration.
            conn2, cursor2 = self.open()
            try:
                # Change the sequence by altering the increment.
                # (this is safer than dropping and re-creating the sequence)
                diff = oid - next_oid
                cursor2.execute(
                    "ALTER SEQUENCE zoid_seq INCREMENT BY %d" % diff)
                cursor2.execute("SELECT zoid_seq.nextval FROM DUAL")
                cursor2.execute("ALTER SEQUENCE zoid_seq INCREMENT BY 1")
                conn2.commit()
            finally:
                self.close(conn2, cursor2)

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

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        # No action needed
        pass


    def _add_object_ref_rows(self, cursor, add_rows):
        """Add rows to object_ref.

        The input rows are tuples containing (from_zoid, tid, to_zoid).

        This overrides the method by the same name in common.Adapter.
        """
        stmt = """
        INSERT INTO object_ref (zoid, tid, to_zoid)
        VALUES (:1, :2, :3)
        """
        cursor.executemany(stmt, add_rows)


    _poll_query = "SELECT MAX(tid) FROM transaction"


class TrackingMap:
    """Provides values for keys while tracking which keys are accessed."""

    def __init__(self, source):
        self.source = source
        self.used = set()

    def __getitem__(self, key):
        self.used.add(key)
        return self.source[key]
