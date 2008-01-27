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
"""PostgreSQL adapter for RelStorage."""

from base64 import decodestring, encodestring
import logging
import psycopg2, psycopg2.extensions
from ZODB.POSException import ConflictError, StorageError, UndoError

log = logging.getLogger("relstorage.postgresql")


# Notes about adapters:
#
# An adapter must not hold a connection, cursor, or database state, because
# RelStorage opens multiple concurrent connections using a single adapter
# instance.
# All OID and TID values are integers, not binary strings, except as noted.


class PostgreSQLAdapter(object):
    """PostgreSQL adapter for RelStorage."""

    def __init__(self, dsn=''):
        self._dsn = dsn

    def create_schema(self, cursor):
        """Create the database tables."""
        stmt = """
        CREATE TABLE commit_lock ();

        -- The list of all transactions in the database
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            username    VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
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

        -- Pointers to the current object state
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state
        );

        -- During packing, an exclusive lock is held on pack_lock.
        CREATE TABLE pack_lock ();

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
            tid         BIGINT NOT NULL PRIMARY KEY
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
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);
        """
        cursor.execute(stmt)


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


    def zap(self):
        """Clear all data out of the database.

        Used by the test suite.
        """
        conn, cursor = self.open()
        try:
            try:
                cursor.execute("""
                TRUNCATE object_refs_added, object_ref, current_object,
                    object_state, transaction;
                -- Create a special transaction to represent object creation.
                INSERT INTO transaction (tid, username, description)
                    VALUES (0, '', '');
                ALTER SEQUENCE zoid_seq START WITH 1;
                """)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def open(self, isolation=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE):
        """Open a database connection and return (conn, cursor)."""
        try:
            conn = psycopg2.connect(self._dsn)
            conn.set_isolation_level(isolation)
            cursor = conn.cursor()
            cursor.arraysize = 64
        except psycopg2.OperationalError:
            log.debug("Unable to connect in %s", repr(self))
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

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open()
        cursor.execute("LISTEN invalidate")
        return conn, cursor

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
        conn, cursor = self.open()
        try:
            cursor.execute("SELECT SUM(relpages) FROM pg_class")
            # A relative page on postgres is 8K
            relpages = cursor.fetchone()[0]
            return relpages * 8 * 1024
        finally:
            self.close(conn, cursor)

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
        ORDER BY tid desc
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

    def get_object_tids(self, cursor, oids):
        """Returns a map containing the current tid for each oid in a list.

        OIDs that do not exist are not included.
        """
        # query in chunks to avoid running into a maximum query length
        chunk_size = 512
        res = {}
        for i in xrange(0, len(oids), chunk_size):
            chunk = oids[i : i + chunk_size]
            oid_str = ','.join(str(oid) for oid in chunk)
            stmt = """
            SELECT zoid, tid FROM current_object WHERE zoid IN (%s)
            """ % oid_str
            cursor.execute(stmt)
            res.update(dict(iter(cursor)))
        return res

    def open_for_commit(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        # psycopg2 doesn't support prepared transactions, so
        # tell psycopg2 to use the autocommit isolation level, but covertly
        # switch to the serializable isolation level.
        conn, cursor = self.open(
            isolation=psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
        cursor.execute("""
        BEGIN ISOLATION LEVEL SERIALIZABLE;
        LOCK commit_lock IN EXCLUSIVE MODE
        """)
        return conn, cursor

    def restart_commit(self, cursor):
        """Rollback the commit and start over.

        The cursor must be the type created by open_for_commit().
        """
        cursor.execute("""
        ROLLBACK;
        BEGIN ISOLATION LEVEL SERIALIZABLE;
        LOCK commit_lock IN EXCLUSIVE MODE
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

    def add_transaction(self, cursor, tid, username, description, extension):
        """Add a transaction.

        Raises ConflictError if the given tid has already been used.
        """
        try:
            stmt = """
            INSERT INTO transaction
                (tid, username, description, extension)
            VALUES (%s, %s, %s, decode(%s, 'base64'))
            """
            cursor.execute(stmt, (
                tid, username, description, encodestring(extension)))
        except psycopg2.IntegrityError, e:
            raise ConflictError(e)

    def store(self, cursor, oid, tid, prev_tid, md5sum, data):
        """Store an object.  May raise ConflictError."""
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (%s, %s, %s, %s, decode(%s, 'base64'))
        """
        try:
            cursor.execute(stmt, (
                oid, tid, prev_tid, md5sum, encodestring(data)))
        except psycopg2.ProgrammingError, e:
            # This can happen if another thread is currently packing
            # and prev_tid refers to a transaction being packed.
            if 'concurrent update' in e.args[0]:
                raise ConflictError(e)
            else:
                raise

    def update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        try:
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
        except psycopg2.ProgrammingError, e:
            if 'concurrent update' in e.args[0]:
                raise ConflictError(e)
            else:
                raise

    def commit_phase1(self, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        try:
            txn = 'T%d' % tid
            stmt = "NOTIFY invalidate; PREPARE TRANSACTION %s"
            cursor.execute(stmt, (txn,))
            return txn
        except psycopg2.ProgrammingError, e:
            if 'concurrent update' in e.args[0]:
                raise ConflictError(e)
            else:
                raise

    def commit_phase2(self, cursor, txn):
        """Final transaction commit."""
        cursor.execute('COMMIT PREPARED %s', (txn,))

    def abort(self, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        if txn is not None:
            cursor.execute('ROLLBACK PREPARED %s', (txn,))
        else:
            cursor.execute('ROLLBACK')

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "SELECT NEXTVAL('zoid_seq')"
        cursor.execute(stmt)
        return cursor.fetchone()[0]


    def iter_transactions(self, cursor):
        """Iterate over the transaction log.

        Yields (tid, username, description, extension) for each transaction.
        """
        stmt = """
        SELECT tid, username, description, extension
        FROM transaction
        WHERE packed = FALSE
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
        SELECT 1 FROM current_object WHERE zoid = %s
        """
        cursor.execute(stmt, (oid,))
        if not cursor.rowcount:
            raise KeyError(oid)

        stmt = """
        SELECT tid, username, description, extension, OCTET_LENGTH(state)
        FROM transaction
            JOIN object_state USING (tid)
        WHERE zoid = %s
            AND packed = FALSE
        ORDER BY tid desc
        """
        cursor.execute(stmt, (oid,))
        return iter(cursor)


    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        stmt = """
        LOCK pack_lock IN EXCLUSIVE MODE NOWAIT
        """
        try:
            cursor.execute(stmt)
        except psycopg2.DatabaseError:
            raise StorageError('A pack or undo operation is in progress')


    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        stmt = """
        SELECT 1 FROM transaction WHERE tid = %s AND packed = FALSE
        """
        cursor.execute(stmt, (undo_tid,))
        if not cursor.rowcount:
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
        # Update records produced by earlier undo operations
        # within this transaction.  Change the state, but not
        # prev_tid, since prev_tid is still correct.
        # Table names: 'undoing' refers to the transaction being
        # undone and 'prev' refers to the object state identified
        # by undoing.prev_tid.
        stmt = """
        UPDATE object_state SET state = (
            SELECT prev.state
            FROM object_state undoing
                LEFT JOIN object_state prev
                ON (prev.zoid = undoing.zoid
                    AND prev.tid = undoing.prev_tid)
            WHERE undoing.tid = %(undo_tid)s
                AND undoing.zoid = object_state.zoid
        ),
        md5 = (
            SELECT prev.md5
            FROM object_state undoing
                LEFT JOIN object_state prev
                ON (prev.zoid = undoing.zoid
                    AND prev.tid = undoing.prev_tid)
            WHERE undoing.tid = %(undo_tid)s
                AND undoing.zoid = object_state.zoid
        )
        WHERE tid = %(self_tid)s
            AND zoid IN (
                SELECT zoid FROM object_state WHERE tid = %(undo_tid)s);

        -- Add new undo records.

        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT undoing.zoid, %(self_tid)s, current_object.tid,
            prev.md5, prev.state
        FROM object_state undoing
            JOIN current_object USING (zoid)
            LEFT JOIN object_state prev
                ON (prev.zoid = undoing.zoid
                    AND prev.tid = undoing.prev_tid)
        WHERE undoing.tid = %(undo_tid)s
            AND undoing.zoid NOT IN (
                SELECT zoid FROM object_state WHERE tid = %(self_tid)s);

        -- List the changed OIDs.

        SELECT zoid FROM object_state WHERE tid = %(undo_tid)s
        """
        cursor.execute(stmt, {'undo_tid': undo_tid, 'self_tid': self_tid})
        return [oid_int for (oid_int,) in cursor]


    def choose_pack_transaction(self, cursor, pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """
        stmt = """
        SELECT tid
        FROM transaction
        WHERE tid > 0 AND tid <= %s
            AND packed = FALSE
        ORDER BY tid desc
        LIMIT 1
        """
        cursor.execute(stmt, (pack_point,))
        if not cursor.rowcount:
            # Nothing needs to be packed.
            return None
        assert cursor.rowcount == 1
        return cursor.fetchone()[0]


    def pre_pack(self, cursor, pack_tid, get_references):
        """Decide what to pack.

        tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.
        """
        # Fill object_ref with references from object states
        # in transactions that will not be packed.
        self._fill_nonpacked_refs(cursor, pack_tid, get_references)

        # Ensure the temporary pack_object table is clear.
        cursor.execute("TRUNCATE pack_object")

        args = {'pack_tid': pack_tid}

        # Fill the pack_object table with OIDs that either will be
        # removed (if nothing references the OID) or whose history will
        # be cut.
        stmt = """
        INSERT INTO pack_object (zoid, keep)
        SELECT DISTINCT zoid, false
        FROM object_state
        WHERE tid <= %(pack_tid)s
        """
        cursor.execute(stmt, args)

        # If the root object is in pack_object, keep it.
        stmt = """
        UPDATE pack_object SET keep = true
        WHERE zoid = 0
        """
        cursor.execute(stmt)

        # Keep objects that have been revised since pack_tid.
        stmt = """
        UPDATE pack_object SET keep = true
        WHERE keep = false
            AND zoid IN (
                SELECT zoid
                FROM current_object
                WHERE tid > %(pack_tid)s
            )
        """
        cursor.execute(stmt, args)

        # Keep objects that are still referenced by object states in
        # transactions that will not be packed.
        stmt = """
        UPDATE pack_object SET keep = true
        WHERE keep = false
            AND zoid IN (
                SELECT to_zoid
                FROM object_ref
                WHERE tid > %(pack_tid)s
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
                    SELECT tid
                    FROM object_state
                    WHERE zoid = pack_object.zoid
                        AND tid > 0
                        AND tid <= %(pack_tid)s
                    ORDER BY tid DESC
                    LIMIT 1
                )
            WHERE keep = true AND keep_tid IS NULL
            """
            cursor.execute(stmt, args)

            self._fill_pack_object_refs(cursor, get_references)

            stmt = """
            UPDATE pack_object SET keep = true
            WHERE keep = false
                AND zoid IN (
                    SELECT DISTINCT to_zoid
                    FROM object_ref
                        JOIN pack_object parent ON (
                            object_ref.zoid = parent.zoid)
                    WHERE parent.keep = true
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
        WHERE tid > %s
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
        SELECT zoid, encode(state, 'base64')
        FROM object_state
        WHERE tid = %s
        """
        cursor.execute(stmt, (tid,))

        to_add = []  # [(from_oid, tid, to_oid)]
        for from_oid, state64 in cursor:
            if state64 is not None:
                state = decodestring(state64)
                to_oids = get_references(state)
                for to_oid in to_oids:
                    to_add.append((from_oid, tid, to_oid))

        if to_add:
            stmt = """
            INSERT INTO object_ref (zoid, tid, to_zoid)
            VALUES (%s, %s, %s)
            """
            cursor.executemany(stmt, to_add)

        # The references have been computed for this transaction.
        stmt = """
        INSERT INTO object_refs_added (tid)
        VALUES (%s)
        """
        cursor.execute(stmt, (tid,))


    def pack(self, pack_tid):
        """Pack.  Requires populated pack tables."""

        # Read committed mode is sufficient.
        conn, cursor = self.open(
            isolation=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        try:
            try:

                for table in ('object_ref', 'current_object', 'object_state'):

                    # Remove objects that are in pack_object and have keep
                    # set to false.
                    stmt = """
                    DELETE FROM %s
                    WHERE zoid IN (
                            SELECT zoid
                            FROM pack_object
                            WHERE keep = false
                        )
                    """ % table
                    cursor.execute(stmt)

                    if table != 'current_object':
                        # Cut the history of objects in pack_object that
                        # have keep set to true.
                        stmt = """
                        DELETE FROM %s
                        WHERE zoid IN (
                                SELECT zoid
                                FROM pack_object
                                WHERE keep = true
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
                WHERE tid <= %(tid)s
                    AND prev_tid != 0;

                -- For each tid to be removed, delete the corresponding row in
                -- object_refs_added.
                DELETE FROM object_refs_added
                WHERE tid > 0
                    AND tid <= %(tid)s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = object_refs_added.tid
                    );

                -- Delete transactions no longer used.
                DELETE FROM transaction
                WHERE tid > 0
                    AND tid <= %(tid)s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = transaction.tid
                    );

                -- Mark the remaining packable transactions as packed
                UPDATE transaction SET packed = true
                WHERE tid > 0
                    AND tid <= %(tid)s
                    AND packed = false
                """
                cursor.execute(stmt, {'tid': pack_tid})

                # Clean up
                cursor.execute("TRUNCATE pack_object")

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
            if prev_polled_tid is not None:
                if not cursor.isready():
                    # No invalidate notifications arrived.
                    return (), prev_polled_tid
                del conn.notifies[:]

            # find out the tid of the most recent transaction.
            stmt = """
            SELECT tid
            FROM transaction
            ORDER BY tid desc
            LIMIT 1
            """
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

        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            raise StorageError("database disconnected")

