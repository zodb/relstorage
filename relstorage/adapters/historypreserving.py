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
"""Code common to history-preserving adapters."""


import logging
import time
from relstorage.adapters.abstract import AbstractAdapter
from ZODB.POSException import UndoError

log = logging.getLogger(__name__)


class HistoryPreservingAdapter(AbstractAdapter):
    """An abstract adapter that retains history.

    Derivatives should have at least the following schema::

        -- The list of all transactions in the database
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            empty       BOOLEAN NOT NULL DEFAULT FALSE,
            username    BYTEA NOT NULL,
            description BYTEA NOT NULL,
            extension   BYTEA
        );

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

        -- Pointers to the current object state
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state
        );

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

    keep_history = True

    _scripts = {
        'create_temp_pack_visit': """
            CREATE TEMPORARY TABLE temp_pack_visit (
                zoid BIGINT NOT NULL,
                keep_tid BIGINT
            );
            CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid)
            """,

        'pre_pack_follow_child_refs': """
            UPDATE pack_object SET keep = %(TRUE)s
            WHERE keep = %(FALSE)s
                AND zoid IN (
                    SELECT DISTINCT to_zoid
                    FROM object_ref
                        JOIN temp_pack_visit USING (zoid)
                    WHERE object_ref.tid >= temp_pack_visit.keep_tid
                )
            """,

        'choose_pack_transaction': """
            SELECT tid
            FROM transaction
            WHERE tid > 0
                AND tid <= %(tid)s
                AND packed = FALSE
            ORDER BY tid DESC
            LIMIT 1
            """,

        'create_temp_undo': """
            CREATE TEMPORARY TABLE temp_undo (
                zoid BIGINT NOT NULL,
                prev_tid BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX temp_undo_zoid ON temp_undo (zoid)
            """,

        'reset_temp_undo': "DROP TABLE temp_undo",

        'transaction_has_data': """
            SELECT tid
            FROM object_state
            WHERE tid = %(tid)s
            LIMIT 1
            """,

        'pack_current_object': """
            DELETE FROM current_object
            WHERE tid = %(tid)s
                AND zoid in (
                    SELECT pack_state.zoid
                    FROM pack_state
                    WHERE pack_state.tid = %(tid)s
                )
            """,

        'pack_object_state': """
            DELETE FROM object_state
            WHERE tid = %(tid)s
                AND zoid in (
                    SELECT pack_state.zoid
                    FROM pack_state
                    WHERE pack_state.tid = %(tid)s
                )
            """,

        'pack_object_ref': """
            DELETE FROM object_refs_added
            WHERE tid IN (
                SELECT tid
                FROM transaction
                WHERE empty = %(TRUE)s
                );
            DELETE FROM object_ref
            WHERE tid IN (
                SELECT tid
                FROM transaction
                WHERE empty = %(TRUE)s
                )
            """,
    }

    def _transaction_iterator(self, cursor):
        """Iterate over a list of transactions returned from the database.

        Each row begins with (tid, username, description, extension)
        and may have other columns.
        """
        for row in cursor:
            tid, username, description, ext = row[:4]
            if username is None:
                username = ''
            else:
                username = str(username)
            if description is None:
                description = ''
            else:
                description = str(description)
            if ext is None:
                ext = ''
            else:
                ext = str(ext)
            yield (tid, username, description, ext) + tuple(row[4:])


    def iter_transactions(self, cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields (tid, username, description, extension) for each transaction.
        """
        stmt = """
        SELECT tid, username, description, extension
        FROM transaction
        WHERE packed = %(FALSE)s
            AND tid != 0
        ORDER BY tid DESC
        """
        self._run_script_stmt(cursor, stmt)
        return self._transaction_iterator(cursor)


    def iter_transactions_range(self, cursor, start=None, stop=None):
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields (tid, username, description, extension, packed)
        for each transaction.
        """
        stmt = """
        SELECT tid, username, description, extension,
            CASE WHEN packed = %(TRUE)s THEN 1 ELSE 0 END
        FROM transaction
        WHERE tid >= 0
        """
        if start is not None:
            stmt += " AND tid >= %(min_tid)s"
        if stop is not None:
            stmt += " AND tid <= %(max_tid)s"
        stmt += " ORDER BY tid"
        self._run_script_stmt(cursor, stmt,
            {'min_tid': start, 'max_tid': stop})
        return self._transaction_iterator(cursor)


    def iter_object_history(self, cursor, oid):
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """
        stmt = """
        SELECT 1 FROM current_object WHERE zoid = %(oid)s
        """
        self._run_script_stmt(cursor, stmt, {'oid': oid})
        if not cursor.fetchall():
            raise KeyError(oid)

        stmt = """
        SELECT tid, username, description, extension, %(OCTET_LENGTH)s(state)
        FROM transaction
            JOIN object_state USING (tid)
        WHERE zoid = %(oid)s
            AND packed = %(FALSE)s
        ORDER BY tid DESC
        """
        self._run_script_stmt(cursor, stmt, {'oid': oid})
        return self._transaction_iterator(cursor)


    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        stmt = """
        SELECT 1 FROM transaction
        WHERE tid = %(undo_tid)s
            AND packed = %(FALSE)s
        """
        self._run_script_stmt(cursor, stmt, {'undo_tid': undo_tid})
        if not cursor.fetchall():
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
        WHERE prev_os.tid = %(undo_tid)s
            AND cur_os.md5 != prev_os.md5
        """
        self._run_script_stmt(cursor, stmt, {'undo_tid': undo_tid})
        if cursor.fetchmany():
            raise UndoError(
                "Some data were modified by a later transaction")

        # Rule: don't allow the creation of the root object to
        # be undone.  It's hard to get it back.
        stmt = """
        SELECT 1
        FROM object_state
        WHERE tid = %(undo_tid)s
            AND zoid = 0
            AND prev_tid = 0
        """
        self._run_script_stmt(cursor, stmt, {'undo_tid': undo_tid})
        if cursor.fetchall():
            raise UndoError("Can't undo the creation of the root object")


    def undo(self, cursor, undo_tid, self_tid):
        """Undo a transaction.

        Parameters: "undo_tid", the integer tid of the transaction to undo,
        and "self_tid", the integer tid of the current transaction.

        Returns the states copied forward by the undo operation as a
        list of (oid, old_tid).
        """
        stmt = self._scripts['create_temp_undo']
        if stmt:
            self._run_script(cursor, stmt)

        stmt = """
        DELETE FROM temp_undo;

        -- Put into temp_undo the list of objects to be undone and
        -- the tid of the transaction that has the undone state.
        INSERT INTO temp_undo (zoid, prev_tid)
        SELECT zoid, prev_tid
        FROM object_state
        WHERE tid = %(undo_tid)s;

        -- Override previous undo operations within this transaction
        -- by resetting the current_object pointer and deleting
        -- copied states from object_state.
        UPDATE current_object
        SET tid = (
                SELECT prev_tid
                FROM object_state
                WHERE zoid = current_object.zoid
                    AND tid = %(self_tid)s
            )
        WHERE zoid IN (SELECT zoid FROM temp_undo)
            AND tid = %(self_tid)s;

        DELETE FROM object_state
        WHERE zoid IN (SELECT zoid FROM temp_undo)
            AND tid = %(self_tid)s;

        -- Copy old states forward.
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT temp_undo.zoid, %(self_tid)s, current_object.tid,
            prev.md5, prev.state
        FROM temp_undo
            JOIN current_object ON (temp_undo.zoid = current_object.zoid)
            LEFT JOIN object_state prev
                ON (prev.zoid = temp_undo.zoid
                    AND prev.tid = temp_undo.prev_tid);

        -- List the copied states.
        SELECT zoid, prev_tid FROM temp_undo
        """
        self._run_script(cursor, stmt,
            {'undo_tid': undo_tid, 'self_tid': self_tid})
        res = list(cursor)

        stmt = self._scripts['reset_temp_undo']
        if stmt:
            self._run_script(cursor, stmt)

        return res


    def choose_pack_transaction(self, pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """
        conn, cursor = self.open()
        try:
            stmt = self._scripts['choose_pack_transaction']
            self._run_script(cursor, stmt, {'tid': pack_point})
            rows = cursor.fetchall()
            if not rows:
                # Nothing needs to be packed.
                return None
            return rows[0][0]
        finally:
            self.close(conn, cursor)


    def pre_pack(self, pack_tid, get_references, options):
        """Decide what to pack.

        tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        options is an instance of relstorage.Options.
        The options.pack_gc flag indicates whether to run garbage collection.
        If pack_gc is false, at least one revision of every object is kept,
        even if nothing refers to it.  Packing with pack_gc disabled can be
        much faster.
        """
        conn, cursor = self.open_for_pre_pack()
        try:
            try:
                if options.pack_gc:
                    log.info("pre_pack: start with gc enabled")
                    self._pre_pack_with_gc(
                        conn, cursor, pack_tid, get_references)
                else:
                    log.info("pre_pack: start without gc")
                    self._pre_pack_without_gc(
                        conn, cursor, pack_tid)
                conn.commit()

                log.info("pre_pack: enumerating states to pack")
                stmt = "%(TRUNCATE)s pack_state"
                self._run_script_stmt(cursor, stmt)
                to_remove = 0

                if options.pack_gc:
                    # Pack objects with the keep flag set to false.
                    stmt = """
                    INSERT INTO pack_state (tid, zoid)
                    SELECT tid, zoid
                    FROM object_state
                        JOIN pack_object USING (zoid)
                    WHERE keep = %(FALSE)s
                        AND tid > 0
                        AND tid <= %(pack_tid)s
                    """
                    self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})
                    to_remove += cursor.rowcount

                # Pack object states with the keep flag set to true.
                stmt = """
                INSERT INTO pack_state (tid, zoid)
                SELECT tid, zoid
                FROM object_state
                    JOIN pack_object USING (zoid)
                WHERE keep = %(TRUE)s
                    AND tid > 0
                    AND tid != keep_tid
                    AND tid <= %(pack_tid)s
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid':pack_tid})
                to_remove += cursor.rowcount

                log.info("pre_pack: enumerating transactions to pack")
                stmt = "%(TRUNCATE)s pack_state_tid"
                self._run_script_stmt(cursor, stmt)
                stmt = """
                INSERT INTO pack_state_tid (tid)
                SELECT DISTINCT tid
                FROM pack_state
                """
                cursor.execute(stmt)

                log.info("pre_pack: will remove %d object state(s)",
                    to_remove)

            except:
                log.exception("pre_pack: failed")
                conn.rollback()
                raise
            else:
                log.info("pre_pack: finished successfully")
                conn.commit()
        finally:
            self.close(conn, cursor)


    def _pre_pack_without_gc(self, conn, cursor, pack_tid):
        """Determine what to pack, without garbage collection.

        With garbage collection disabled, there is no need to follow
        object references.
        """
        # Fill the pack_object table with OIDs, but configure them
        # all to be kept by setting keep to true.
        log.debug("pre_pack: populating pack_object")
        stmt = """
        %(TRUNCATE)s pack_object;

        INSERT INTO pack_object (zoid, keep, keep_tid)
        SELECT zoid, %(TRUE)s, MAX(tid)
        FROM object_state
        WHERE tid > 0 AND tid <= %(pack_tid)s
        GROUP BY zoid
        """
        self._run_script(cursor, stmt, {'pack_tid': pack_tid})


    def _pre_pack_with_gc(self, conn, cursor, pack_tid, get_references):
        """Determine what to pack, with garbage collection.
        """
        stmt = self._scripts['create_temp_pack_visit']
        if stmt:
            self._run_script(cursor, stmt)

        self.fill_object_refs(conn, cursor, get_references)

        log.info("pre_pack: filling the pack_object table")
        # Fill the pack_object table with OIDs that either will be
        # removed (if nothing references the OID) or whose history will
        # be cut.
        stmt = """
        %(TRUNCATE)s pack_object;

        INSERT INTO pack_object (zoid, keep, keep_tid)
        SELECT zoid, %(FALSE)s, MAX(tid)
        FROM object_state
        WHERE tid > 0 AND tid <= %(pack_tid)s
        GROUP BY zoid;

        -- If the root object is in pack_object, keep it.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid = 0;

        -- Keep objects that have been revised since pack_tid.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid IN (
            SELECT zoid
            FROM current_object
            WHERE tid > %(pack_tid)s
        );

        -- Keep objects that are still referenced by object states in
        -- transactions that will not be packed.
        -- Use temp_pack_visit for temporary state; otherwise MySQL 5 chokes.
        INSERT INTO temp_pack_visit (zoid)
        SELECT DISTINCT to_zoid
        FROM object_ref
        WHERE tid > %(pack_tid)s;

        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid IN (
            SELECT zoid
            FROM temp_pack_visit
        );

        %(TRUNCATE)s temp_pack_visit;
        """
        self._run_script(cursor, stmt, {'pack_tid': pack_tid})

        # Set the 'keep' flags in pack_object
        self._visit_all_references(cursor)


    def pack(self, pack_tid, options, sleep=time.sleep, packed_func=None):
        """Pack.  Requires the information provided by pre_pack."""

        # Read committed mode is sufficient.
        conn, cursor = self.open()
        try:
            try:
                stmt = """
                SELECT transaction.tid,
                    CASE WHEN packed = %(TRUE)s THEN 1 ELSE 0 END,
                    CASE WHEN pack_state_tid.tid IS NOT NULL THEN 1 ELSE 0 END
                FROM transaction
                    LEFT JOIN pack_state_tid ON (
                        transaction.tid = pack_state_tid.tid)
                WHERE transaction.tid > 0
                    AND transaction.tid <= %(pack_tid)s
                    AND (packed = %(FALSE)s OR pack_state_tid.tid IS NOT NULL)
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})
                tid_rows = list(cursor)
                tid_rows.sort()  # oldest first

                log.info("pack: will pack %d transaction(s)", len(tid_rows))

                stmt = self._scripts['create_temp_pack_visit']
                if stmt:
                    self._run_script(cursor, stmt)

                # Hold the commit lock while packing to prevent deadlocks.
                # Pack in small batches of transactions in order to minimize
                # the interruption of concurrent write operations.
                start = time.time()
                packed_list = []
                self._hold_commit_lock(cursor)
                for tid, packed, has_removable in tid_rows:
                    self._pack_transaction(
                        cursor, pack_tid, tid, packed, has_removable,
                        packed_list)
                    if time.time() >= start + options.pack_batch_timeout:
                        conn.commit()
                        if packed_func is not None:
                            for oid, tid in packed_list:
                                packed_func(oid, tid)
                        del packed_list[:]
                        self._release_commit_lock(cursor)
                        self._pause_pack(sleep, options, start)
                        self._hold_commit_lock(cursor)
                        start = time.time()
                if packed_func is not None:
                    for oid, tid in packed_list:
                        packed_func(oid, tid)
                packed_list = None

                self._pack_cleanup(conn, cursor)

            except:
                log.exception("pack: failed")
                conn.rollback()
                raise

            else:
                log.info("pack: finished successfully")
                conn.commit()

        finally:
            self.close(conn, cursor)


    def _pack_transaction(self, cursor, pack_tid, tid, packed,
            has_removable, packed_list):
        """Pack one transaction.  Requires populated pack tables."""
        log.debug("pack: transaction %d: packing", tid)
        removed_objects = 0
        removed_states = 0

        if has_removable:
            stmt = self._scripts['pack_current_object']
            self._run_script_stmt(cursor, stmt, {'tid': tid})
            removed_objects = cursor.rowcount

            stmt = self._scripts['pack_object_state']
            self._run_script_stmt(cursor, stmt, {'tid': tid})
            removed_states = cursor.rowcount

            # Terminate prev_tid chains
            stmt = """
            UPDATE object_state SET prev_tid = 0
            WHERE prev_tid = %(tid)s
                AND tid <= %(pack_tid)s
            """
            self._run_script_stmt(cursor, stmt,
                {'pack_tid': pack_tid, 'tid': tid})

            stmt = """
            SELECT pack_state.zoid
            FROM pack_state
            WHERE pack_state.tid = %(tid)s
            """
            self._run_script_stmt(cursor, stmt, {'tid': tid})
            for (oid,) in cursor:
                packed_list.append((oid, tid))

        # Find out whether the transaction is empty
        stmt = self._scripts['transaction_has_data']
        self._run_script_stmt(cursor, stmt, {'tid': tid})
        empty = not list(cursor)

        # mark the transaction packed and possibly empty
        if empty:
            clause = 'empty = %(TRUE)s'
            state = 'empty'
        else:
            clause = 'empty = %(FALSE)s'
            state = 'not empty'
        stmt = "UPDATE transaction SET packed = %(TRUE)s, " + clause
        stmt += " WHERE tid = %(tid)s"
        self._run_script_stmt(cursor, stmt, {'tid': tid})

        log.debug(
            "pack: transaction %d (%s): removed %d object(s) and %d state(s)",
            tid, state, removed_objects, removed_states)


    def _pack_cleanup(self, conn, cursor):
        """Remove unneeded table rows after packing"""
        # commit the work done so far
        conn.commit()
        self._release_commit_lock(cursor)
        self._hold_commit_lock(cursor)
        log.info("pack: cleaning up")

        log.debug("pack: removing unused object references")
        stmt = self._scripts['pack_object_ref']
        self._run_script(cursor, stmt)

        log.debug("pack: removing empty packed transactions")
        stmt = """
        DELETE FROM transaction
        WHERE packed = %(TRUE)s
            AND empty = %(TRUE)s
        """
        self._run_script_stmt(cursor, stmt)

        # perform cleanup that does not require the commit lock
        conn.commit()
        self._release_commit_lock(cursor)

        log.debug("pack: clearing temporary pack state")
        for _table in ('pack_object', 'pack_state', 'pack_state_tid'):
            stmt = '%(TRUNCATE)s ' + _table
            self._run_script_stmt(cursor, stmt)
