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
"""Pack/Undo implementations.
"""

# pylint:disable=too-many-lines,unused-argument

from ZODB.POSException import UndoError
from ZODB.utils import u64
from perfmetrics import metricmethod
from relstorage.adapters.interfaces import IPackUndo
from relstorage.iter import fetchmany
from relstorage.treemark import TreeMarker
from zope.interface import implementer
import logging
import time

from relstorage._compat import db_binary_to_bytes
from relstorage._compat import mysql_connection

log = logging.getLogger(__name__)

class PackUndo(object):
    """Abstract base class for pack/undo"""

    verify_sane_database = False

    _script_choose_pack_transaction = None

    def __init__(self, database_type, connmanager, runner, locker, options):
        self.database_type = database_type
        self.connmanager = connmanager
        self.runner = runner
        self.locker = locker
        self.options = options

    def _fetchmany(self, cursor):
        return fetchmany(cursor)

    def choose_pack_transaction(self, pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """
        conn, cursor = self.connmanager.open()
        try:
            stmt = self._script_choose_pack_transaction
            self.runner.run_script(cursor, stmt, {'tid': pack_point})
            rows = cursor.fetchall()
            if not rows:
                # Nothing needs to be packed.
                return None
            return rows[0][0]
        finally:
            self.connmanager.close(conn, cursor)

    def _traverse_graph(self, cursor):
        """Visit the entire object graph to find out what should be kept.

        Sets the pack_object.keep flags.
        """
        log.info("pre_pack: downloading pack_object and object_ref.")

        marker = TreeMarker()

        # Download the list of object references into the TreeMarker.

        # Note the Oracle optimizer hints in the following statement; MySQL
        # and PostgreSQL ignore these (MySQL 5.7, though, emits a warning).
        # Oracle fails to notice that pack_object
        # is now filled and chooses the wrong execution plan, completely
        # killing this query on large RelStorage databases, unless these hints
        # are included.
        stmt = """
        SELECT
            /*+ FULL(object_ref) */
            /*+ FULL(pack_object) */
            object_ref.zoid, object_ref.to_zoid
        FROM object_ref
            JOIN pack_object ON (object_ref.zoid = pack_object.zoid)
        WHERE object_ref.tid >= pack_object.keep_tid
        ORDER BY object_ref.zoid, object_ref.to_zoid
        """
        self.runner.run_script_stmt(cursor, stmt)
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            marker.add_refs(rows)

        # Use the TreeMarker to find all reachable objects.

        log.info("pre_pack: traversing the object graph "
                 "to find reachable objects.")
        stmt = """
        SELECT zoid
        FROM pack_object
        WHERE keep = %(TRUE)s
        """
        self.runner.run_script_stmt(cursor, stmt)
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            marker.mark(oid for (oid,) in rows)

        marker.free_refs()

        # Upload the TreeMarker results to the database.

        log.info(
            "pre_pack: marking objects reachable: %d",
            marker.reachable_count)

        batch = []

        def upload_batch():
            oids_str = ','.join(str(oid) for oid in batch)
            del batch[:]
            stmt = """
            UPDATE pack_object SET keep = %%(TRUE)s, visited = %%(TRUE)s
            WHERE zoid IN (%s)
            """ % oids_str
            self.runner.run_script_stmt(cursor, stmt)

        batch_append = batch.append
        for oid in marker.reachable:
            batch_append(oid)
            if len(batch) >= 1000:
                upload_batch()
        if batch:
            upload_batch()

    def _pause_pack_until_lock(self, cursor, sleep):
        """Pause until we can obtain a nowait commit lock."""
        if sleep is None:
            sleep = time.sleep
        delay = self.options.pack_commit_busy_delay
        while not self.locker.hold_commit_lock(cursor, nowait=True):
            mysql_connection(cursor).rollback()
            log.debug('pack: commit lock busy, sleeping %.4g second(s)', delay)
            sleep(delay)


@implementer(IPackUndo)
class HistoryPreservingPackUndo(PackUndo):
    """
    History-preserving pack/undo.
    """

    keep_history = True

    _script_choose_pack_transaction = """
        SELECT tid
        FROM transaction
        WHERE tid > 0
            AND tid <= %(tid)s
            AND packed = FALSE
        ORDER BY tid DESC
        LIMIT 1
        """

    _script_create_temp_pack_visit = """
        CREATE TEMPORARY TABLE temp_pack_visit (
            zoid BIGINT NOT NULL,
            keep_tid BIGINT NOT NULL
        );
        CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid);
        CREATE INDEX temp_pack_keep_tid ON temp_pack_visit (keep_tid)
        """

    _script_create_temp_undo = """
        CREATE TEMPORARY TABLE temp_undo (
            zoid BIGINT NOT NULL,
            prev_tid BIGINT NOT NULL
        );
        CREATE UNIQUE INDEX temp_undo_zoid ON temp_undo (zoid)
        """

    _script_reset_temp_undo = "DROP TABLE temp_undo"

    _script_find_pack_tid = """
        SELECT keep_tid
        FROM pack_object
        ORDER BY keep_tid DESC
        LIMIT 1
        """

    _script_transaction_has_data = """
        SELECT tid
        FROM object_state
        WHERE tid = %(tid)s
        LIMIT 1
        """

    _script_pack_current_object = """
        DELETE FROM current_object
        WHERE tid = %(tid)s
            AND zoid in (
                SELECT pack_state.zoid
                FROM pack_state
                WHERE pack_state.tid = %(tid)s
            )
        """

    _script_pack_object_state = """
        DELETE FROM object_state
        WHERE tid = %(tid)s
            AND zoid in (
                SELECT pack_state.zoid
                FROM pack_state
                WHERE pack_state.tid = %(tid)s
            )
        """

    _script_pack_object_ref = """
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
        """

    # See http://www.postgres.cz/index.php/PostgreSQL_SQL_Tricks#Fast_first_n_rows_removing
    # for = any(array(...)) rationale.
    _script_delete_empty_transactions_batch = """
        DELETE FROM transaction
        WHERE tid = any(array(
            SELECT tid FROM transaction
            WHERE packed = %(TRUE)s
              AND empty = %(TRUE)s
            LIMIT 1000
        ))
        """

    @metricmethod
    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        stmt = """
        SELECT 1 FROM transaction
        WHERE tid = %(undo_tid)s
            AND packed = %(FALSE)s
        """
        self.runner.run_script_stmt(cursor, stmt, {'undo_tid': undo_tid})
        if not cursor.fetchall():
            raise UndoError("Transaction not found or packed")

        # Rule: we can undo an object if the object's state in the
        # transaction to undo matches the object's current state.
        # If any object in the transaction does not fit that rule,
        # refuse to undo.
        # (Note that this prevents conflict-resolving undo as described
        # by ZODB.tests.ConflictResolution.ConflictResolvingTransUndoStorage.
        # Do people need that? If so, we can probably support it, but it
        # will require additional code.)
        stmt = """
        SELECT prev_os.zoid, current_object.tid
        FROM object_state prev_os
            JOIN object_state cur_os ON (prev_os.zoid = cur_os.zoid)
            JOIN current_object ON (cur_os.zoid = current_object.zoid
                AND cur_os.tid = current_object.tid)
        WHERE prev_os.tid = %(undo_tid)s
            AND cur_os.md5 != prev_os.md5
        """
        self.runner.run_script_stmt(cursor, stmt, {'undo_tid': undo_tid})
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
        self.runner.run_script_stmt(cursor, stmt, {'undo_tid': undo_tid})
        if cursor.fetchall():
            raise UndoError("Can't undo the creation of the root object")


    @metricmethod
    def undo(self, cursor, undo_tid, self_tid):
        """Undo a transaction.

        Parameters: "undo_tid", the integer tid of the transaction to undo,
        and "self_tid", the integer tid of the current transaction.

        Returns the states copied forward by the undo operation as a
        list of (oid, old_tid).
        """
        stmt = self._script_create_temp_undo
        if stmt:
            self.runner.run_script(cursor, stmt)

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
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state_size, state)
        SELECT temp_undo.zoid, %(self_tid)s, current_object.tid,
            md5, COALESCE(state_size, 0), state
        FROM temp_undo
            JOIN current_object ON (temp_undo.zoid = current_object.zoid)
            LEFT JOIN object_state
                ON (object_state.zoid = temp_undo.zoid
                    AND object_state.tid = temp_undo.prev_tid);

        -- Copy old blob chunks forward.
        INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
        SELECT temp_undo.zoid, %(self_tid)s, chunk_num, chunk
        FROM temp_undo
            JOIN blob_chunk
                ON (blob_chunk.zoid = temp_undo.zoid
                    AND blob_chunk.tid = temp_undo.prev_tid);

        -- List the copied states.
        SELECT zoid, prev_tid FROM temp_undo
        """
        self.runner.run_script(cursor, stmt,
                               {'undo_tid': undo_tid, 'self_tid': self_tid})
        res = list(cursor)

        stmt = self._script_reset_temp_undo
        if stmt:
            self.runner.run_script(cursor, stmt)

        return res

    def on_filling_object_refs(self):
        """Test injection point"""

    def fill_object_refs(self, conn, cursor, get_references):
        """Update the object_refs table by analyzing new transactions."""
        stmt = """
        SELECT transaction.tid
        FROM transaction
            LEFT JOIN object_refs_added
                ON (transaction.tid = object_refs_added.tid)
        WHERE object_refs_added.tid IS NULL
        ORDER BY transaction.tid
        """
        self.runner.run_script_stmt(cursor, stmt)
        tids = [tid for (tid,) in cursor]
        log_at = time.time() + 60
        if tids:
            self.on_filling_object_refs()
            tid_count = len(tids)
            txns_done = 0
            log.info(
                "pre_pack: analyzing references from objects in %d new "
                "transaction(s)", tid_count)
            for tid in tids:
                self._add_refs_for_tid(cursor, tid, get_references)
                txns_done += 1
                now = time.time()
                if now >= log_at:
                    # save the work done so far
                    conn.commit()
                    log_at = now + 60
                    log.info(
                        "pre_pack: transactions analyzed: %d/%d",
                        txns_done, tid_count)
            conn.commit()
            log.info(
                "pre_pack: transactions analyzed: %d/%d", txns_done, tid_count)

    def _add_refs_for_tid(self, cursor, tid, get_references):
        """Fill object_refs with all states for a transaction.

        Returns the number of references added.
        """
        log.debug("pre_pack: transaction %d: computing references ", tid)
        from_count = 0
        stmt = """
            SELECT zoid, state
            FROM object_state
            WHERE tid = %(tid)s
            """
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})

        add_rows = []  # [(from_oid, tid, to_oid)]
        for from_oid, state in self._fetchmany(cursor):
            state = db_binary_to_bytes(state)
            if hasattr(state, 'read'):
                # Oracle
                state = state.read() # pylint:disable=no-member
            if state:
                assert isinstance(state, bytes), type(state) # PY3: used to be str(state)
                from_count += 1
                try:
                    to_oids = get_references(state)
                except:
                    log.error(
                        "pre_pack: can't unpickle "
                        "object %d in transaction %d; state length = %d",
                        from_oid, tid, len(state))
                    raise
                for to_oid in to_oids:
                    add_rows.append((from_oid, tid, to_oid))

        # A previous pre-pack may have been interrupted.  Delete rows
        # from the interrupted attempt.
        stmt = "DELETE FROM object_ref WHERE tid = %(tid)s"
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})

        # Add the new references.
        stmt = """
        INSERT INTO object_ref (zoid, tid, to_zoid)
        VALUES (%s, %s, %s)
        """
        self.runner.run_many(cursor, stmt, add_rows)

        # The references have been computed for this transaction.
        stmt = """
        INSERT INTO object_refs_added (tid)
        VALUES (%(tid)s)
        """
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})

        to_count = len(add_rows)
        log.debug("pre_pack: transaction %d: has %d reference(s) "
                  "from %d object(s)", tid, to_count, from_count)
        return to_count

    @metricmethod
    def pre_pack(self, pack_tid, get_references):
        """Decide what to pack.

        pack_tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        The self.options.pack_gc flag indicates whether
        to run garbage collection.
        If pack_gc is false, at least one revision of every object is kept,
        even if nothing refers to it.  Packing with pack_gc disabled can be
        much faster.
        """
        conn, cursor = self.connmanager.open_for_pre_pack()
        try:
            try:
                if self.options.pack_gc:
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
                self.runner.run_script_stmt(cursor, stmt)
                to_remove = 0

                if self.options.pack_gc:
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
                    self.runner.run_script_stmt(
                        cursor, stmt, {'pack_tid': pack_tid})
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
                self.runner.run_script_stmt(
                    cursor, stmt, {'pack_tid': pack_tid})
                to_remove += cursor.rowcount

                log.info("pre_pack: enumerating transactions to pack")
                stmt = "%(TRUNCATE)s pack_state_tid"
                self.runner.run_script_stmt(cursor, stmt)
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
            self.connmanager.close(conn, cursor)


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
        self.runner.run_script(cursor, stmt, {'pack_tid': pack_tid})


    def _pre_pack_with_gc(self, conn, cursor, pack_tid, get_references):
        """Determine what to pack, with garbage collection.
        """
        stmt = self._script_create_temp_pack_visit
        if stmt:
            self.runner.run_script(cursor, stmt)

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

        -- Keep the root object.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid = 0;

        -- Keep objects that have been revised since pack_tid.
        -- Use temp_pack_visit for temporary state; otherwise MySQL 5 chokes.
        INSERT INTO temp_pack_visit (zoid, keep_tid)
        SELECT zoid, 0
        FROM current_object
        WHERE tid > %(pack_tid)s;

        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid IN (
            SELECT zoid
            FROM temp_pack_visit
        );

        %(TRUNCATE)s temp_pack_visit;

        -- Keep objects that are still referenced by object states in
        -- transactions that will not be packed.
        -- Use temp_pack_visit for temporary state; otherwise MySQL 5 chokes.
        INSERT INTO temp_pack_visit (zoid, keep_tid)
        SELECT DISTINCT to_zoid, 0
        FROM object_ref
        WHERE tid > %(pack_tid)s;

        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid IN (
            SELECT zoid
            FROM temp_pack_visit
        );

        %(TRUNCATE)s temp_pack_visit;
        """
        self.runner.run_script(cursor, stmt, {'pack_tid': pack_tid})

        # Traverse the graph, setting the 'keep' flags in pack_object
        self._traverse_graph(cursor)


    def _find_pack_tid(self):
        """If pack was not completed, find our pack tid again"""
        conn, cursor = self.connmanager.open_for_pre_pack()
        try:
            stmt = self._script_find_pack_tid
            self.runner.run_script_stmt(cursor, stmt)
            res = [tid for (tid,) in cursor]
        finally:
            self.connmanager.close(conn, cursor)
        return res and res[0] or 0


    @metricmethod
    def pack(self, pack_tid, sleep=None, packed_func=None):
        """Pack.  Requires the information provided by pre_pack."""
        # pylint:disable=too-many-locals
        # Read committed mode is sufficient.

        conn, cursor = self.connmanager.open()
        try: # pylint:disable=too-many-nested-blocks
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
                self.runner.run_script_stmt(
                    cursor, stmt, {'pack_tid': pack_tid})
                tid_rows = list(self._fetchmany(cursor))
                tid_rows.sort()  # oldest first

                total = len(tid_rows)
                log.info("pack: will pack %d transaction(s)", total)

                stmt = self._script_create_temp_pack_visit
                if stmt:
                    self.runner.run_script(cursor, stmt)

                # Hold the commit lock while packing to prevent deadlocks.
                # Pack in small batches of transactions only after we are able
                # to obtain a commit lock in order to minimize the
                # interruption of concurrent write operations.
                start = time.time()
                packed_list = []
                counter, lastreport, statecounter = 0, 0, 0
                # We'll report on progress in at most .1% step increments
                reportstep = max(total / 1000, 1)

                self._pause_pack_until_lock(cursor, sleep)
                for tid, packed, has_removable in tid_rows:
                    self._pack_transaction(
                        cursor, pack_tid, tid, packed, has_removable,
                        packed_list)
                    counter += 1
                    if time.time() >= start + self.options.pack_batch_timeout:
                        conn.commit()
                        if packed_func is not None:
                            for oid, tid in packed_list:
                                packed_func(oid, tid)
                        statecounter += len(packed_list)
                        if counter >= lastreport + reportstep:
                            log.info("pack: packed %d (%.1f%%) transaction(s), "
                                     "affecting %d states",
                                     counter, counter / float(total) * 100,
                                     statecounter)
                            lastreport = counter / reportstep * reportstep
                        del packed_list[:]
                        self.locker.release_commit_lock(cursor)
                        self._pause_pack_until_lock(cursor, sleep)
                        start = time.time()
                if packed_func is not None:
                    for oid, tid in packed_list:
                        packed_func(oid, tid)
                packed_list = None

                self._pack_cleanup(conn, cursor, sleep)

            except:
                log.exception("pack: failed")
                conn.rollback()
                raise

            else:
                log.info("pack: finished successfully")
                conn.commit()

        finally:
            self.connmanager.close(conn, cursor)


    def _pack_transaction(self, cursor, pack_tid, tid, packed,
                          has_removable, packed_list):
        """Pack one transaction.  Requires populated pack tables."""
        log.debug("pack: transaction %d: packing", tid)
        removed_objects = 0
        removed_states = 0

        if has_removable:
            stmt = self._script_pack_current_object
            self.runner.run_script_stmt(cursor, stmt, {'tid': tid})
            removed_objects = cursor.rowcount

            stmt = self._script_pack_object_state
            self.runner.run_script_stmt(cursor, stmt, {'tid': tid})
            removed_states = cursor.rowcount

            # Terminate prev_tid chains
            stmt = """
            UPDATE object_state SET prev_tid = 0
            WHERE prev_tid = %(tid)s
                AND tid <= %(pack_tid)s
            """
            self.runner.run_script_stmt(cursor, stmt,
                                        {'pack_tid': pack_tid, 'tid': tid})

            stmt = """
            SELECT pack_state.zoid
            FROM pack_state
            WHERE pack_state.tid = %(tid)s
            """
            self.runner.run_script_stmt(cursor, stmt, {'tid': tid})
            for (oid,) in self._fetchmany(cursor):
                packed_list.append((oid, tid))

        # Find out whether the transaction is empty
        stmt = self._script_transaction_has_data
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})
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
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})

        log.debug(
            "pack: transaction %d (%s): removed %d object(s) and %d state(s)",
            tid, state, removed_objects, removed_states)


    def _pack_cleanup(self, conn, cursor, sleep=None):
        """Remove unneeded table rows after packing"""
        # commit the work done so far
        conn.commit()
        self.locker.release_commit_lock(cursor)
        log.info("pack: cleaning up")

        # This section does not need to hold the commit lock, as it only
        # touches pack-specific tables. We already hold a pack lock for that.
        log.debug("pack: removing unused object references")
        stmt = self._script_pack_object_ref
        self.runner.run_script(cursor, stmt)

        # We need a commit lock when touching the transaction table though.
        # We'll do it in batches of 1000 rows.
        log.debug("pack: removing empty packed transactions")
        while True:
            self._pause_pack_until_lock(cursor, sleep)
            stmt = self._script_delete_empty_transactions_batch
            self.runner.run_script_stmt(cursor, stmt)
            deleted = cursor.rowcount
            conn.commit()
            self.locker.release_commit_lock(cursor)
            if deleted < 1000:
                # Last set of deletions complete
                break

        # perform cleanup that does not require the commit lock
        log.debug("pack: clearing temporary pack state")
        for _table in ('pack_object', 'pack_state', 'pack_state_tid'):
            stmt = '%(TRUNCATE)s ' + _table
            self.runner.run_script_stmt(cursor, stmt)


@implementer(IPackUndo)
class HistoryFreePackUndo(PackUndo):
    """
    History-free pack/undo.
    """

    keep_history = False

    _script_choose_pack_transaction = """
        SELECT tid
        FROM object_state
        WHERE tid > 0
            AND tid <= %(tid)s
        ORDER BY tid DESC
        LIMIT 1
        """

    _script_create_temp_pack_visit = """
        CREATE TEMPORARY TABLE temp_pack_visit (
            zoid BIGINT NOT NULL,
            keep_tid BIGINT NOT NULL
        );
        CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid);
        CREATE INDEX temp_pack_keep_tid ON temp_pack_visit (keep_tid)
        """

    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        raise UndoError("Undo is not supported by this storage")

    def undo(self, cursor, undo_tid, self_tid):
        """Undo a transaction.

        Parameters: "undo_tid", the integer tid of the transaction to undo,
        and "self_tid", the integer tid of the current transaction.

        Returns the list of OIDs undone.
        """
        raise UndoError("Undo is not supported by this storage")

    def on_filling_object_refs(self):
        """Test injection point"""

    def fill_object_refs(self, conn, cursor, get_references):
        """Update the object_refs table by analyzing new object states.

        Note that ZODB connections can change the object states while this
        method is running, possibly obscuring object references,
        so this method runs repeatedly until it detects no changes between
        two passes.
        """
        # XXX: We open the prepack connection in at most READ COMMITTED isolation mode,
        # which is why the state can be changing. Why do we do that and not use a
        # stronger isolation mode?
        holding_commit = False
        attempt = 0
        while True:
            attempt += 1
            if attempt >= 3 and not holding_commit:
                # Starting with the third attempt, hold the commit lock
                # to prevent changes.
                holding_commit = True
                self.locker.hold_commit_lock(cursor)

            stmt = """
            SELECT object_state.zoid FROM object_state
                LEFT JOIN object_refs_added
                    ON (object_state.zoid = object_refs_added.zoid)
            WHERE object_refs_added.tid IS NULL
                OR object_refs_added.tid != object_state.tid
            ORDER BY object_state.zoid
            """
            self.runner.run_script_stmt(cursor, stmt)
            oids = [oid for (oid,) in self._fetchmany(cursor)]
            log_at = time.time() + 60
            if oids:
                if attempt == 1:
                    self.on_filling_object_refs()
                oid_count = len(oids)
                oids_done = 0
                log.info(
                    "pre_pack: analyzing references from %d object(s)",
                    oid_count)
                while oids:
                    batch = oids[:100]
                    oids = oids[100:]
                    self._add_refs_for_oids(cursor, batch, get_references)
                    oids_done += len(batch)
                    now = time.time()
                    if now >= log_at:
                        # Save the work done so far.
                        conn.commit()
                        log_at = now + 60
                        log.info(
                            "pre_pack: objects analyzed: %d/%d",
                            oids_done, oid_count)
                conn.commit()
                log.info(
                    "pre_pack: objects analyzed: %d/%d", oids_done, oid_count)
            else:
                # No changes since last pass.
                break

        if holding_commit:
            # The above `conn.commit()` will have released the locks
            # under PostgreSQL and Oracle, this is necessary for MySQL.
            # See https://github.com/zodb/relstorage/pull/9/
            self.locker.release_commit_lock(cursor)

    def _add_refs_for_oids(self, cursor, oids, get_references):
        """Fill object_refs with the states for some objects.

        Returns the number of references added.
        """
        # XXX PY3: This could be tricky
        oid_list = ','.join(str(oid) for oid in oids)
        stmt = """
            SELECT zoid, tid, state
            FROM object_state
            WHERE zoid IN (%s)
            """ % oid_list
        self.runner.run_script_stmt(cursor, stmt)

        add_objects = []
        add_refs = []

        for from_oid, tid, state in self._fetchmany(cursor):
            state = db_binary_to_bytes(state)
            if hasattr(state, 'read'):
                # Oracle
                state = state.read() # pylint:disable=no-member
            add_objects.append((from_oid, tid))
            if state:
                assert isinstance(state, bytes), type(state)
                # XXX PY3 state = str(state)
                try:
                    to_oids = get_references(state)
                except:
                    log.error("pre_pack: can't unpickle "
                              "object %d in transaction %d; state length = %d",
                              from_oid, tid, len(state))
                    raise
                for to_oid in to_oids:
                    add_refs.append((from_oid, tid, to_oid))

        if not add_objects:
            return 0

        stmt = "DELETE FROM object_refs_added WHERE zoid IN (%s)" % oid_list
        self.runner.run_script_stmt(cursor, stmt)
        stmt = "DELETE FROM object_ref WHERE zoid IN (%s)" % oid_list
        self.runner.run_script_stmt(cursor, stmt)

        stmt = """
        INSERT INTO object_ref (zoid, tid, to_zoid) VALUES (%s, %s, %s)
        """
        self.runner.run_many(cursor, stmt, add_refs)

        stmt = """
        INSERT INTO object_refs_added (zoid, tid) VALUES (%s, %s)
        """
        self.runner.run_many(cursor, stmt, add_objects)

        return len(add_refs)

    @metricmethod
    def pre_pack(self, pack_tid, get_references):
        """Decide what the garbage collector should delete.

        Objects created or modified after pack_tid will not be
        garbage collected.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        The self.options.pack_gc flag indicates whether to run garbage
        collection.  If pack_gc is false, this method does nothing.
        """
        if not self.options.pack_gc:
            log.warning("pre_pack: garbage collection is disabled on a "
                        "history-free storage, so doing nothing")
            return

        conn, cursor = self.connmanager.open_for_pre_pack()
        try:
            try:
                self._pre_pack_main(conn, cursor, pack_tid, get_references)
            except:
                log.exception("pre_pack: failed")
                conn.rollback()
                raise
            else:
                conn.commit()
                log.info("pre_pack: finished successfully")
        finally:
            self.connmanager.close(conn, cursor)


    def _pre_pack_main(self, conn, cursor, pack_tid, get_references):
        """Determine what to garbage collect.
        """
        stmt = self._script_create_temp_pack_visit
        if stmt:
            self.runner.run_script(cursor, stmt)

        self.fill_object_refs(conn, cursor, get_references)

        log.info("pre_pack: filling the pack_object table")
        # Fill the pack_object table with all known OIDs.
        stmt = """
        %(TRUNCATE)s pack_object;

        INSERT INTO pack_object (zoid, keep, keep_tid)
        SELECT zoid, %(FALSE)s, tid
        FROM object_state;

        -- Keep the root object.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid = 0;

        -- Keep objects that have been revised since pack_tid.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE keep_tid > %(pack_tid)s;
        """
        self.runner.run_script(cursor, stmt, {'pack_tid': pack_tid})

        # Traverse the graph, setting the 'keep' flags in pack_object
        self._traverse_graph(cursor)


    def _find_pack_tid(self):
        """If pack was not completed, find our pack tid again"""

        # pack (below) ignores it's pack_tid argument, so we can safely
        # return None here
        return None


    @metricmethod
    def pack(self, pack_tid, sleep=None, packed_func=None):
        """Run garbage collection.

        Requires the information provided by pre_pack.
        """
        # pylint:disable=too-many-locals
        # Read committed mode is sufficient.
        conn, cursor = self.connmanager.open()
        try: # pylint:disable=too-many-nested-blocks
            try:
                stmt = """
                SELECT zoid, keep_tid
                FROM pack_object
                WHERE keep = %(FALSE)s
                """
                self.runner.run_script_stmt(cursor, stmt)
                to_remove = list(self._fetchmany(cursor))

                total = len(to_remove)
                log.info("pack: will remove %d object(s)", total)

                # Hold the commit lock while packing to prevent deadlocks.
                # Pack in small batches of transactions only after we are able
                # to obtain a commit lock in order to minimize the
                # interruption of concurrent write operations.
                start = time.time()
                packed_list = []
                # We'll report on progress in at most .1% step increments
                lastreport, reportstep = 0, max(total / 1000, 1)

                self._pause_pack_until_lock(cursor, sleep)
                while to_remove:
                    items = to_remove[:100]
                    del to_remove[:100]
                    stmt = """
                    DELETE FROM object_state
                    WHERE zoid = %s AND tid = %s
                    """
                    self.runner.run_many(cursor, stmt, items)
                    packed_list.extend(items)

                    if time.time() >= start + self.options.pack_batch_timeout:
                        conn.commit()
                        if packed_func is not None:
                            for oid, tid in packed_list:
                                packed_func(oid, tid)
                        del packed_list[:]
                        counter = total - len(to_remove)
                        if counter >= lastreport + reportstep:
                            log.info("pack: removed %d (%.1f%%) state(s)",
                                     counter, counter / float(total) * 100)
                            lastreport = counter / reportstep * reportstep
                        self.locker.release_commit_lock(cursor)
                        self._pause_pack_until_lock(cursor, sleep)
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
            self.connmanager.close(conn, cursor)


    def _pack_cleanup(self, conn, cursor):
        # commit the work done so far
        conn.commit()
        self.locker.release_commit_lock(cursor)
        log.info("pack: cleaning up")

        # This section does not need to hold the commit lock, as it only
        # touches pack-specific tables. We already hold a pack lock for that.
        stmt = """
        DELETE FROM object_refs_added
        WHERE zoid IN (
            SELECT zoid
            FROM pack_object
            WHERE keep = %(FALSE)s
        );

        DELETE FROM object_ref
        WHERE zoid IN (
            SELECT zoid
            FROM pack_object
            WHERE keep = %(FALSE)s
        );

        %(TRUNCATE)s pack_object
        """
        self.runner.run_script(cursor, stmt)

    def deleteObject(self, cursor, oid, oldserial):
        # The only things to worry about are object_state and blob_chuck.
        # blob chunks are deleted automatically by a foreign key.

        # We shouldn't *have* to verify the oldserial in the delete statement,
        # because our only consumer is zc.zodbdgc which only calls us for
        # unreachable objects, so they shouldn't be modified and get a new
        # TID. But this is safer.
        state = """
        DELETE FROM object_state
        WHERE zoid = %(oid)s
        and tid = %(tid)s
        """
        self.runner.run_script_stmt(cursor, state, {'oid': u64(oid), 'tid': u64(oldserial)})
        return cursor.rowcount
