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
from __future__ import absolute_import

import logging
import time

from perfmetrics import metricmethod
from ZODB.POSException import UndoError
from ZODB.utils import u64
from zope.interface import implementer

from ..iter import fetchmany
from ..treemark import TreeMarker
from .interfaces import IPackUndo
from ._util import DatabaseHelpersMixin

# pylint:disable=too-many-lines,unused-argument


log = logging.getLogger(__name__)

class PackUndo(DatabaseHelpersMixin):
    """Abstract base class for pack/undo"""

    verify_sane_database = False

    _script_choose_pack_transaction = None

    _lock_for_share = 'FOR SHARE'
    _lock_for_update = 'FOR UPDATE'

    def __init__(self, database_driver, connmanager, runner, locker, options):
        self.driver = database_driver
        self.connmanager = connmanager
        self.runner = runner
        self.locker = locker
        self.options = options

    def _fetchmany(self, cursor):
        return fetchmany(cursor)

    def with_options(self, options):
        """
        Return a new instance that will use the given options, instead
        of the options originally constructed.
        """
        if options == self.options:
            # If the options haven't changed, return ourself. This is
            # for tests that make changes to the structure of this
            # object not captured in the constructor or options.
            # (checkPackWhileReferringObjectChanges)
            return self
        return self.__class__(self.driver, self.connmanager, self.runner, self.locker, options)

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

    # Subclasses (notably Oracle) can define this to provide hints
    # that affect graph traversal.
    #
    # We cannot include the hints Oracle wants as standard; /*+ ... */
    # is also the syntax for a MySQL 5.7 optimizer hint, but FULL(...)
    # isn't valid syntax, so it produces a warning (and some
    # frameworks/drivers want to treat warnings as errors, or print
    # them).
    #
    # The alternate comment syntax for Oracle hints, --+ ..., isn't a
    # valid MySQL comment (it requires whitespace after --) and raises
    # a syntax error.
    #
    # PostgreSQL doesn't have hints, so this is a no-op there.
    _traverse_graph_optimizer_hint = ''

    def _traverse_graph(self, cursor):
        """Visit the entire object graph to find out what should be kept.

        Sets the pack_object.keep flags.
        """
        log.info("pre_pack: downloading pack_object and object_ref.")

        marker = TreeMarker()

        # Download the graph of object references into the TreeMarker.
        # TODO: We can probably do much or most of this in SQL, at least
        # in recent databases that support recursive WITH queries?

        # XXX: In history-free mode, ``pack_object`` contains exactly
        # the set of OIDs that are present in ``object_state`` and I
        # *think* that ``pack_object.keep_tid`` is always going to be equal to
        # ``object_ref.tid``. We may get better behaviour if we join
        # against that table here
        stmt = """
        SELECT {}
            object_ref.zoid, object_ref.to_zoid
        FROM object_ref
            JOIN pack_object ON (object_ref.zoid = pack_object.zoid)
        WHERE object_ref.tid >= pack_object.keep_tid
        ORDER BY object_ref.zoid, object_ref.to_zoid
        """.format(self._traverse_graph_optimizer_hint)
        self.runner.run_script_stmt(cursor, stmt)
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            marker.add_refs(rows)

        # Use the TreeMarker to find all reachable objects, starting
        # with the ones that are known reachable. These are the roots:
        #
        # - ZOID 0 which is explicitly marked as such
        #
        # - In history preserving databases where we are not doing GC,
        #   this includes all objects (except those explicitly
        #   deleted) --- but we don't actually call this method for
        #   the HP-no-gc case.
        #
        # - In history preserving *with* gc, this is all objects that
        #   have been modified after the pack time or are referenced
        #   from objects that have been modified after the pack time.
        #
        # - In history free *with* gc, this is all objects that have
        #   been modified after the pack time.
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
            # XXX: No, this is very wrong. Use the RowBatcher
            # Alternately, flush to a table and then join
            # against that at the end.
            oids_str = ','.join(str(oid) for oid in batch)
            del batch[:]
            stmt = """
            UPDATE pack_object
            SET keep = %%(TRUE)s,
                visited = %%(TRUE)s
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

    # The only things to worry about are object_state and blob_chuck.
    # blob chunks are deleted automatically by a foreign key.

    # We shouldn't *have* to verify the oldserial in the delete statement,
    # because our only consumer is zc.zodbdgc which only calls us for
    # unreachable objects, so they shouldn't be modified and get a new
    # TID. But it's safer to do so.
    _delete_object_stmt = None

    def deleteObject(self, cursor, oid, oldserial):
        self.runner.run_script_stmt(
            cursor,
            self._delete_object_stmt,
            {'oid': u64(oid), 'tid': u64(oldserial)})
        return cursor.rowcount

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
        zoid BIGINT NOT NULL PRIMARY KEY,
        keep_tid BIGINT NOT NULL
    );
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
    SELECT 1
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
        ORDER BY pack_state.zoid
    )
    """

    _script_pack_object_state = """
    DELETE FROM object_state
    WHERE tid = %(tid)s
    AND zoid in (
        SELECT pack_state.zoid
        FROM pack_state
        WHERE pack_state.tid = %(tid)s
        ORDER BY pack_state.zoid
    )
    """

    _script_pack_object_ref = """
    DELETE FROM object_refs_added
    WHERE tid IN (
        SELECT tid
        FROM transaction
        WHERE is_empty = %(TRUE)s
    );

    DELETE FROM object_ref
    WHERE tid IN (
        SELECT tid
        FROM transaction
        WHERE is_empty = %(TRUE)s
    );
        """

    # Previously we used `= ANY(ARRAY(...))`, as was once recommended,
    # (See http://www.postgres.cz/index.php/PostgreSQL_SQL_Tricks#Fast_first_n_rows_removing)
    # but that is no longer recommended or expected to be faster.
    # Also, it was postgres specific. Now we use a more standard syntax,
    # that lets us preserve order (in case that matters).
    _script_delete_empty_transactions_batch = """
    DELETE FROM transaction
    WHERE tid IN (
        SELECT tid FROM transaction
        WHERE packed = %(TRUE)s
        AND is_empty = %(TRUE)s
        ORDER BY tid
        LIMIT 1000
    )
    """

    @metricmethod
    def verify_undoable(self, cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn."""
        stmt = """
        SELECT 1
        FROM transaction
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
        INNER JOIN object_state cur_os
            ON (prev_os.zoid = cur_os.zoid)
        INNER JOIN current_object
            ON (cur_os.zoid = current_object.zoid
                AND cur_os.tid = current_object.tid)
        WHERE prev_os.tid = %(undo_tid)s
            AND cur_os.md5 != prev_os.md5
        ORDER BY prev_os.zoid
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
        WHERE tid = %(undo_tid)s
        ORDER BY zoid;

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
        WHERE zoid IN (SELECT zoid FROM temp_undo ORDER BY zoid)
        AND tid = %(self_tid)s;

        DELETE FROM object_state
        WHERE zoid IN (SELECT zoid FROM temp_undo ORDER BY zoid)
            AND tid = %(self_tid)s;

        -- Copy old states forward.
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state_size, state)
        SELECT temp_undo.zoid, %(self_tid)s, current_object.tid,
            md5, COALESCE(state_size, 0), state
        FROM temp_undo
        INNER JOIN current_object ON (temp_undo.zoid = current_object.zoid)
        LEFT OUTER JOIN object_state
            ON (object_state.zoid = temp_undo.zoid
                AND object_state.tid = temp_undo.prev_tid)
        ORDER BY current_object.zoid;

        -- Copy old blob chunks forward.
        INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
        SELECT temp_undo.zoid, %(self_tid)s, chunk_num, chunk
        FROM temp_undo
            JOIN blob_chunk
                ON (blob_chunk.zoid = temp_undo.zoid
                    AND blob_chunk.tid = temp_undo.prev_tid);

        -- List the copied states.
        SELECT zoid, prev_tid
        FROM temp_undo;
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
        LEFT OUTER JOIN object_refs_added
            ON (transaction.tid = object_refs_added.tid)
        WHERE object_refs_added.tid IS NULL
        ORDER BY transaction.tid
        """
        self.runner.run_script_stmt(cursor, stmt)
        tids = [tid for (tid,) in cursor]
        log_at = time.time() + 60
        tid_count = len(tids)
        txns_done = 0
        if tids:
            self.on_filling_object_refs()
            log.info(
                "pre_pack: analyzing references from objects in %d new "
                "transaction(s)", tid_count)
            for tid in tids:
                self._add_refs_for_tid(cursor, tid, get_references)
                txns_done += 1
                now = time.time()
                if now >= log_at:
                    # save the work done so far
                    self.connmanager.commit(conn, cursor)
                    log_at = now + 60
                    log.info(
                        "pre_pack: transactions analyzed: %d/%d",
                        txns_done, tid_count)
        self.connmanager.commit(conn, cursor)
        log.info("pre_pack: transactions analyzed: %d/%d", txns_done, tid_count)

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
        ORDER BY zoid
        """
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})

        add_rows = []  # [(from_oid, tid, to_oid)]
        for from_oid, state in self._fetchmany(cursor):
            state = self.driver.binary_column_as_state_type(state)
            if state:
                assert isinstance(state, self.driver.state_types), type(state)
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
        # TODO: Use RowBatcher
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
            # The pre-pack functions are responsible for managing
            # their own commits; when they return, the transaction
            # should be committed.
            #
            # ``pack_object`` should be populated,
            # essentially with the distinct list of all objects and their
            # maximum (newest) transaction ids.
            if self.options.pack_gc:
                log.info("pre_pack: start with gc enabled")
                self._pre_pack_with_gc(
                    conn, cursor, pack_tid, get_references)
            else:
                log.info("pre_pack: start without gc")
                self._pre_pack_without_gc(
                    conn, cursor, pack_tid)

            log.info("pre_pack: enumerating states to pack")
            stmt = "%(TRUNCATE)s pack_state"
            self.runner.run_script_stmt(cursor, stmt)
            to_remove = 0

            if self.options.pack_gc:
                # Mark all objects we said not to keep as something
                # we should discard.
                stmt = """
                INSERT INTO pack_state (tid, zoid)
                SELECT tid, zoid
                FROM object_state
                INNER JOIN pack_object USING (zoid)
                WHERE keep = %(FALSE)s
                    AND tid > 0
                    AND tid <= %(pack_tid)s
                ORDER BY zoid
                """
                self.runner.run_script_stmt(
                    cursor, stmt, {'pack_tid': pack_tid})
                to_remove += cursor.rowcount
            else:
                # Support for IExternalGC. Also remove deleted objects.
                stmt = """
                INSERT INTO pack_state (tid, zoid)
                SELECT t.tid, t.zoid
                FROM (
                    SELECT zoid, tid
                    FROM object_state
                    WHERE state IS NULL
                    AND tid = (
                        SELECT MAX(i.tid)
                        FROM object_state i
                        WHERE i.zoid = object_state.zoid
                    )
                ) t
                """
                self.runner.run_script_stmt(cursor, stmt)
                to_remove += cursor.rowcount

            # Pack object states with the keep flag set to true,
            # excluding their current TID.
            stmt = """
            INSERT INTO pack_state (tid, zoid)
            SELECT tid, zoid
            FROM object_state
            INNER JOIN pack_object USING (zoid)
            WHERE keep = %(TRUE)s
                AND tid > 0
                AND tid != keep_tid
                AND tid <= %(pack_tid)s
            ORDER BY zoid
            """
            self.runner.run_script_stmt(
                cursor, stmt, {'pack_tid': pack_tid})
            to_remove += cursor.rowcount

            # Make a simple summary of the transactions to examine.
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

            log.info("pre_pack: finished successfully")
            self.connmanager.commit(conn, cursor)
        except:
            self.connmanager.rollback_quietly(conn, cursor)
            conn, cursor = None, None
            raise
        finally:
            self.connmanager.close(conn, cursor)

    def __initial_populate_pack_object(self, conn, cursor, pack_tid, keep):
        """
        Put all objects into ``pack_object`` that have revisions equal
        to or below *pack_tid*, setting their initial ``keep`` status
        to *keep*.

        Commits the transaction to release locks.
        """
        # Access the tables that are used by online transactions
        # in a short transaction and immediately commit to release any
        # locks.

        # TRUNCATE may or may not cause implicit commits. (MySQL: Yes,
        # PostgreSQL: No)
        self.runner.run_script(cursor, "%(TRUNCATE)s pack_object;")

        affected_objects = """
        SELECT zoid, tid
        FROM object_state
        WHERE tid > 0 AND tid <= %(pack_tid)s
        ORDER BY zoid
        """

        # Take the locks we need up front, in order, because
        # locking in a subquery doing an INSERT isn't guaranteed to use that
        # order (deadlocks seen with commits on MySQL 5.7 without this,
        # when using REPEATABLE READ.)
        #
        # We must do this on its own, because some drivers (notably
        # mysql-connector-python) get very upset
        # ("mysql.connector.errors.InternalError: Unread result
        # found") if you issue a SELECT that you don't then consume.
        #
        # Since we switched MySQL back to READ COMMITTED (what PostgreSQL uses)
        # I haven't been able to produce the error anymore. So don't explicitly lock.

        stmt = """
        INSERT INTO pack_object (zoid, keep, keep_tid)
        SELECT zoid, """ + ('%(TRUE)s' if keep else '%(FALSE)s') + """, MAX(tid)
        FROM ( """ + affected_objects + """ ) t
        GROUP BY zoid;

        -- Keep the root object.
        UPDATE pack_object
        SET keep = %(TRUE)s
        WHERE zoid = 0;
        """
        self.runner.run_script(cursor, stmt, {'pack_tid': pack_tid})
        self.connmanager.commit(conn, cursor)

    def _pre_pack_without_gc(self, conn, cursor, pack_tid):
        """
        Determine what to pack, without garbage collection.

        With garbage collection disabled, there is no need to follow
        object references.
        """
        # Fill the pack_object table with OIDs, but configure them
        # all to be kept by setting keep to true.
        log.debug("pre_pack: populating pack_object")
        self.__initial_populate_pack_object(conn, cursor, pack_tid, keep=True)

    def _pre_pack_with_gc(self, conn, cursor, pack_tid, get_references):
        """
        Determine what to pack, with garbage collection.
        """
        stmt = self._script_create_temp_pack_visit
        if stmt:
            self.runner.run_script(cursor, stmt)

        self.fill_object_refs(conn, cursor, get_references)

        log.info("pre_pack: filling the pack_object table")
        # Fill the pack_object table with OIDs that either will be
        # removed (if nothing references the OID) or whose history will
        # be cut.
        self.__initial_populate_pack_object(conn, cursor, pack_tid, keep=False)

        stmt = """
        -- Keep objects that have been revised since pack_tid.
        -- Use temp_pack_visit for temporary state; otherwise MySQL 5 chokes.
        INSERT INTO temp_pack_visit (zoid, keep_tid)
        SELECT zoid, 0
        FROM current_object
        WHERE tid > %(pack_tid)s
        ORDER BY zoid;

        UPDATE pack_object
        SET keep = %(TRUE)s
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

        UPDATE pack_object
        SET keep = %(TRUE)s
        WHERE zoid IN (
            SELECT zoid
            FROM temp_pack_visit
        );

        %(TRUNCATE)s temp_pack_visit;
        """
        self.runner.run_script(cursor, stmt, {'pack_tid': pack_tid})

        # Traverse the graph, setting the 'keep' flags in pack_object
        self._traverse_graph(cursor)
        self.connmanager.commit(conn, cursor)

    def _find_pack_tid(self):
        """If pack was not completed, find our pack tid again"""
        conn, cursor = self.connmanager.open_for_pre_pack()
        try:
            stmt = self._script_find_pack_tid
            self.runner.run_script_stmt(cursor, stmt)
            res = [tid for (tid,) in cursor]
        finally:
            self.connmanager.close(conn, cursor)
        return res[0] if res else 0


    @metricmethod
    def pack(self, pack_tid, packed_func=None):
        """Pack.  Requires the information provided by pre_pack."""
        # pylint:disable=too-many-locals
        # Read committed mode is sufficient.

        conn, cursor = self.connmanager.open_for_store()
        try: # pylint:disable=too-many-nested-blocks
            try:
                # If we have a transaction entry in ``pack_state_tid`` (that is,
                # we found a transaction with an object in the range of transactions
                # we can pack away) that matches an actual transaction entry (XXX:
                # How could we be in the state where the transaction row is gone but we still
                # have object_state with that transaction id?), then we need to pack that
                # transaction. The presence of an entry in ``pack_state_tid`` means that all
                # object states from that transaction should be removed.
                stmt = """
                SELECT transaction.tid,
                       CASE WHEN packed = %(TRUE)s THEN 1 ELSE 0 END,
                       CASE WHEN pack_state_tid.tid IS NOT NULL THEN 1 ELSE 0 END
                FROM transaction
                LEFT OUTER JOIN pack_state_tid ON (transaction.tid = pack_state_tid.tid)
                WHERE transaction.tid > 0
                    AND transaction.tid <= %(pack_tid)s
                    AND (packed = %(FALSE)s OR pack_state_tid.tid IS NOT NULL)
                ORDER BY transaction.tid
                """
                self.runner.run_script_stmt(
                    cursor, stmt, {'pack_tid': pack_tid})
                tid_rows = list(self._fetchmany(cursor)) # oldest first, sorted in SQL

                total = len(tid_rows)
                log.info("pack: will pack %d transaction(s)", total)

                stmt = self._script_create_temp_pack_visit
                if stmt:
                    self.runner.run_script(cursor, stmt)

                # Lock and delete rows in the same order that
                # new commits would in order to prevent deadlocks.
                # Pack in small batches of transactions only after we are able
                # to obtain a commit lock in order to minimize the
                # interruption of concurrent write operations.
                start = time.time()
                packed_list = []
                counter, lastreport, statecounter = 0, 0, 0
                # We'll report on progress in at most .1% step increments
                reportstep = max(total / 1000, 1)

                for tid, packed, has_removable in tid_rows:
                    self._pack_transaction(
                        cursor, pack_tid, tid, packed, has_removable,
                        packed_list)
                    counter += 1
                    if time.time() >= start + self.options.pack_batch_timeout:
                        self.connmanager.commit(conn, cursor)
                        if packed_func is not None:
                            for poid, ptid in packed_list:
                                packed_func(poid, ptid)
                        statecounter += len(packed_list)
                        if counter >= lastreport + reportstep:
                            log.info("pack: packed %d (%.1f%%) transaction(s), "
                                     "affecting %d states",
                                     counter, counter / float(total) * 100,
                                     statecounter)
                            lastreport = counter / reportstep * reportstep
                        del packed_list[:]
                        start = time.time()
                if packed_func is not None:
                    for oid, tid in packed_list:
                        packed_func(oid, tid)
                packed_list = None

                self._pack_cleanup(conn, cursor)

            except:
                log.exception("pack: failed")
                self.connmanager.rollback_quietly(conn, cursor)
                raise

            else:
                log.info("pack: finished successfully")
                self.connmanager.commit(conn, cursor)

        finally:
            self.connmanager.close(conn, cursor)


    def _pack_transaction(self, cursor, pack_tid, tid, packed,
                          has_removable, packed_list):
        """
        Pack one transaction. Requires populated pack tables.

        If *has_removable* is true, then we have object states and current
        object pointers to remove.
        """
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
            clause = 'is_empty = %(TRUE)s'
            state = 'empty'
        else:
            clause = 'is_empty = %(FALSE)s'
            state = 'not empty'
        stmt = "UPDATE transaction SET packed = %(TRUE)s, " + clause
        stmt += " WHERE tid = %(tid)s"
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})

        log.debug(
            "pack: transaction %d (%s): removed %d object(s) and %d state(s)",
            tid, state, removed_objects, removed_states)


    def _pack_cleanup(self, conn, cursor):
        """Remove unneeded table rows after packing"""
        # commit the work done so far, releasing row-level locks.
        self.connmanager.commit(conn, cursor)
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
            stmt = self._script_delete_empty_transactions_batch
            self.runner.run_script_stmt(cursor, stmt)
            deleted = cursor.rowcount
            self.connmanager.commit(conn, cursor)
            self.locker.release_commit_lock(cursor)
            if deleted < 1000:
                # Last set of deletions complete
                break

        # perform cleanup that does not require the commit lock
        log.debug("pack: clearing temporary pack state")
        for _table in ('pack_object', 'pack_state', 'pack_state_tid'):
            stmt = '%(TRUNCATE)s ' + _table
            self.runner.run_script_stmt(cursor, stmt)

    _delete_object_stmt = """
    UPDATE object_state
    SET state = NULL,
        state_size = 0,
        md5 = ''
    WHERE zoid = %(oid)s
    and tid = %(tid)s
    """


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
            zoid BIGINT NOT NULL PRIMARY KEY,
            keep_tid BIGINT NOT NULL
        );
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
        """
        Update the object_refs table by analyzing new object states.
        """
        # XXX: We open the prepack connection in at most READ
        # COMMITTED isolation mode, which is why the state can be
        # changing and hence we need a share lock. Why do we do that and not use
        # a stronger isolation mode? Is the share lock even needed? We just
        # need to be consistent with a single point in time
        #
        # Before we used shared locks, we would repeat the query a few
        # times, watching for changes, and if we kept getting changes,
        # we would hold the commit lock for an iteration; but we'd
        # still release the commit lock at the end of this method, so
        # they'd be free to mutate again.
        #
        # TODO: Maybe use 'SKIP LOCKED' here? As it stands, the shared
        # locks are still going to prevent modifications to existing objects,
        # just as if we held the commit lock.
        #
        # XXX: The above is outdated. There seems to be no need to rely on
        # multiple retries or locks at all, so long as we get an initial
        # consistent snapshot. An optimization, though, might be to
        # collect the TID here and then discard later?


        # Recall pre_pack can be run many times.
        stmt = """
        SELECT object_state.zoid FROM object_state
        LEFT OUTER JOIN object_refs_added
            ON (object_state.zoid = object_refs_added.zoid)
        WHERE object_refs_added.tid IS NULL
          OR object_refs_added.tid != object_state.tid
        ORDER BY object_state.zoid
        """ + self._lock_for_share
        self.runner.run_script_stmt(cursor, stmt)
        oids = list(r[0] for r in cursor)
        self.connmanager.commit(conn, cursor) # Release row locks
        log_at = time.time() + 60
        if oids:
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
                    self.connmanager.commit(conn, cursor)
                    log_at = now + 60
                    log.info(
                        "pre_pack: objects analyzed: %d/%d",
                        oids_done, oid_count)
            self.connmanager.commit(conn, cursor) # Release all the share locks.
            log.info(
                "pre_pack: objects analyzed: %d/%d", oids_done, oid_count)

    def _add_refs_for_oids(self, cursor, oids, get_references):
        """Fill object_refs with the states for some objects.

        Returns the number of references added.
        """
        # XXX PY3: This could be tricky
        # TODO: Use the row batcher's SELECT FROM
        oid_list = ','.join(str(oid) for oid in oids)
        stmt = """
            SELECT zoid, tid, state
            FROM object_state
            WHERE zoid IN (%s)
            ORDER BY zoid
            """ % oid_list
        self.runner.run_script_stmt(cursor, stmt)

        add_objects = []
        add_refs = []

        for from_oid, tid, state in self._fetchmany(cursor):
            state = self.driver.binary_column_as_state_type(state)
            add_objects.append((from_oid, tid))
            if state:
                assert isinstance(state, self.driver.state_types), type(state)
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

        # TODO: RowBatcher for all of these
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
                self.connmanager.rollback_quietly(conn, cursor)
                raise
            else:
                self.connmanager.commit(conn, cursor)
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
        FROM object_state
        ORDER BY zoid;

        -- Keep the root object.
        UPDATE pack_object
        SET keep = %(TRUE)s
        WHERE zoid = 0;

        -- Keep objects that have been revised since pack_tid.
        UPDATE pack_object
        SET keep = %(TRUE)s
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
    def pack(self, pack_tid, packed_func=None):
        """Run garbage collection.

        Requires the information provided by pre_pack.
        """
        # pylint:disable=too-many-locals
        # Read committed mode is sufficient.
        conn, cursor = self.connmanager.open_for_store()
        try: # pylint:disable=too-many-nested-blocks
            try:
                stmt = """
                SELECT zoid, keep_tid
                FROM pack_object
                WHERE keep = %(FALSE)s
                ORDER BY zoid
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

                while to_remove:
                    # TODO: Use the row batcher for this,
                    # or simply do a join against the table.
                    items = to_remove[:100]
                    del to_remove[:100]
                    # XXX: History free. We shouldn't need to include the TID,
                    # but that's probably there to protect against concurrent modification
                    # of the object. With our row-level locking in 3.0 this may not strictly be
                    # necessary?
                    stmt = """
                    DELETE FROM object_state
                    WHERE zoid = %s AND tid = %s
                    """
                    self.runner.run_many(cursor, stmt, items)
                    packed_list.extend(items)

                    if time.time() >= start + self.options.pack_batch_timeout:
                        self.connmanager.commit(conn, cursor)
                        if packed_func is not None:
                            for oid, tid in packed_list:
                                packed_func(oid, tid)
                        del packed_list[:]
                        counter = total - len(to_remove)
                        if counter >= lastreport + reportstep:
                            log.info("pack: removed %d (%.1f%%) state(s)",
                                     counter, counter / float(total) * 100)
                            lastreport = counter / reportstep * reportstep
                        start = time.time()

                if packed_func is not None:
                    for oid, tid in packed_list:
                        packed_func(oid, tid)
                packed_list = None

                self._pack_cleanup(conn, cursor)

            except:
                log.exception("pack: failed")
                self.connmanager.rollback_quietly(conn, cursor)
                raise

            else:
                log.info("pack: finished successfully")
                self.connmanager.commit(conn, cursor)
        finally:
            self.connmanager.close(conn, cursor)


    def _pack_cleanup(self, conn, cursor):
        # commit the work done so far
        self.connmanager.commit(conn, cursor)
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


    _delete_object_stmt = """
    DELETE FROM object_state
    WHERE zoid = %(oid)s
    and tid = %(tid)s
    """
