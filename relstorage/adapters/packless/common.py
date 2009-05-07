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
"""Code common to most adapters."""

from ZODB.POSException import UndoError

import logging
import time

log = logging.getLogger("relstorage.adapters.packless.common")

verify_sane_database = False


# Notes about adapters:
#
# An adapter must not hold a connection, cursor, or database state, because
# RelStorage opens multiple concurrent connections using a single adapter
# instance.
# Within the context of an adapter, all OID and TID values are integers,
# not binary strings, except as noted.

class PacklessAdapter(object):
    """Common code for a packless database adapter.

    This is an abstract class; a lot of methods are expected to be
    provided by subclasses.
    """

    # _script_vars contains replacements for statements in scripts.
    # These are correct for PostgreSQL and MySQL but not for Oracle.
    _script_vars = {
        'TRUE':         'TRUE',
        'FALSE':        'FALSE',
        'OCTET_LENGTH': 'OCTET_LENGTH',
        'TRUNCATE':     'TRUNCATE',
        'oid':          '%(oid)s',
        'tid':          '%(tid)s',
        'self_tid':     '%(self_tid)s',
        'min_tid':      '%(min_tid)s',
        'max_tid':      '%(max_tid)s',
    }

    _scripts = {
        'create_temp_gc_visit': """
            CREATE TEMPORARY TABLE temp_gc_visit (
                zoid BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX temp_gc_visit_zoid ON temp_gc_visit (zoid)
            """,

        'pre_gc_follow_child_refs': """
            UPDATE gc_object SET keep = %(TRUE)s
            WHERE keep = %(FALSE)s
                AND zoid IN (
                    SELECT DISTINCT to_zoid
                    FROM object_ref
                        JOIN temp_gc_visit USING (zoid)
                )
            """,

        'gc': """
            DELETE FROM object_state
            WHERE zoid IN (
                SELECT zoid
                FROM gc_object
                WHERE keep = %(FALSE)s
            );

            DELETE FROM object_refs_added
            WHERE tid NOT IN (
                SELECT DISTINCT tid
                FROM object_state
            );

            DELETE FROM object_ref
            WHERE zoid IN (
                SELECT zoid
                FROM gc_object
                WHERE keep = %(FALSE)s
            );

            %(TRUNCATE)s gc_object
            """,
    }

    def _run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        Subclasses may override this.
        The input statement is generic and needs to be transformed
        into a database-specific statement.
        """
        stmt = generic_stmt % self._script_vars
        try:
            cursor.execute(stmt, generic_params)
        except:
            log.warning("script statement failed: %r; parameters: %r",
                stmt, generic_params)
            raise


    def _run_script(self, cursor, script, params=()):
        """Execute a series of statements in the database.

        The statements are transformed by _run_script_stmt
        before execution.
        """
        lines = []
        for line in script.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            if line.endswith(';'):
                line = line[:-1]
                lines.append(line)
                stmt = '\n'.join(lines)
                self._run_script_stmt(cursor, stmt, params)
                lines = []
            else:
                lines.append(line)
        if lines:
            stmt = '\n'.join(lines)
            self._run_script_stmt(cursor, stmt, params)

    def _open_and_call(self, callback):
        """Call a function with an open connection and cursor.

        If the function returns, commits the transaction and returns the
        result returned by the function.
        If the function raises an exception, aborts the transaction
        then propagates the exception.
        """
        conn, cursor = self.open()
        try:
            try:
                res = callback(conn, cursor)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
                return res
        finally:
            self.close(conn, cursor)


    def iter_transactions(self, cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields (tid, username, description, extension) for each transaction.
        """
        stmt = """
        SELECT DISTINCT tid
        FROM object_state
        ORDER BY tid DESC
        """
        self._run_script_stmt(cursor, stmt)
        return ((tid, '', '', '') for (tid,) in cursor)


    def iter_transactions_range(self, cursor, start=None, stop=None):
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields (tid, username, description, extension, packed)
        for each transaction.
        """
        stmt = """
        SELECT DISTINCT tid
        FROM object_state
        WHERE tid > 0
        """
        if start is not None:
            stmt += " AND tid >= %(min_tid)s"
        if stop is not None:
            stmt += " AND tid <= %(max_tid)s"
        stmt += " ORDER BY tid"
        self._run_script_stmt(cursor, stmt,
            {'min_tid': start, 'max_tid': stop})
        return ((tid, '', '', '', True) for (tid,) in cursor)


    def iter_object_history(self, cursor, oid):
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """
        stmt = """
        SELECT tid, %(OCTET_LENGTH)s(state)
        FROM object_state
        WHERE zoid = %(oid)s
        """
        self._run_script_stmt(cursor, stmt, {'oid': oid})
        return ((tid, '', '', '', size) for (tid, size) in cursor)


    def iter_objects(self, cursor, tid):
        """Iterate over object states in a transaction.

        Yields (oid, prev_tid, state) for each object state.
        """
        stmt = """
        SELECT zoid, state
        FROM object_state
        WHERE tid = %(tid)s
        ORDER BY zoid
        """
        self._run_script_stmt(cursor, stmt, {'tid': tid})
        for oid, state in cursor:
            if hasattr(state, 'read'):
                # Oracle
                state = state.read()
            yield oid, state


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


    def choose_pack_transaction(self, pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """
        return 1


    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).

        Subclasses may override this.
        """
        return self.open()


    def pre_pack(self, pack_tid, get_references, options):
        """Decide what the garbage collector should delete.

        pack_tid is ignored.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        options is an instance of relstorage.Options.
        The options.pack_gc flag indicates whether to run garbage collection.
        If pack_gc is false, this method does nothing.
        """
        if not options.pack_gc:
            log.warning("pre_gc: garbage collection is disabled")
            return

        conn, cursor = self.open_for_pre_pack()
        try:
            try:
                self._pre_gc(conn, cursor, get_references)
                conn.commit()

                stmt = """
                SELECT COUNT(1)
                FROM gc_object
                WHERE keep = %(FALSE)s
                """
                self._run_script_stmt(cursor, stmt)
                to_remove = cursor.fetchone()[0]

                log.info("pre_gc: will remove %d object(s)",
                    to_remove)

            except:
                log.exception("pre_gc: failed")
                conn.rollback()
                raise
            else:
                log.info("pre_gc: finished successfully")
                conn.commit()
        finally:
            self.close(conn, cursor)


    def _pre_gc(self, conn, cursor, get_references):
        """Determine what to garbage collect.
        """
        stmt = self._scripts['create_temp_gc_visit']
        if stmt:
            self._run_script(cursor, stmt)

        self.fill_object_refs(conn, cursor, get_references)

        log.info("pre_gc: filling the gc_object table")
        # Fill the gc_object table with all known OIDs.
        stmt = """
        %(TRUNCATE)s gc_object;

        INSERT INTO gc_object (zoid)
        SELECT zoid
        FROM object_state;

        -- Keep the root object
        UPDATE gc_object SET keep = %(TRUE)s
        WHERE zoid = 0;
        """
        self._run_script(cursor, stmt)

        # Each of the objects to be kept might
        # refer to other objects.  If some of those references
        # include objects currently set to be removed, mark
        # the referenced objects to be kept as well.  Do this
        # repeatedly until all references have been satisfied.
        pass_num = 1
        while True:
            log.info("pre_gc: following references, pass %d", pass_num)

            # Make a list of all parent objects that still need
            # to be visited.  Then set gc_object.visited for all gc_object
            # rows with keep = true.
            stmt = """
            %(TRUNCATE)s temp_gc_visit;

            INSERT INTO temp_gc_visit (zoid)
            SELECT zoid
            FROM gc_object
            WHERE keep = %(TRUE)s
                AND visited = %(FALSE)s;

            UPDATE gc_object SET visited = %(TRUE)s
            WHERE keep = %(TRUE)s
                AND visited = %(FALSE)s
            """
            self._run_script(cursor, stmt)
            visit_count = cursor.rowcount

            if verify_sane_database:
                # Verify the update actually worked.
                # MySQL 5.1.23 fails this test; 5.1.24 passes.
                stmt = """
                SELECT 1
                FROM gc_object
                WHERE keep = %(TRUE)s AND visited = %(FALSE)s
                """
                self._run_script_stmt(cursor, stmt)
                if list(cursor):
                    raise AssertionError(
                        "database failed to update gc_object")

            log.debug("pre_gc: checking references from %d object(s)",
                visit_count)

            # Visit the children of all parent objects that were
            # just visited.
            stmt = self._scripts['pre_gc_follow_child_refs']
            self._run_script(cursor, stmt)
            found_count = cursor.rowcount

            log.debug("pre_gc: found %d more referenced object(s) in "
                "pass %d", found_count, pass_num)
            if not found_count:
                # No new references detected.
                break
            else:
                pass_num += 1


    def _add_object_ref_rows(self, cursor, add_rows):
        """Add rows to object_ref.

        The input rows are tuples containing (from_zoid, to_zoid).

        Subclasses can override this.
        """
        stmt = """
        INSERT INTO object_ref (zoid, to_zoid)
        VALUES (%s, %s)
        """
        cursor.executemany(stmt, add_rows)


    def _add_refs_for_tid(self, cursor, tid, get_references):
        """Fill object_refs with all states for a transaction.

        Returns the number of references added.
        """
        log.debug("pre_gc: transaction %d: computing references ", tid)
        from_count = 0

        stmt = """
        SELECT zoid, state
        FROM object_state
        WHERE tid = %(tid)s
        """
        self._run_script_stmt(cursor, stmt, {'tid': tid})

        add_rows = []  # [(from_oid, to_oid)]
        for from_oid, state in cursor:
            if hasattr(state, 'read'):
                # Oracle
                state = state.read()
            if state:
                from_count += 1
                to_oids = get_references(str(state))
                for to_oid in to_oids:
                    add_rows.append((from_oid, to_oid))

        if add_rows:
            stmt = """
            DELETE FROM object_ref
            WHERE zoid in (
                SELECT zoid
                FROM object_state
                WHERE tid = %(tid)s
                )
            """
            self._run_script_stmt(cursor, stmt, {'tid': tid})
            self._add_object_ref_rows(cursor, add_rows)

        # The references have been computed for this transaction.
        stmt = """
        INSERT INTO object_refs_added (tid)
        VALUES (%(tid)s)
        """
        self._run_script_stmt(cursor, stmt, {'tid': tid})

        to_count = len(add_rows)
        log.debug("pre_gc: transaction %d: has %d reference(s) "
            "from %d object(s)", tid, to_count, from_count)
        return to_count


    def fill_object_refs(self, conn, cursor, get_references):
        """Update the object_refs table by analyzing new transactions."""
        stmt = """
        SELECT transaction.tid
        FROM (SELECT DISTINCT tid FROM object_state) AS transaction
            LEFT JOIN object_refs_added
                ON (transaction.tid = object_refs_added.tid)
        WHERE object_refs_added.tid IS NULL
        ORDER BY transaction.tid
        """
        cursor.execute(stmt)
        tids = [tid for (tid,) in cursor]
        if tids:
            added = 0
            log.info("discovering references from objects in %d "
                "transaction(s)" % len(tids))
            for tid in tids:
                added += self._add_refs_for_tid(cursor, tid, get_references)
                if added >= 10000:
                    # save the work done so far
                    conn.commit()
                    added = 0
            if added:
                conn.commit()


    def _hold_commit_lock(self, cursor):
        """Hold the commit lock for gc"""
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")


    def _release_commit_lock(self, cursor):
        """Release the commit lock during gc"""
        # no action needed
        pass


    def pack(self, pack_tid, options, sleep=time.sleep):
        """Run garbage collection.

        Requires the information provided by _pre_gc.
        """

        # Read committed mode is sufficient.
        conn, cursor = self.open()
        try:
            try:
                log.info("gc: running")
                stmt = self._scripts['gc']
                self._run_script(cursor, stmt)

            except:
                log.exception("gc: failed")
                conn.rollback()
                raise

            else:
                log.info("gc: finished successfully")
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

        Returns (changed_oids, new_polled_tid).
        """
        # find out the tid of the most recent transaction.
        cursor.execute(self._poll_query)
        new_polled_tid = cursor.fetchone()[0]

        if prev_polled_tid is None:
            # This is the first time the connection has polled.
            return None, new_polled_tid

        if new_polled_tid == prev_polled_tid:
            # No transactions have been committed since prev_polled_tid.
            return (), new_polled_tid

        # Get the list of changed OIDs and return it.
        if ignore_tid is None:
            stmt = """
            SELECT zoid
            FROM object_state
            WHERE tid > %(tid)s
            """
            cursor.execute(intern(stmt % self._script_vars),
                {'tid': prev_polled_tid})
        else:
            stmt = """
            SELECT zoid
            FROM object_state
            WHERE tid > %(tid)s
                AND tid != %(self_tid)s
            """
            cursor.execute(intern(stmt % self._script_vars),
                {'tid': prev_polled_tid, 'self_tid': ignore_tid})
        oids = [oid for (oid,) in cursor]

        return oids, new_polled_tid
