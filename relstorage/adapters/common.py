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
"""Code common to most adapters."""

from ZODB.POSException import UndoError

import logging

log = logging.getLogger("relstorage.adapters.common")


class Adapter(object):
    """Common code for a database adapter.

    This is an abstract class; a lot of methods are expected to be
    provided by subclasses.
    """

    # _script_vars contains replacements for statements in scripts.
    # These are correct for PostgreSQL and MySQL but not for Oracle.
    _script_vars = {
        'TRUE':         'TRUE',
        'FALSE':        'FALSE',
        'OCTET_LENGTH': 'OCTET_LENGTH',
        'oid':          '%(oid)s',
        'tid':          '%(tid)s',
        'pack_tid':     '%(pack_tid)s',
        'undo_tid':     '%(undo_tid)s',
        'self_tid':     '%(self_tid)s',
    }

    _scripts = {
        'select_keep_tid': """
            SELECT tid
            FROM object_state
            WHERE zoid = pack_object.zoid
                AND tid > 0
                AND tid <= %(pack_tid)s
            ORDER BY tid DESC
            LIMIT 1
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

        'create_temp_pack_visit': """
            CREATE TEMPORARY TABLE temp_pack_visit (
                zoid BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid)
            """,

        'create_temp_undo': """
            CREATE TEMPORARY TABLE temp_undo (
                zoid BIGINT NOT NULL,
                prev_tid BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX temp_undo_zoid ON temp_undo (zoid)
            """,

        'reset_temp_undo': "DROP TABLE temp_undo",
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


    def iter_transactions(self, cursor):
        """Iterate over the transaction log.

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
        return iter(cursor)


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
        return iter(cursor)


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

        Returns the list of OIDs undone.
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

        -- Add new undo records.
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT temp_undo.zoid, %(self_tid)s, current_object.tid,
            prev.md5, prev.state
        FROM temp_undo
            JOIN current_object ON (temp_undo.zoid = current_object.zoid)
            LEFT JOIN object_state prev
                ON (prev.zoid = temp_undo.zoid
                    AND prev.tid = temp_undo.prev_tid);

        -- List the changed OIDs.
        SELECT zoid FROM object_state WHERE tid = %(undo_tid)s
        """
        self._run_script(cursor, stmt,
            {'undo_tid': undo_tid, 'self_tid': self_tid})
        res = [oid_int for (oid_int,) in cursor]

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


    def pre_pack(self, pack_tid, get_references, gc=True):
        """Decide what to pack.

        Subclasses may override this.

        tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        gc is a boolean indicating whether to run garbage collection.
        If gc is false, at least one revision of every object is kept,
        even if nothing refers to it.  Packing with gc disabled can be
        much faster.
        """
        conn, cursor = self.open()
        try:
            try:
                if gc:
                    self._pre_pack_with_gc(cursor, pack_tid, get_references)
                else:
                    self._pre_pack_without_gc(cursor, pack_tid)
            except:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            self.close(conn, cursor)


    def _pre_pack_without_gc(self, cursor, pack_tid):
        """Determine what to pack, without garbage collection.

        With garbage collection disabled, there is no need to follow
        object references.
        """
        # Fill the pack_object table with OIDs, but configure them
        # all to be kept by setting keep and keep_tid.
        stmt = """
        DELETE FROM pack_object;

        INSERT INTO pack_object (zoid, keep)
        SELECT DISTINCT zoid, %(TRUE)s
        FROM object_state
        WHERE tid <= %(pack_tid)s;

        UPDATE pack_object SET keep_tid = (@select_keep_tid@)
        """
        stmt = stmt.replace(
            '@select_keep_tid@', self._scripts['select_keep_tid'])
        self._run_script(cursor, stmt, {'pack_tid': pack_tid})


    def _pre_pack_with_gc(self, cursor, pack_tid, get_references):
        """Determine what to pack, with garbage collection.
        """
        # Fill object_ref with references from object states
        # in transactions that will not be packed.
        self._fill_nonpacked_refs(cursor, pack_tid, get_references)

        # Fill the pack_object table with OIDs that either will be
        # removed (if nothing references the OID) or whose history will
        # be cut.
        stmt = """
        DELETE FROM pack_object;

        INSERT INTO pack_object (zoid, keep)
        SELECT DISTINCT zoid, %(FALSE)s
        FROM object_state
        WHERE tid <= %(pack_tid)s;

        -- If the root object is in pack_object, keep it.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid = 0;

        -- Keep objects that have been revised since pack_tid.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE keep = %(FALSE)s
            AND zoid IN (
                SELECT zoid
                FROM current_object
                WHERE tid > %(pack_tid)s
            );

        -- Keep objects that are still referenced by object states in
        -- transactions that will not be packed.
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE keep = %(FALSE)s
            AND zoid IN (
                SELECT to_zoid
                FROM object_ref
                WHERE tid > %(pack_tid)s
            );
        """
        self._run_script(cursor, stmt, {'pack_tid': pack_tid})

        stmt = self._scripts['create_temp_pack_visit']
        if stmt:
            self._run_script(cursor, stmt)

        # Each of the packable objects to be kept might
        # refer to other objects.  If some of those references
        # include objects currently set to be removed, keep
        # those objects as well.  Do this
        # repeatedly until all references have been satisfied.
        while True:

            # Make a list of all parent objects that still need
            # to be visited.  Then set keep_tid for all pack_object
            # rows with keep = true.
            # keep_tid must be set before _fill_pack_object_refs examines
            # references.
            stmt = """
            DELETE FROM temp_pack_visit;

            INSERT INTO temp_pack_visit (zoid)
            SELECT zoid
            FROM pack_object
            WHERE keep = %(TRUE)s
                AND keep_tid IS NULL;

            UPDATE pack_object SET keep_tid = (@select_keep_tid@)
            WHERE keep = %(TRUE)s AND keep_tid IS NULL
            """
            stmt = stmt.replace(
                '@select_keep_tid@', self._scripts['select_keep_tid'])
            self._run_script(cursor, stmt, {'pack_tid': pack_tid})

            self._fill_pack_object_refs(cursor, get_references)

            # Visit the children of all parent objects that were
            # just visited.
            stmt = """
            UPDATE pack_object SET keep = %(TRUE)s
            WHERE keep = %(FALSE)s
                AND zoid IN (
                    SELECT DISTINCT to_zoid
                    FROM object_ref
                        JOIN temp_pack_visit USING (zoid)
                )
            """
            self._run_script_stmt(cursor, stmt)
            if not cursor.rowcount:
                # No new references detected.
                break


    def _fill_nonpacked_refs(self, cursor, pack_tid, get_references):
        """Fill object_ref for all transactions that will not be packed."""
        stmt = """
        SELECT DISTINCT tid
        FROM object_state
        WHERE tid > %(pack_tid)s
            AND NOT EXISTS (
                SELECT 1
                FROM object_refs_added
                WHERE tid = object_state.tid
            )
        """
        self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})
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


    def _add_object_ref_rows(self, cursor, add_rows):
        """Add rows to object_ref.

        The input rows are tuples containing (from_zoid, tid, to_zoid).

        Subclasses can override this.
        """
        stmt = """
        INSERT INTO object_ref (zoid, tid, to_zoid)
        VALUES (%s, %s, %s)
        """
        cursor.executemany(stmt, add_rows)


    def _add_refs_for_tid(self, cursor, tid, get_references):
        """Fill object_refs with all states for a transaction.
        """
        stmt = """
        SELECT zoid, state
        FROM object_state
        WHERE tid = %(tid)s
        """
        self._run_script_stmt(cursor, stmt, {'tid': tid})

        add_rows = []  # [(from_oid, tid, to_oid)]
        for from_oid, state in cursor:
            if hasattr(state, 'read'):
                # cx_Oracle detail
                state = state.read()
            if state:
                to_oids = get_references(str(state))
                for to_oid in to_oids:
                    add_rows.append((from_oid, tid, to_oid))

        if add_rows:
            self._add_object_ref_rows(cursor, add_rows)

        # The references have been computed for this transaction.
        stmt = """
        INSERT INTO object_refs_added (tid)
        VALUES (%(tid)s)
        """
        self._run_script_stmt(cursor, stmt, {'tid': tid})


    def _hold_commit_lock(self, cursor):
        """Hold the commit lock for packing"""
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")


    def pack(self, pack_tid):
        """Pack.  Requires populated pack tables."""

        # Read committed mode is sufficient.
        conn, cursor = self.open()
        try:
            try:
                # hold the commit lock for a moment to prevent deadlocks.
                self._hold_commit_lock(cursor)

                for table in ('object_ref', 'current_object', 'object_state'):

                    # Remove objects that are in pack_object and have keep
                    # set to false.
                    stmt = """
                    DELETE FROM %s
                    WHERE zoid IN (
                            SELECT zoid
                            FROM pack_object
                            WHERE keep = %%(FALSE)s
                        )
                    """ % table
                    self._run_script_stmt(cursor, stmt)

                    if table != 'current_object':
                        # Cut the history of objects in pack_object that
                        # have keep set to true.
                        stmt = """
                        DELETE FROM %s
                        WHERE zoid IN (
                                SELECT zoid
                                FROM pack_object
                                WHERE keep = %%(TRUE)s
                            )
                            AND tid < (
                                SELECT keep_tid
                                FROM pack_object
                                WHERE zoid = %s.zoid
                            )
                        """ % (table, table)
                        self._run_script_stmt(cursor, stmt)

                stmt = """
                -- Terminate prev_tid chains
                UPDATE object_state SET prev_tid = 0
                WHERE tid <= %(pack_tid)s
                    AND prev_tid != 0;

                -- For each tid to be removed, delete the corresponding row in
                -- object_refs_added.
                DELETE FROM object_refs_added
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = object_refs_added.tid
                    );

                -- Delete transactions no longer used.
                DELETE FROM transaction
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = transaction.tid
                    );

                -- Mark the remaining packable transactions as packed
                UPDATE transaction SET packed = %(TRUE)s
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                    AND packed = %(FALSE)s;

                -- Clean up.
                DELETE FROM pack_object;
                """
                self._run_script(cursor, stmt, {'pack_tid': pack_tid})

            except:
                conn.rollback()
                raise

            else:
                conn.commit()

        finally:
            self.close(conn, cursor)
