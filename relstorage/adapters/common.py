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


# Notes about adapters:
#
# An adapter must not hold a connection, cursor, or database state, because
# RelStorage opens multiple concurrent connections using a single adapter
# instance.
# Within the context of an adapter, all OID and TID values are integers,
# not binary strings, except as noted.

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
        'min_tid':      '%(min_tid)s',
        'max_tid':      '%(max_tid)s',
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
        Yields (tid, packed, username, description, extension)
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
        SELECT zoid FROM temp_undo
        """
        self._run_script(cursor, stmt,
            {'undo_tid': undo_tid, 'self_tid': self_tid})
        res = [oid for (oid,) in cursor]

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


    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).

        Subclasses may override this.
        """
        return self.open()


    def pre_pack(self, pack_tid, get_references, gc):
        """Decide what to pack.

        tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        gc is a boolean indicating whether to run garbage collection.
        If gc is false, at least one revision of every object is kept,
        even if nothing refers to it.  Packing with gc disabled can be
        much faster.
        """
        conn, cursor = self.open_for_pre_pack()
        try:
            try:
                if gc:
                    log.info("pre_pack: start with gc enabled")
                    self._pre_pack_with_gc(
                        conn, cursor, pack_tid, get_references)
                else:
                    log.info("pre_pack: start without gc")
                    self._pre_pack_without_gc(
                        conn, cursor, pack_tid)
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
        # all to be kept by setting keep and keep_tid.
        log.debug("pre_pack: populating pack_object")
        subselect = self._scripts['select_keep_tid']
        stmt = """
        DELETE FROM pack_object;

        INSERT INTO pack_object (zoid, keep)
        SELECT DISTINCT zoid, %(TRUE)s
        FROM object_state
        WHERE tid <= %(pack_tid)s;

        UPDATE pack_object SET keep_tid = (""" + subselect + """)
        """
        self._run_script(cursor, stmt, {'pack_tid': pack_tid})


    def _pre_pack_with_gc(self, conn, cursor, pack_tid, get_references):
        """Determine what to pack, with garbage collection.
        """
        log.info("pre_pack: following references after the pack point")
        # Fill object_ref with references from object states
        # in transactions that will not be packed.
        self._fill_nonpacked_refs(conn, cursor, pack_tid, get_references)

        log.debug("pre_pack: populating pack_object")
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
        pass_num = 1
        while True:
            log.info("pre_pack: following references before the pack point, "
                "pass %d", pass_num)

            # Make a list of all parent objects that still need
            # to be visited.  Then set keep_tid for all pack_object
            # rows with keep = true.
            # keep_tid must be set before _fill_pack_object_refs examines
            # references.
            subselect = self._scripts['select_keep_tid']
            stmt = """
            DELETE FROM temp_pack_visit;

            INSERT INTO temp_pack_visit (zoid)
            SELECT zoid
            FROM pack_object
            WHERE keep = %(TRUE)s
                AND keep_tid IS NULL;

            UPDATE pack_object SET keep_tid = (""" + subselect + """)
            WHERE keep = %(TRUE)s AND keep_tid IS NULL;

            SELECT COUNT(1) FROM temp_pack_visit
            """
            self._run_script(cursor, stmt, {'pack_tid': pack_tid})
            visit_count = cursor.fetchone()[0]
            log.debug("pre_pack: checking references from %d object(s)",
                visit_count)

            self._fill_pack_object_refs(conn, cursor, get_references)

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
            found_count = cursor.rowcount

            log.info("pre_pack: found %d more referenced object(s) in "
                "pass %d", found_count, pass_num)
            if not found_count:
                # No new references detected.
                break
            else:
                pass_num += 1


    def _fill_nonpacked_refs(self, conn, cursor, pack_tid, get_references):
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
        tids = [tid for (tid,) in cursor]
        self._add_refs_for_tids(conn, cursor, tids, get_references)


    def _fill_pack_object_refs(self, conn, cursor, get_references):
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
        tids = [tid for (tid,) in cursor]
        self._add_refs_for_tids(conn, cursor, tids, get_references)


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

        Returns the number of references added.
        """
        log.debug("pre_pack: transaction %d: computing references ", tid)
        from_count = 0

        stmt = """
        SELECT zoid, state
        FROM object_state
        WHERE tid = %(tid)s
        """
        self._run_script_stmt(cursor, stmt, {'tid': tid})

        add_rows = []  # [(from_oid, tid, to_oid)]
        for from_oid, state in cursor:
            if hasattr(state, 'read'):
                # Oracle
                state = state.read()
            if state:
                from_count += 1
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

        to_count = len(add_rows)
        log.debug("pre_pack: transaction %d: has %d reference(s) "
            "from %d object(s)", tid, to_count, from_count)
        return to_count


    def _add_refs_for_tids(self, conn, cursor, tids, get_references):
        """Fill object_refs with all states for multiple transactions."""
        if tids:
            added = 0
            log.info("pre_pack: examining all references from objects in %d "
                "transaction(s)" % len(tids))
            for tid in tids:
                added += self._add_refs_for_tid(cursor, tid, get_references)
                if added >= 1000:
                    # save the work done so far
                    conn.commit()
                    added = 0


    def _hold_commit_lock(self, cursor):
        """Hold the commit lock for packing"""
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")


    def pack(self, pack_tid):
        """Pack.  Requires populated pack tables."""

        # Read committed mode is sufficient.
        conn, cursor = self.open()
        try:
            try:
                log.info("pack: start, pack_tid = %d", pack_tid)

                stmt = """
                SELECT COUNT(1)
                FROM transaction
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})
                transaction_count = cursor.fetchone()[0]

                stmt = """
                SELECT COUNT(1)
                FROM pack_object
                WHERE keep = %(FALSE)s
                """
                self._run_script_stmt(cursor, stmt)
                delete_count = cursor.fetchone()[0]

                stmt = """
                SELECT COUNT(1)
                FROM pack_object
                WHERE keep = %(TRUE)s
                """
                self._run_script_stmt(cursor, stmt)
                trim_count = cursor.fetchone()[0]

                log.info(
                    "pack: will pack %d transaction(s), delete %s object(s),"
                        " and trim %s old object(s)",
                        transaction_count, delete_count, trim_count)

                # hold the commit lock for a moment to prevent deadlocks.
                self._hold_commit_lock(cursor)

                for table in ('object_ref', 'current_object', 'object_state'):

                    if delete_count > 0:
                        # Remove objects that are in pack_object and have keep
                        # set to false.
                        log.debug("pack: deleting objects from %s", table)
                        stmt = """
                        DELETE FROM %s
                        WHERE zoid IN (
                                SELECT zoid
                                FROM pack_object
                                WHERE keep = %%(FALSE)s
                            )
                        """ % table
                        self._run_script_stmt(cursor, stmt)

                    if trim_count > 0 and table != 'current_object':
                        # Cut the history of objects in pack_object that
                        # have keep set to true.
                        log.debug("pack: trimming objects in %s", table)
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

                log.debug("pack: terminating prev_tid chains")
                stmt = """
                UPDATE object_state SET prev_tid = 0
                WHERE tid <= %(pack_tid)s
                    AND prev_tid != 0
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})

                # For each tid to be removed, delete the corresponding row in
                # object_refs_added.
                log.debug("pack: deleting from object_refs_added")
                stmt = """
                DELETE FROM object_refs_added
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = object_refs_added.tid
                    )
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})

                log.debug("pack: deleting transactions")
                stmt = """
                DELETE FROM transaction
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM object_state
                        WHERE tid = transaction.tid
                    )
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})

                log.debug("pack: marking transactions as packed")
                stmt = """
                UPDATE transaction SET packed = %(TRUE)s
                WHERE tid > 0
                    AND tid <= %(pack_tid)s
                    AND packed = %(FALSE)s
                """
                self._run_script_stmt(cursor, stmt, {'pack_tid': pack_tid})

                log.debug("pack: clearing pack_object")
                cursor.execute("DELETE FROM pack_object")

            except:
                log.exception("pack: failed")
                conn.rollback()
                raise

            else:
                log.info("pack: finished successfully")
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

        stmt = "SELECT 1 FROM transaction WHERE tid = %(tid)s"
        cursor.execute(stmt % self._script_vars, {'tid': prev_polled_tid})
        rows = cursor.fetchall()
        if not rows:
            # Transaction not found; perhaps it has been packed.
            # The connection cache needs to be cleared.
            return None, new_polled_tid

        # Get the list of changed OIDs and return it.
        stmt = """
        SELECT DISTINCT zoid
        FROM object_state
        WHERE tid > %(tid)s
        """
        if ignore_tid is None:
            cursor.execute(stmt % self._script_vars, {'tid': prev_polled_tid})
        else:
            stmt += " AND tid != %(self_tid)s"
            cursor.execute(stmt % self._script_vars,
                {'tid': prev_polled_tid, 'self_tid': ignore_tid})
        oids = [oid for (oid,) in cursor]

        return oids, new_polled_tid
