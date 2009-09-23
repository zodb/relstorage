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

import logging
import time

try:
    from hashlib import md5
except ImportError:
    from md5 import new as md5


log = logging.getLogger(__name__)

# Notes about adapters:
#
# An adapter must not hold a connection, cursor, or database state, because
# RelStorage opens multiple concurrent connections using a single adapter
# instance.
# Within the context of an adapter, all OID and TID values are integers,
# not binary strings, except as noted.

class AbstractAdapter(object):
    """Common code for a database adapter.
    """

    keep_history = None  # True or False
    verify_sane_database = False

    # _script_vars contains replacements for statements in scripts.
    # These are correct for PostgreSQL and MySQL but not for Oracle.
    _script_vars = {
        'TRUE':         'TRUE',
        'FALSE':        'FALSE',
        'OCTET_LENGTH': 'OCTET_LENGTH',
        'TRUNCATE':     'TRUNCATE',
        'oid':          '%(oid)s',
        'tid':          '%(tid)s',
        'pack_tid':     '%(pack_tid)s',
        'undo_tid':     '%(undo_tid)s',
        'self_tid':     '%(self_tid)s',
        'min_tid':      '%(min_tid)s',
        'max_tid':      '%(max_tid)s',
    }

    def _run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        params should be either an empty tuple (no parameters) or
        a map.

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

        params should be either an empty tuple (no parameters) or
        a map.

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

    def _run_many(self, cursor, stmt, items):
        """Execute a statement repeatedly.  Items should be a list of tuples.

        stmt should use '%s' parameter format.  Overridden by adapters
        that use a different parameter format.
        """
        cursor.executemany(stmt, items)

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

    def md5sum(self, data):
        if data is not None:
            return md5(data).hexdigest()
        else:
            # George Bailey object
            return None

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

    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).

        Subclasses may override this.
        """
        return self.open()

    def _hold_commit_lock(self, cursor):
        """Hold the commit lock for packing"""
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")

    def _release_commit_lock(self, cursor):
        """Release the commit lock during packing"""
        # no action needed
        pass


    def fill_object_refs(self, conn, cursor, get_references):
        """Update the object_refs table by analyzing new transactions."""
        if self.keep_history:
            stmt = """
            SELECT transaction.tid
            FROM transaction
                LEFT JOIN object_refs_added
                    ON (transaction.tid = object_refs_added.tid)
            WHERE object_refs_added.tid IS NULL
            ORDER BY transaction.tid
            """
        else:
            stmt = """
            SELECT transaction.tid
            FROM (SELECT DISTINCT tid FROM object_state) AS transaction
                LEFT JOIN object_refs_added
                    ON (transaction.tid = object_refs_added.tid)
            WHERE object_refs_added.tid IS NULL
            ORDER BY transaction.tid
            """

        self._run_script_stmt(cursor, stmt)
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
                try:
                    to_oids = get_references(str(state))
                except:
                    log.error("pre_pack: can't unpickle "
                        "object %d in transaction %d; state length = %d" % (
                        from_oid, tid, len(state)))
                    raise
                if self.keep_history:
                    for to_oid in to_oids:
                        add_rows.append((from_oid, tid, to_oid))
                else:
                    for to_oid in to_oids:
                        add_rows.append((from_oid, to_oid))

        if self.keep_history:
            stmt = """
            INSERT INTO object_ref (zoid, tid, to_zoid)
            VALUES (%s, %s, %s)
            """
            self._run_many(cursor, stmt, add_rows)

        else:
            stmt = """
            DELETE FROM object_ref
            WHERE zoid in (
                SELECT zoid
                FROM object_state
                WHERE tid = %(tid)s
                )
            """
            self._run_script(cursor, stmt, {'tid': tid})

            stmt = """
            INSERT INTO object_ref (zoid, to_zoid)
            VALUES (%s, %s)
            """
            self._run_many(cursor, stmt, add_rows)

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


    def _visit_all_references(self, cursor):
        """Visit all references in pack_object and set the keep flags.
        """
        # Each of the objects to be kept might refer to other objects.
        # Mark the referenced objects to be kept as well. Do this
        # repeatedly until all references have been satisfied.
        pass_num = 1
        while True:
            log.info("pre_pack: following references, pass %d", pass_num)

            # Make a list of all parent objects that still need to be
            # visited. Then set pack_object.visited for all pack_object
            # rows with keep = true.
            stmt = """
            %(TRUNCATE)s temp_pack_visit;

            INSERT INTO temp_pack_visit (zoid, keep_tid)
            SELECT zoid, keep_tid
            FROM pack_object
            WHERE keep = %(TRUE)s
                AND visited = %(FALSE)s;

            UPDATE pack_object SET visited = %(TRUE)s
            WHERE keep = %(TRUE)s
                AND visited = %(FALSE)s
            """
            self._run_script(cursor, stmt)
            visit_count = cursor.rowcount

            if self.verify_sane_database:
                # Verify the update actually worked.
                # MySQL 5.1.23 fails this test; 5.1.24 passes.
                stmt = """
                SELECT 1
                FROM pack_object
                WHERE keep = %(TRUE)s AND visited = %(FALSE)s
                """
                self._run_script_stmt(cursor, stmt)
                if list(cursor):
                    raise AssertionError(
                        "database failed to update pack_object")

            log.debug("pre_pack: checking references from %d object(s)",
                visit_count)

            # Visit the children of all parent objects that were
            # just visited.
            stmt = self._scripts['pre_pack_follow_child_refs']
            self._run_script(cursor, stmt)
            found_count = cursor.rowcount

            log.debug("pre_pack: found %d more referenced object(s) in "
                "pass %d", found_count, pass_num)
            if not found_count:
                # No new references detected.
                break
            else:
                pass_num += 1


    def _pause_pack(self, sleep, options, start):
        """Pause packing to allow concurrent commits."""
        elapsed = time.time() - start
        if elapsed == 0.0:
            # Compensate for low timer resolution by
            # assuming that at least 10 ms elapsed.
            elapsed = 0.01
        duty_cycle = options.pack_duty_cycle
        if duty_cycle > 0.0 and duty_cycle < 1.0:
            delay = min(options.pack_max_delay,
                elapsed * (1.0 / duty_cycle - 1.0))
            if delay > 0:
                log.debug('pack: sleeping %.4g second(s)', delay)
                sleep(delay)


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

        if self.keep_history:
            stmt = "SELECT 1 FROM transaction WHERE tid = %(tid)s"
        else:
            stmt = "SELECT 1 FROM object_state WHERE tid <= %(tid)s LIMIT 1"
        cursor.execute(intern(stmt % self._script_vars),
            {'tid': prev_polled_tid})
        rows = cursor.fetchall()
        if not rows:
            # Transaction not found; perhaps it has been packed.
            # The connection cache needs to be cleared.
            return None, new_polled_tid

        # Get the list of changed OIDs and return it.
        if ignore_tid is None:
            stmt = """
            SELECT zoid
            FROM current_object
            WHERE tid > %(tid)s
            """
            cursor.execute(intern(stmt % self._script_vars),
                {'tid': prev_polled_tid})
        else:
            stmt = """
            SELECT zoid
            FROM current_object
            WHERE tid > %(tid)s
                AND tid != %(self_tid)s
            """
            cursor.execute(intern(stmt % self._script_vars),
                {'tid': prev_polled_tid, 'self_tid': ignore_tid})
        oids = [oid for (oid,) in cursor]

        return oids, new_polled_tid
