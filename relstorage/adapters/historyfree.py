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
"""Code common to history-free adapters."""


import logging
import time
from relstorage.adapters.abstract import AbstractAdapter
from ZODB.POSException import UndoError

log = logging.getLogger(__name__)


class HistoryFreeAdapter(AbstractAdapter):
    """An abstract adapter that does not retain history.

    Derivatives should have at least the following schema::

        -- All object states in all transactions.
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL CHECK (tid > 0),
            state       BYTEA
        );

        -- A list of referenced OIDs from each object_state.
        -- This table is populated as needed during garbage collection.
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );

        -- The object_refs_added table tracks whether object_refs has
        -- been populated for all states in a given transaction.
        -- An entry is added only when the work is finished.
        CREATE TABLE object_refs_added (
            tid         BIGINT NOT NULL PRIMARY KEY
        );

        -- Temporary state during garbage collection:
        -- The list of all objects, a flag signifying whether
        -- the object should be kept, and a flag signifying whether
        -- the object's references have been visited.
        -- The keep_tid field specifies the current revision of the object.
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL DEFAULT FALSE,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        );
    """

    keep_history = False

    _scripts = {
        'create_temp_pack_visit': """
            CREATE TEMPORARY TABLE temp_pack_visit (
                zoid BIGINT NOT NULL
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
                )
            """,
    }

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
            log.warning("pre_pack: garbage collection is disabled on a "
                "history-free storage, so doing nothing")
            return

        conn, cursor = self.open_for_pre_pack()
        try:
            try:
                self._pre_pack_main(conn, cursor, get_references)
            except:
                log.exception("pre_pack: failed")
                conn.rollback()
                raise
            else:
                conn.commit()
                log.info("pre_pack: finished successfully")
        finally:
            self.close(conn, cursor)


    def _pre_pack_main(self, conn, cursor, get_references):
        """Determine what to garbage collect.
        """
        stmt = self._scripts['create_temp_pack_visit']
        if stmt:
            self._run_script(cursor, stmt)

        self.fill_object_refs(conn, cursor, get_references)

        log.info("pre_pack: filling the pack_object table")
        # Fill the pack_object table with all known OIDs.
        stmt = """
        %(TRUNCATE)s pack_object;

        INSERT INTO pack_object (zoid, keep_tid)
        SELECT zoid, tid
        FROM object_state;

        -- Keep the root object
        UPDATE pack_object SET keep = %(TRUE)s
        WHERE zoid = 0;
        """
        self._run_script(cursor, stmt)

        # Set the 'keep' flags in pack_object
        self._visit_all_references(cursor)


    def pack(self, pack_tid, options, sleep=time.sleep, packed_func=None):
        """Run garbage collection.

        Requires the information provided by _pre_gc.
        """

        # Read committed mode is sufficient.
        conn, cursor = self.open()
        try:
            try:
                stmt = """
                SELECT zoid, keep_tid
                FROM pack_object
                WHERE keep = %(FALSE)s
                """
                self._run_script_stmt(cursor, stmt)
                to_remove = list(cursor)

                log.info("pack: will remove %d object(s)", len(to_remove))

                # Hold the commit lock while packing to prevent deadlocks.
                # Pack in small batches of transactions in order to minimize
                # the interruption of concurrent write operations.
                start = time.time()
                packed_list = []
                self._hold_commit_lock(cursor)

                for item in to_remove:
                    oid, tid = item
                    stmt = """
                    DELETE FROM object_state
                    WHERE zoid = %(oid)s
                        AND tid = %(tid)s
                    """
                    self._run_script_stmt(
                        cursor, stmt, {'oid': oid, 'tid': tid})
                    packed_list.append(item)

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


    def _pack_cleaup(self, conn, cursor):
        # commit the work done so far
        conn.commit()
        self._release_commit_lock(cursor)
        self._hold_commit_lock(cursor)
        log.info("pack: cleaning up")

        stmt = """
        DELETE FROM object_refs_added
        WHERE tid NOT IN (
            SELECT DISTINCT tid
            FROM object_state
        );

        DELETE FROM object_ref
        WHERE zoid IN (
            SELECT zoid
            FROM pack_object
            WHERE keep = %(FALSE)s
        );

        %(TRUNCATE)s pack_object
        """
        self._run_script(cursor, stmt)
