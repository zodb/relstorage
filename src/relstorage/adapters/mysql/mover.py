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
"""IObjectMover implementation.
"""
from __future__ import print_function, absolute_import

from relstorage.adapters.interfaces import IObjectMover
from zope.interface import implementer
import os

from ..mover import AbstractObjectMover
from ..mover import metricmethod_sampled
from .._util import query_property
from .._util import noop_when_history_free


def to_prepared_queries(name, queries, extension=''):

    return [
        'PREPARE ' + name + ' FROM "' + x.replace('%s', '?') + extension + '"'
        for x in queries
    ]

@implementer(IObjectMover)
class MySQLObjectMover(AbstractObjectMover):

    _prepare_detect_conflict_queries = to_prepared_queries(
        'detect_conflicts',
        AbstractObjectMover._detect_conflict_queries,
        ' LOCK IN SHARE MODE')

    _prepare_detect_conflict_query = query_property('_prepare_detect_conflict')

    _detect_conflict_query = 'EXECUTE detect_conflicts'

    on_load_opened_statement_names = ()

    on_store_opened_statement_names = on_load_opened_statement_names
    on_store_opened_statement_names += ('_prepare_detect_conflict_query',)

    @metricmethod_sampled
    def on_store_opened(self, cursor, restart=False):
        """Create the temporary table for storing objects"""
        if restart:
            # TRUNCATE is a DDL statement, even against a temporary
            # table, and as such does an implicit transaction commit.
            # Normally we want to avoid that, but here its OK since
            # this method is called between transactions, as it were.
            # TRUNCATE benchmarks (zodbshoot add) substantially faster
            # (10157) than a DELETE (75xx) and moderately faster than
            # a DROP/CREATE (9457). TRUNCATE is in the replication
            # logs like a DROP/CREATE. (DROP TEMPORARY TABLE is *not*
            # DDL and not transaction ending).
            stmt = "TRUNCATE TABLE temp_store"
            cursor.execute(stmt)
            stmt = "TRUNCATE TABLE temp_blob_chunk"
            cursor.execute(stmt)
        else:
            # InnoDB tables benchmark much faster for concurrency=2
            # and 6 than MyISAM tables under both MySQL 5.5 and 5.7,
            # at least on OS X 10.12. The OS X filesystem is single
            # threaded, though, so the effects of flushing MyISAM tables
            # to disk on every operation are probably magnified.

            # note that the md5 column is not used if self.keep_history == False.
            stmt = """
            CREATE TEMPORARY TABLE temp_store (
                zoid        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                prev_tid    BIGINT UNSIGNED NOT NULL,
                md5         CHAR(32),
                state       LONGBLOB
            ) ENGINE InnoDB
            """
            cursor.execute(stmt)

            stmt = """
            CREATE TEMPORARY TABLE temp_blob_chunk (
                zoid        BIGINT UNSIGNED NOT NULL,
                chunk_num   BIGINT UNSIGNED NOT NULL,
                            PRIMARY KEY (zoid, chunk_num),
                chunk       LONGBLOB
            ) ENGINE InnoDB
            """
            cursor.execute(stmt)

        AbstractObjectMover.on_store_opened(self, cursor, restart=restart)

    @metricmethod_sampled
    def store_temp(self, cursor, batcher, oid, prev_tid, data):
        self._generic_store_temp(batcher, oid, prev_tid, data, 'REPLACE')

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        self._generic_restore(batcher, oid, tid, data, 'REPLACE')

    # Override this query from the superclass. The MySQL optimizer, up
    # through at least 5.7.17 doesn't like actual subqueries in a DELETE
    # statement. See https://github.com/zodb/relstorage/issues/175
    _move_from_temp_hf_delete_blob_chunk_query = """
    DELETE bc
    FROM blob_chunk bc
    INNER JOIN (SELECT zoid FROM temp_store) sq
    ON bc.zoid = sq.zoid
    """

    def _move_from_temp_object_state(self, cursor, tid):
        stmt = """
        REPLACE INTO object_state (zoid, tid, state_size, state)
        SELECT zoid, %s, COALESCE(LENGTH(state), 0), state
        FROM temp_store
        """
        cursor.execute(stmt, (tid,))

    @noop_when_history_free
    @metricmethod_sampled
    def update_current(self, cursor, tid):  # pylint:disable=method-hidden
        """
        Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        cursor.execute("""
        REPLACE INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %s
        """, (tid,))

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        stmt = """
        SELECT chunk
        FROM blob_chunk
        WHERE zoid = %s
            AND tid = %s
            AND chunk_num = %s
        """

        f = None
        bytecount = 0
        try:
            chunk_num = 0
            while True:
                cursor.execute(stmt, (oid, tid, chunk_num))
                rows = list(cursor)
                if rows:
                    assert len(rows) == 1
                    chunk = rows[0][0]
                else:
                    # No more chunks.  Note: if there are no chunks at
                    # all, then this method should not write a file.
                    break

                if f is None:
                    f = open(filename, 'wb')
                f.write(chunk)
                bytecount += len(chunk)
                chunk_num += 1
        except:
            if f is not None:
                f.close()
                os.remove(filename)
            raise

        if f is not None:
            f.close()
        return bytecount

    @metricmethod_sampled
    def upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        if tid is not None:
            if self.keep_history:
                delete_stmt = """
                DELETE FROM blob_chunk
                WHERE zoid = %s AND tid = %s
                """
                cursor.execute(delete_stmt, (oid, tid))
            else:
                delete_stmt = "DELETE FROM blob_chunk WHERE zoid = %s"
                cursor.execute(delete_stmt, (oid,))

            use_tid = True
            insert_stmt = """
            INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
            VALUES (%s, %s, %s, %s)
            """
        else:
            use_tid = False
            delete_stmt = "DELETE FROM temp_blob_chunk WHERE zoid = %s"
            cursor.execute(delete_stmt, (oid,))

            insert_stmt = """
            INSERT INTO temp_blob_chunk (zoid, chunk_num, chunk)
            VALUES (%s, %s, %s)
            """

        with open(filename, 'rb') as f:
            chunk_num = 0
            while True:
                chunk = f.read(self.blob_chunk_size)
                if not chunk and chunk_num > 0:
                    # EOF.  Note that we always write at least one
                    # chunk, even if the blob file is empty.
                    break
                if use_tid:
                    params = (oid, tid, chunk_num, chunk)
                else:
                    params = (oid, chunk_num, chunk)
                cursor.execute(insert_stmt, params)
                chunk_num += 1
