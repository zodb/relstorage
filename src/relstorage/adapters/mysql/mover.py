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
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage.adapters.interfaces import IObjectMover

from ..schema import Schema
from ..mover import AbstractObjectMover
from ..mover import metricmethod_sampled


@implementer(IObjectMover)
class MySQLObjectMover(AbstractObjectMover):

    _create_temp_store = Schema.temp_store.create()

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
            # We are up to 4 temp tables, doing this with a call to a stored
            # proc saves round trips.
            #
            # It's possible that the DDL lock that TRUNCATE takes can be a bottleneck
            # in some places, though?
            self.driver.callproc_no_result(
                cursor,
                "clean_temp_state(false)"
            )
        else:
            # InnoDB tables benchmark much faster for concurrency=2
            # and 6 than MyISAM tables under both MySQL 5.5 and 5.7,
            # at least on OS X 10.12. The OS X filesystem is single
            # threaded, though, so the effects of flushing MyISAM tables
            # to disk on every operation are probably magnified.

            # note that the md5 column is not used if self.keep_history == False.
            self._create_temp_store.execute(cursor)

            stmt = """
            CREATE TEMPORARY TABLE temp_read_current (
                zoid        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                tid         BIGINT UNSIGNED NOT NULL
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

            stmt = """
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_locked_zoid (
                zoid BIGINT UNSIGNED NOT NULL PRIMARY KEY
            ) ENGINE InnoDB
            """
            cursor.execute(stmt)

        AbstractObjectMover.on_store_opened(self, cursor, restart=restart)

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        if self.keep_history:
            suffix = """
            ON DUPLICATE KEY UPDATE
                tid = VALUES(tid),
                prev_tid = VALUES(prev_tid),
                md5 = VALUES(md5),
                state_size = VALUES(state_size),
                state = VALUES(state)
            """
        else:
            suffix = """
            ON DUPLICATE KEY UPDATE
                tid = VALUES(tid),
                state_size = VALUES(state_size),
                state = VALUES(state)
            """
        self._generic_restore(batcher, oid, tid, data,
                              command='INSERT', suffix=suffix)

    # Override this query from the superclass. The MySQL optimizer, up
    # through at least 5.7.17 doesn't like actual subqueries in a DELETE
    # statement. See https://github.com/zodb/relstorage/issues/175
    _move_from_temp_hf_delete_blob_chunk_query = """
    DELETE bc
    FROM blob_chunk bc
    INNER JOIN (SELECT zoid FROM temp_store) sq
    ON bc.zoid = sq.zoid
    """

    # We UPSERT for hf movement; no need to do a delete.
    _move_from_temp_hf_delete_query = ''
