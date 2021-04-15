# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import implementer

from relstorage._util import consume

from ..interfaces import IObjectMover
from ..schema import Schema
from .scriptrunner import Sqlite3ScriptRunner
from ..mover import AbstractObjectMover
from ..mover import metricmethod_sampled

from .batch import Sqlite3RowBatcher


@implementer(IObjectMover)
class Sqlite3ObjectMover(AbstractObjectMover):

    _create_temp_store = Schema.temp_store.create()
    # SQLite doesn't do well at joining temporary tables to normal tables.
    # Even after running ANALYZE. (ANALYZE doesn't get temp tables). It assumes
    # that a table with no stats has a million rows (1,048,576), so until
    # the normal table is actually recorded as having a million rows, the join
    # order of these two tables is always going to put OBJECT_STATE as the outer loop
    # and scan that table. In practice, that's a poor choice: object_state will have
    # many more rows than the temp table. We can workaround that by explicitly
    # adding a stat for object_state to sqlite_stat1, but that will vanish
    # the next time an ANALYZE is run. Instead, if we use a CROSS JOIN,
    # we force the optimizer to follow our query ordering and make the smaller
    # number of loops.
    #
    # References:
    # - https://www.sqlite.org/lang_select.html#fromclause
    # - https://www.sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join
    _detect_conflict_query = AbstractObjectMover._detect_conflict_query.join_kind(
        'CROSS JOIN'
    )

    def __init__(self, database_driver, options, runner=None,
                 version_detector=None,
                 batcher_factory=Sqlite3RowBatcher):
        super(Sqlite3ObjectMover, self).__init__(
            database_driver,
            options, runner=runner, version_detector=version_detector,
            batcher_factory=batcher_factory)

        # We only ever have one blob chunk so we can simplify that
        # table.
        self.__on_store_opened_script = """
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_blob_chunk (
                zoid        INTEGER PRIMARY KEY,
                chunk_num   INTEGER NOT NULL,
                chunk       BLOB,
                CHECK(chunk_num = 0)
        );
        """ + str(self._create_temp_store) + """;
        """

    @metricmethod_sampled
    def on_store_opened(self, cursor, restart=False):
        """Create the temporary table for storing objects"""
        # Don't use cursor.executescript, it commits. Remember our store
        # connection is usually meant to be in auto-commit mode and shouldn't take
        # exclusive locks until tpc_vote time. Prior to Python 3.6, if we were
        # in a transaction this would commit it. but we shouldn't be in a transaction.
        if not restart:
            assert not cursor.connection.in_transaction, cursor
            Sqlite3ScriptRunner().run_script(cursor, self.__on_store_opened_script)
            assert not cursor.connection.in_transaction
            cursor.connection.register_before_commit_cleanup(self._before_commit)

        super(Sqlite3ObjectMover, self).on_store_opened(cursor, restart)

    def _before_commit(self, connection, _rolling_back=None):
        # Regardless of whether we're rolling back or not we need to delete
        # everything to freshen the connection. DELETE with no WHERE clause
        # in SQLite is optimized like a TRUNCATE.
        consume(connection.execute('DELETE FROM temp_store'))
        consume(connection.execute('DELETE FROM temp_blob_chunk'))

    _upload_blob_uses_chunks = False

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        self._generic_restore(batcher, oid, tid, data,
                              command='INSERT OR REPLACE', suffix='')

    def store_temps(self, cursor, state_oid_tid_iter):
        AbstractObjectMover.store_temps(self, cursor, state_oid_tid_iter)
        # That should have started a transaction, effectively moving us
        # from READ COMMITTED mode into REPEATABLE READ. We don't want that because we haven't
        # taken out an exclusive lock on the main database yet (but we do
        # want the write to be transactional and fast) so we now COMMIT and go back to
        # floating.
        cursor.connection.return_to_repeatable_read()

    def replace_temps(self, cursor, state_oid_tid_iter):
        # This is called when we're already holding exclusive locks
        # on the DB and must not release them.
        AbstractObjectMover.store_temps(self, cursor, state_oid_tid_iter)
