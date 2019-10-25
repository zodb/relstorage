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
        assert not cursor.connection.in_transaction
        if not restart:
            Sqlite3ScriptRunner().run_script(cursor, self.__on_store_opened_script)
            assert not cursor.connection.in_transaction
            cursor.connection.register_before_commit_cleanup(self._before_commit)

        super(Sqlite3ObjectMover, self).on_store_opened(cursor, restart)

    def _before_commit(self, connection, _rolling_back=None):
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
        # from READ COMMITTED mode into REPEATABLE READ. We don't want that (but we do
        # want the write to be transactional and fast) so we now COMMIT and go back to
        # floating.
        cursor.connection.commit(run_cleanups=False)

    def replace_temps(self, cursor, state_oid_tid_iter):
        # This is called when we're already holding exclusive locks
        # on the DB and must not release them.
        AbstractObjectMover.store_temps(self, cursor, state_oid_tid_iter)
