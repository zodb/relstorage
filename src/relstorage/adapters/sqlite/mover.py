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


from ..interfaces import IObjectMover
from ..schema import Schema
from ..mover import AbstractObjectMover
from ..mover import metricmethod_sampled

from .batch import Sqlite3RowBatcher

@implementer(IObjectMover)
class Sqlite3ObjectMover(AbstractObjectMover):

    def __init__(self, database_driver, options, runner=None,
                 version_detector=None,
                 batcher_factory=Sqlite3RowBatcher):
        super(Sqlite3ObjectMover, self).__init__(
            database_driver,
            options, runner=runner, version_detector=version_detector,
            batcher_factory=batcher_factory)

    _create_temp_store = Schema.temp_store.create()
    _clear_temp_store = Schema.temp_store.truncate()

    @metricmethod_sampled
    def on_store_opened(self, cursor, restart=False):
        """Create the temporary table for storing objects"""
        if restart:
            self._clear_temp_store.execute(cursor)
            cursor.execute('DELETE FROM temp_blob_chunk')
        else:
            self._create_temp_store.execute(cursor)
            # TODO: We don't actually need more than one chunk.
            stmt = """
            CREATE TEMPORARY TABLE temp_blob_chunk (
                zoid        INTEGER NOT NULL,
                chunk_num   INTEGER NOT NULL,
                chunk       BLOB,
                PRIMARY KEY (zoid, chunk_num)
            )
            """
            cursor.execute(stmt)

        super(Sqlite3ObjectMover, self).on_store_opened(cursor, restart)

    @metricmethod_sampled
    def store_temp(self, _cursor, batcher, oid, prev_tid, data):
        # suffix = """
        # ON CONFLICT (zoid) DO UPDATE SET state = excluded.state,
        #                       prev_tid = excluded.prev_tid,
        #                       md5 = excluded.md5
        # """
        # TODO: Use the update syntax when available.
        self._generic_store_temp(batcher, oid, prev_tid, data)

    _upload_blob_uses_chunks = False

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        # TODO: Use the update syntax when available.
        # if self.keep_history:
        #     suffix = """
        #     ON DUPLICATE KEY UPDATE
        #         tid = VALUES(tid),
        #         prev_tid = VALUES(prev_tid),
        #         md5 = VALUES(md5),
        #         state_size = VALUES(state_size),
        #         state = VALUES(state)
        #     """
        # else:
        #     suffix = """
        #     ON DUPLICATE KEY UPDATE
        #         tid = VALUES(tid),
        #         state_size = VALUES(state_size),
        #         state = VALUES(state)
        #     """
        self._generic_restore(batcher, oid, tid, data, suffix='')
