##############################################################################
#
# Copyright (c) 2026 Zope Foundation and Contributors.
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

from relstorage.tests import TestCase
from relstorage.tests import MockCursor


class _Driver(object):

    @staticmethod
    def binary_column_as_state_type(state):
        return state


class _LoadBatcher(object):

    def __init__(self, rows):
        self.rows = rows

    def select_from(self, columns, table, suffix='', **kw):
        return iter(self.rows)


class HistoryFreeAddRefsForOidsTests(TestCase):

    def _makeOne(self):
        from relstorage.adapters.packundo import HistoryFreePackUndo
        packundo = HistoryFreePackUndo.__new__(HistoryFreePackUndo)
        packundo.driver = _Driver()
        return packundo

    def _executed_tables(self, cursor):
        return [
            ' '.join(stmt.split()[:3])
            for stmt, _ in cursor.executed
        ]

    def test_old_refs_added_row_is_deleted_before_the_new_one_is_inserted(self):
        # An object whose references were read at an older tid and that has
        # changed since: ``object_refs_added`` still holds its old row, and
        # the new one has the same primary key (zoid). The batcher executes
        # deletes before inserts *within* a flush, but a flush can happen as
        # soon as a row is queued; if the insert is queued first and fills
        # the batch, it reaches the database while the old row is still
        # there: "duplicate key value violates unique constraint
        # object_refs_added_pkey" (#482). row_limit=1 flushes on every row.
        from relstorage.adapters.batch import RowBatcher
        cursor = MockCursor()
        store_batcher = RowBatcher(cursor, row_limit=1)
        packundo = self._makeOne()

        packundo._add_refs_for_oids(
            _LoadBatcher([(1, 5, b'state')]),
            store_batcher,
            [1],
            lambda state: [2],
        )
        store_batcher.flush()

        executed = self._executed_tables(cursor)
        delete = executed.index('DELETE FROM object_refs_added')
        insert = executed.index('INSERT INTO object_refs_added')
        self.assertLess(delete, insert, executed)

    def test_old_object_ref_rows_are_deleted_before_the_new_ones_are_inserted(self):
        from relstorage.adapters.batch import RowBatcher
        cursor = MockCursor()
        store_batcher = RowBatcher(cursor, row_limit=1)
        packundo = self._makeOne()

        packundo._add_refs_for_oids(
            _LoadBatcher([(1, 5, b'state')]),
            store_batcher,
            [1],
            lambda state: [2],
        )
        store_batcher.flush()

        executed = self._executed_tables(cursor)
        delete = executed.index('DELETE FROM object_ref')
        insert = executed.index('INSERT INTO object_ref')
        self.assertLess(delete, insert, executed)
