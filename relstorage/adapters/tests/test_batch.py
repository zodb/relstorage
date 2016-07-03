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

import unittest

class RowBatcherTests(unittest.TestCase):

    def getClass(self):
        from relstorage.adapters.batch import RowBatcher
        return RowBatcher

    def test_delete_defer(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.delete_from("mytable", id=2)
        self.assertEqual(cursor.executed, [])
        self.assertEqual(batcher.rows_added, 1)
        self.assertEqual(batcher.size_added, 0)
        self.assertEqual(batcher.deletes,
            {('mytable', ('id',)): set([("2",)])})

    def test_delete_multiple_column(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.delete_from("mytable", id=2, tid=10)
        self.assertEqual(cursor.executed, [])
        self.assertEqual(batcher.rows_added, 1)
        self.assertEqual(batcher.size_added, 0)
        self.assertEqual(batcher.deletes,
            {('mytable', ('id', 'tid')): set([("2", "10")])})

    def test_delete_auto_flush(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.row_limit = 2
        batcher.delete_from("mytable", id=2)
        batcher.delete_from("mytable", id=1)
        self.assertEqual(cursor.executed,
            [('DELETE FROM mytable WHERE id IN (1,2)', None)])
        self.assertEqual(batcher.rows_added, 0)
        self.assertEqual(batcher.size_added, 0)
        self.assertEqual(batcher.deletes, {})

    def test_insert_defer(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'a'),
            rowkey=1,
            size=3,
            )
        self.assertEqual(cursor.executed, [])
        self.assertEqual(batcher.rows_added, 1)
        self.assertEqual(batcher.size_added, 3)
        self.assertEqual(batcher.inserts, {
            ('INSERT', 'mytable (id, name)', '%s, id || %s'): {1: (1, 'a')}
            })

    def test_insert_replace(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'a'),
            rowkey=1,
            size=3,
            command='REPLACE',
            )
        self.assertEqual(cursor.executed, [])
        self.assertEqual(batcher.rows_added, 1)
        self.assertEqual(batcher.size_added, 3)
        self.assertEqual(batcher.inserts, {
            ('REPLACE', 'mytable (id, name)', '%s, id || %s'): {1: (1, 'a')}
            })

    def test_insert_duplicate(self):
        # A second insert on the same rowkey replaces the first insert.
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'a'),
            rowkey=1,
            size=3,
            )
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'b'),
            rowkey=1,
            size=3,
            )
        self.assertEqual(cursor.executed, [])
        self.assertEqual(batcher.rows_added, 2)
        self.assertEqual(batcher.size_added, 6)
        self.assertEqual(batcher.inserts, {
            ('INSERT', 'mytable (id, name)', '%s, id || %s'): {1: (1, 'b')}
            })

    def test_insert_auto_flush(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.size_limit = 10
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'a'),
            rowkey=1,
            size=5,
            )
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (2, 'B'),
            rowkey=2,
            size=5,
            )
        self.assertEqual(cursor.executed, [(
            'INSERT INTO mytable (id, name) VALUES\n'
            '(%s, id || %s),\n'
            '(%s, id || %s)',
            (1, 'a', 2, 'B'))
        ])
        self.assertEqual(batcher.rows_added, 0)
        self.assertEqual(batcher.size_added, 0)
        self.assertEqual(batcher.inserts, {})

    def test_flush(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor)
        batcher.delete_from("mytable", id=1)
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'a'),
            rowkey=1,
            size=5,
            )
        batcher.flush()
        self.assertEqual(cursor.executed, [
            ('DELETE FROM mytable WHERE id IN (1)', None),
            ('INSERT INTO mytable (id, name) VALUES\n(%s, id || %s)',
             (1, 'a')),
            ])


class OracleRowBatcherTests(unittest.TestCase):

    def getClass(self):
        from relstorage.adapters.batch import OracleRowBatcher
        return OracleRowBatcher

    def test_insert_one_row(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor, {})
        batcher.insert_into(
            "mytable (id, name)",
            "%s, id || %s",
            (1, 'a'),
            rowkey=1,
            size=3,
            )
        self.assertEqual(cursor.executed, [])
        batcher.flush()
        self.assertEqual(cursor.executed, [
            ('INSERT INTO mytable (id, name) VALUES (%s, id || %s)', (1, 'a')),
            ])

    def test_insert_two_rows(self):
        cursor = MockCursor()
        batcher = self.getClass()(cursor, {})
        batcher.insert_into(
            "mytable (id, name)",
            ":id, :id || :name",
            {'id': 1, 'name': 'a'},
            rowkey=1,
            size=3,
            )
        batcher.insert_into(
            "mytable (id, name)",
            ":id, :id || :name",
            {'id': 2, 'name': 'b'},
            rowkey=2,
            size=3,
            )
        self.assertEqual(cursor.executed, [])
        batcher.flush()
        self.assertEqual(cursor.executed, [(
            'INSERT ALL\n'
            'INTO mytable (id, name) VALUES (:id_0, :id_0 || :name_0)\n'
            'INTO mytable (id, name) VALUES (:id_1, :id_1 || :name_1)\n'
            'SELECT * FROM DUAL',
            {'id_0': 1, 'id_1': 2, 'name_1': 'b', 'name_0': 'a'})
        ])

    def test_insert_one_raw_row(self):
        class MockRawType(object):
            pass
        cursor = MockCursor()
        batcher = self.getClass()(cursor, {'rawdata': MockRawType})
        batcher.insert_into(
            "mytable (id, data)",
            ":id, :rawdata",
            {'id': 1, 'rawdata': 'xyz'},
            rowkey=1,
            size=3,
            )
        batcher.flush()
        self.assertEqual(cursor.executed, [
            ('INSERT INTO mytable (id, data) VALUES (:id, :rawdata)',
                {'id': 1, 'rawdata': 'xyz'})
        ])
        self.assertEqual(cursor.inputsizes, {'rawdata': MockRawType})

    def test_insert_two_raw_rows(self):
        class MockRawType(object):
            pass
        cursor = MockCursor()
        batcher = self.getClass()(cursor, {'rawdata': MockRawType})
        batcher.insert_into(
            "mytable (id, data)",
            ":id, :rawdata",
            {'id': 1, 'rawdata': 'xyz'},
            rowkey=1,
            size=3,
            )
        batcher.insert_into(
            "mytable (id, data)",
            ":id, :rawdata",
            {'id': 2, 'rawdata': 'abc'},
            rowkey=2,
            size=3,
            )
        batcher.flush()
        self.assertEqual(cursor.executed, [(
            'INSERT ALL\n'
            'INTO mytable (id, data) VALUES (:id_0, :rawdata_0)\n'
            'INTO mytable (id, data) VALUES (:id_1, :rawdata_1)\n'
            'SELECT * FROM DUAL',
            {'id_0': 1, 'id_1': 2, 'rawdata_0': 'xyz', 'rawdata_1': 'abc'})
        ])
        self.assertEqual(cursor.inputsizes, {
            'rawdata_0': MockRawType,
            'rawdata_1': MockRawType,
        })


class MockCursor(object):
    def __init__(self):
        self.executed = []
        self.inputsizes = {}
    def setinputsizes(self, **kw):
        self.inputsizes.update(kw)
    def execute(self, stmt, params=None):
        self.executed.append((stmt, params))

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(RowBatcherTests))
    suite.addTest(unittest.makeSuite(OracleRowBatcherTests))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
