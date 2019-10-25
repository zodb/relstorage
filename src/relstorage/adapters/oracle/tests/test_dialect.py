# -*- coding: utf-8 -*-
"""
Tests for the Oracle dialect.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from relstorage.tests import TestCase
from relstorage.tests import MockCursor

from ...schema import Schema
from ...sql import it

from ..dialect import OracleDialect

class Context(object):
    keep_history = True
    dialect = OracleDialect()

class Driver(object):
    dialect = OracleDialect()
    binary_column_as_state_type = binary_column_as_bytes = lambda b: b
    Binary = bytes

class TestOracleDialect(TestCase):

    def test_boolean_parameter(self):
        transaction = Schema.transaction
        object_state = Schema.object_state

        stmt = transaction.natural_join(
            object_state
        ).select(
            it.c.tid, it.c.username, it.c.description, it.c.extension,
            object_state.c.state_size
        ).where(
            it.c.zoid == it.bindparam("oid")
        ).and_(
            it.c.packed == False # pylint:disable=singleton-comparison
        ).order_by(
            it.c.tid, "DESC"
        )

        # By default we get the generic syntax
        self.assertEqual(
            str(stmt),
            'SELECT tid, username, description, extension, state_size '
            'FROM transaction '
            'JOIN object_state '
            'USING (tid) '
            "WHERE ((zoid = %(oid)s AND packed = FALSE)) "
            'ORDER BY tid DESC'
        )

        # But once we bind to the dialect we get the expected value
        stmt = stmt.bind(Context)
        self.assertEqual(
            str(stmt),
            'SELECT tid, username, description, extension, state_size '
            'FROM transaction '
            'JOIN object_state '
            'USING (tid) '
            "WHERE ((zoid = :oid AND packed = 'N')) "
            'ORDER BY tid DESC'
        )

    def test_boolean_result(self):
        """
        SELECT tid, username, description, extension,

            FROM transaction
            WHERE tid >= 0
        """

        transaction = Schema.transaction
        stmt = transaction.select(
            transaction.c.tid,
            transaction.c.username,
            transaction.c.description,
            transaction.c.extension,
            transaction.c.packed
        ).where(
            transaction.c.tid >= 0
        )

        stmt = stmt.bind(Context)

        self.assertEqual(
            str(stmt),
            'SELECT tid, username, description, extension, '
            "CASE WHEN packed = 'Y' THEN 1 ELSE 0 END "
            'FROM transaction WHERE (tid >= 0)'
        )

class TestMoverQueries(TestCase):

    def _makeOne(self):
        from ..mover import OracleObjectMover
        from relstorage.tests import MockOptions

        return OracleObjectMover(Driver(), MockOptions())

    def test_move_named_query(self):

        inst = self._makeOne()

        unbound = type(inst)._move_from_temp_hf_upsert_query

        self.assertRegex(
            str(unbound),
            r'EXECUTE rs_prep_stmt_.*'
        )
        self.maxDiff = None
        # Bound, the py-format style escapes are replaced with
        # :name params; these are simple numbers.
        self.assertEqual(
            str(inst._move_from_temp_hf_upsert_query),
            'MERGE INTO object_state USING ( '
            'SELECT zoid, :1 tid, COALESCE(LENGTH(state), 0) state_size, state '
            'FROM temp_store ORDER BY zoid) D '
            'ON (object_state.zoid = D.zoid) '
            'WHEN MATCHED THEN UPDATE SET state = D.state, tid = D.tid, state_size = D.state_size '
            'WHEN NOT MATCHED THEN INSERT (zoid, tid, state_size, state) '
            'VALUES (D.zoid, D.tid, D.state_size, D.state)'
        )

class TestDatabaseIteratorQueries(TestCase):

    def _makeOne(self):
        from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
        return HistoryPreservingDatabaseIterator(Driver())

    def test_query(self):
        inst = self._makeOne()

        unbound = type(inst)._iter_objects_query

        self.assertEqual(
            str(unbound),
            'SELECT zoid, state FROM object_state WHERE (tid = %(tid)s) ORDER BY zoid'
        )

        # Bound we get pyformat %(name)s replaced with :name
        self.assertEqual(
            str(inst._iter_objects_query),
            'SELECT zoid, state FROM object_state WHERE (tid = :tid) ORDER BY zoid'
        )

        cursor = MockCursor()
        list(inst.iter_objects(cursor, 1)) # Iterator, must flatten

        self.assertEqual(1, len(cursor.executed))
        stmt, params = cursor.executed[0]

        self.assertEqual(
            stmt,
            'SELECT zoid, state FROM object_state WHERE (tid = :tid) ORDER BY zoid'
        )

        self.assertEqual(
            params,
            {'tid': 1}
        )

    def test_iter_transactions(self):
        inst = self._makeOne()
        cursor = MockCursor()

        inst.iter_transactions(cursor)

        self.assertEqual(1, len(cursor.executed))
        stmt, params = cursor.executed[0]

        self.assertEqual(
            stmt,
            'SELECT tid, username, description, extension, 0 '
            'FROM transaction '
            "WHERE ((packed = 'N' AND tid <> 0)) "
            'ORDER BY tid DESC'
        )

        self.assertEqual(
            params,
            None
        )
