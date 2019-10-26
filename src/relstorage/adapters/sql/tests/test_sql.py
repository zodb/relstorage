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

"""
Tests for the SQL abstraction layer.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.tests import TestCase

from .. import Table
from .. import TemporaryTable
from .. import Char
from .. import HistoryVariantTable
from .. import Column
from .. import it
from .. import DefaultDialect
from .. import OID
from .. import TID
from .. import State
from .. import Boolean
from .. import BinaryString
from .. import func

from ..expressions import bindparam

current_object = Table(
    'current_object',
    Column('zoid', OID),
    Column('tid', TID)
)

object_state = Table(
    'object_state',
    Column('zoid', OID),
    Column('tid', TID),
    Column('state', State),
    Column('state_size'),
)

hp_object_and_state = current_object.natural_join(object_state)

objects = HistoryVariantTable(
    current_object,
    object_state,
)

object_and_state = HistoryVariantTable(
    hp_object_and_state,
    object_state
)

transaction = Table(
    'transaction',
    Column('tid', TID),
    Column('packed', Boolean),
    Column('username', BinaryString),
    Column('description', BinaryString),
    Column('extension', BinaryString),
)

temp_store = TemporaryTable(
    'temp_store',
    Column('zoid', OID, primary_key=True),
    Column('prev_tid', TID, nullable=False),
    Column('md5', Char(32)), # Only used when keep_history=True
    Column('state', State)
)


class TestTableSelect(TestCase):

    def test_simple_eq_select(self):
        table = object_state

        stmt = table.select().where(table.c.zoid == table.c.tid)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size FROM object_state WHERE (zoid = tid)'
        )

    def test_simple_eq_limit(self):
        table = object_state

        stmt = table.select().where(table.c.zoid == table.c.tid).limit(1)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size FROM object_state WHERE (zoid = tid) LIMIT 1'
        )

    def test_simple_eq_for_update(self):
        table = object_state

        stmt = table.select().where(table.c.zoid == table.c.tid).for_update()

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size FROM object_state WHERE (zoid = tid) FOR UPDATE'
        )

        stmt = stmt.nowait()
        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size FROM object_state WHERE (zoid = tid) FOR UPDATE '
            'NOWAIT'
        )

    def test_max_select(self):
        table = object_state

        stmt = table.select(func.max(it.c.tid))

        self.assertEqual(
            str(stmt),
            'SELECT max(tid) FROM object_state'
        )

    def test_distinct(self):
        table = object_state
        stmt = table.select(table.c.zoid).where(table.c.tid == table.bindparam('tid')).distinct()
        self.assertEqual(
            str(stmt),
            'SELECT DISTINCT zoid FROM object_state WHERE (tid = %(tid)s)'
        )

    def test_simple_eq_select_and(self):

        table = object_state

        stmt = table.select().where(table.c.zoid == table.c.tid)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size FROM object_state WHERE (zoid = tid)'
        )

        stmt = stmt.and_(table.c.zoid > 5)
        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size '
            'FROM object_state WHERE ((zoid = tid AND zoid > 5))'
        )

    def test_simple_eq_select_literal(self):
        table = object_state

        # This is a useless query
        stmt = table.select().where(table.c.zoid == 7)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state, state_size FROM object_state WHERE (zoid = 7)'
        )

        self.assertEqual(
            stmt.compiled().params,
            {})

    def test_column_query_variant_table(self):
        stmt = objects.select(objects.c.tid, objects.c.zoid).where(
            objects.c.tid > bindparam('tid')
        )

        self.assertEqual(
            str(stmt),
            'SELECT tid, zoid FROM current_object WHERE (tid > %(tid)s)'
        )

    def test_natural_join(self):
        stmt = object_and_state.select(
            object_and_state.c.zoid, object_and_state.c.state
        ).where(
            object_and_state.c.zoid == object_and_state.bindparam('oid')
        )

        self.assertEqual(
            str(stmt),
            'SELECT zoid, state '
            'FROM current_object '
            'JOIN object_state '
            'USING (tid, zoid) WHERE (zoid = %(oid)s)'
        )

        class H(object):
            keep_history = False
            dialect = DefaultDialect()

        stmt = stmt.bind(H())

        self.assertEqual(
            str(stmt),
            'SELECT zoid, state '
            'FROM object_state '
            'WHERE (zoid = %(oid)s)'
        )

    def test_bind(self):
        from operator import attrgetter
        # pylint:disable=no-member
        select = objects.select(objects.c.tid, objects.c.zoid).where(
            objects.c.tid > bindparam('tid')
        )
        # Unbound we assume history
        self.assertEqual(
            str(select),
            'SELECT tid, zoid FROM current_object WHERE (tid > %(tid)s)'
        )

        class Context(object):
            dialect = DefaultDialect()
            keep_history = True

        context = Context()
        dialect = context.dialect
        query = select.bind(context)

        class Root(object):
            select = query

        # The table replaced itself with the correct variant.
        self.assertIsNot(query.table, objects)
        self.assertIs(query.table, objects.history_preserving)

        for item_name in (
                'select',
                'select._where',
                'select._where.expression',
        ):
            __traceback_info__ = item_name
            item = attrgetter(item_name)(Root)
            # The exact context is passed down the tree.
            self.assertIs(item.context, context)
            # The dialect is first bound, so it's *not* the same
            # as the one we can reference (though it is equal)...
            self.assertEqual(item.dialect, dialect)
            # ...but it *is* the same throughout the tree
            self.assertIs(query.dialect, item.dialect)

        # We take up its history setting
        self.assertEqual(
            str(query),
            'SELECT tid, zoid FROM current_object WHERE (tid > %(tid)s)'
        )

        # Bound to history-free we use history free
        context.keep_history = False
        query = select.bind(context)

        self.assertEqual(
            str(query),
            'SELECT tid, zoid FROM object_state WHERE (tid > %(tid)s)'
        )

    def test_bind_descriptor(self):
        class Context(object):
            keep_history = True
            dialect = DefaultDialect()
            # pylint:disable=no-member
            select = objects.select(objects.c.tid, objects.c.zoid).where(
                objects.c.tid > bindparam('tid')
            )

        # Unbound we assume history
        self.assertEqual(
            str(Context.select),
            'SELECT tid, zoid FROM current_object WHERE (tid > %(tid)s)'
        )

        context = Context()
        context.keep_history = False
        self.assertEqual(
            str(context.select),
            'SELECT tid, zoid FROM object_state WHERE (tid > %(tid)s)'
        )

    def test_prepared_insert_values(self):
        stmt = current_object.insert(
            current_object.c.zoid
        )
        stmt.__name__ = 'ins'

        self.assertEqual(
            str(stmt),
            'INSERT INTO current_object(zoid) VALUES (%s)'
        )

        stmt = stmt.prepared()
        self.assertTrue(
            str(stmt).startswith('EXECUTE rs_prep_stmt')
        )

        stmt = stmt.compiled()
        self.assertRegex(
            stmt._prepare_stmt,
            r"PREPARE rs_prep_stmt_ins_[0-9]*_[0-9]* \(BIGINT\) AS.*"
        )

    def test_prepared_insert_select_with_param(self):
        stmt = current_object.insert().from_select(
            (current_object.c.zoid,
             current_object.c.tid),
            object_state.select(
                object_state.c.zoid,
                object_state.orderedbindparam()
            )
        )
        stmt.__name__ = 'ins'
        self.assertEqual(
            str(stmt),
            'INSERT INTO current_object(zoid, tid) SELECT zoid, %s FROM object_state'
        )

        stmt = stmt.prepared()
        self.assertTrue(
            str(stmt).startswith('EXECUTE rs_prep_stmt')
        )

        stmt = stmt.compiled()
        self.assertRegex(
            stmt._prepare_stmt,
            r"PREPARE rs_prep_stmt_ins_[0-9]*_[0-9]* \(BIGINT\) AS.*"
        )

    def test_it(self):
        stmt = object_state.select(
            it.c.zoid,
            it.c.state
        ).where(
            it.c.tid == it.bindparam('tid')
        ).order_by(
            it.c.zoid
        )

        self.assertEqual(
            str(stmt),
            'SELECT zoid, state FROM object_state WHERE (tid = %(tid)s) ORDER BY zoid'
        )

        # Now something that won't resolve.
        col_ref = it.c.dne

        # In the column list
        with self.assertRaisesRegex(AttributeError, 'does not include dne'):
            object_state.select(col_ref)

        stmt = object_state.select(it.c.zoid)

        # In the where clause
        with self.assertRaisesRegex(AttributeError, 'does not include dne'):
            stmt.where(col_ref == object_state.c.state)

        # In order by
        with self.assertRaisesRegex(AttributeError, 'does not include dne'):
            stmt.order_by(col_ref == object_state.c.state)

    def test_boolean_literal(self):
        stmt = transaction.select(
            transaction.c.tid
        ).where(
            it.c.packed == False # pylint:disable=singleton-comparison
        ).order_by(
            transaction.c.tid, 'DESC'
        )

        self.assertEqual(
            str(stmt),
            'SELECT tid FROM transaction WHERE (packed = FALSE) ORDER BY tid DESC'
        )

    def test_literal_in_select(self):
        stmt = current_object.select(
            1
        ).where(
            current_object.c.zoid == current_object.bindparam('oid')
        )

        self.assertEqual(
            str(stmt),
            'SELECT 1 FROM current_object WHERE (zoid = %(oid)s)'
        )

    def test_boolean_literal_it_joined_table(self):
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

        self.assertEqual(
            str(stmt),
            'SELECT tid, username, description, extension, state_size '
            'FROM transaction '
            'JOIN object_state '
            'USING (tid) '
            'WHERE ((zoid = %(oid)s AND packed = FALSE)) '
            'ORDER BY tid DESC'
        )

class TestUpsert(TestCase):
    # This is here rather than test_dialect.py because it uses the higher level
    # schema objects.
    keep_history = False
    dialect = DefaultDialect()
    maxDiff = None

    insert_or_replace = (
        'INSERT INTO object_state(zoid, state, tid, state_size) '
        'VALUES (%s, %s, %s, %s) '
        'ON CONFLICT (zoid) '
        'DO UPDATE SET state = excluded.state, tid = excluded.tid, '
        'state_size = excluded.state_size'
    )

    def test_insert_or_replace(self):
        stmt = object_state.upsert(
            it.c.zoid,
            it.c.state,
            it.c.tid,
            it.c.state_size
        ).on_conflict(
            it.c.zoid
        ).do_update(
            it.c.state,
            it.c.tid,
            it.c.state_size
        ).bind(self)

        self.assertEqual(
            str(stmt),
            self.insert_or_replace
        )

    insert_or_replace_subquery = (
        'INSERT INTO object_state(zoid, tid, state, state_size) '
        'SELECT zoid, %s, state, COALESCE(LENGTH(state), 0) FROM temp_store '
        'ORDER BY zoid '
        'ON CONFLICT (zoid) '
        'DO UPDATE SET state = excluded.state, tid = excluded.tid, '
        'state_size = excluded.state_size'
    )

    def test_insert_or_replace_subquery(self):
        stmt = object_state.upsert(
            it.c.zoid,
            it.c.tid,
            it.c.state,
            it.c.state_size
        ).from_select(
            (object_state.c.zoid,
             object_state.c.tid,
             object_state.c.state,
             object_state.c.state_size,
            ),
            temp_store.select(
                temp_store.c.zoid,
                temp_store.orderedbindparam(),
                temp_store.c.state,
                'COALESCE(LENGTH(state), 0)',
            ).order_by(
                temp_store.c.zoid
            )
        ).on_conflict(
            it.c.zoid
        ).do_update(
            it.c.state,
            it.c.tid,
            it.c.state_size
        ).bind(self)

        self.maxDiff = None
        self.assertEqual(
            str(stmt),
            self.insert_or_replace_subquery
        )

    upsert_unconstrained_subquery = (
        'INSERT INTO object_state(zoid, tid, state, state_size) '
        'SELECT zoid, %s, state, COALESCE(LENGTH(state), 0) FROM temp_store '
        'ON CONFLICT (zoid) '
        'DO UPDATE SET state = excluded.state, tid = excluded.tid, '
        'state_size = excluded.state_size'
    )

    def test_upsert_unconstrained_subquery(self):
        stmt = object_state.upsert(
            it.c.zoid,
            it.c.tid,
            it.c.state,
            it.c.state_size
        ).from_select(
            (object_state.c.zoid,
             object_state.c.tid,
             object_state.c.state,
             object_state.c.state_size,
            ),
            temp_store.select(
                temp_store.c.zoid,
                temp_store.orderedbindparam(),
                temp_store.c.state,
                'COALESCE(LENGTH(state), 0)',
            )
        ).on_conflict(
            it.c.zoid
        ).do_update(
            it.c.state,
            it.c.tid,
            it.c.state_size
        ).bind(self)

        self.maxDiff = None
        self.assertEqual(
            str(stmt),
            self.upsert_unconstrained_subquery
        )
