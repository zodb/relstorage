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

from .._sql import Table
from .._sql import HistoryVariantTable
from .._sql import Column
from .._sql import bindparam

current_object = Table(
    'current_object',
    Column('zoid'),
    Column('tid')
)

object_state = Table(
    'object_state',
    Column('zoid'),
    Column('tid'),
    Column('state'),
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

class TestTableSelect(TestCase):

    def test_simple_eq_select(self):
        table = object_state

        stmt = table.select().where(table.c.zoid == table.c.tid)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state FROM object_state WHERE (zoid = tid)'
        )

    def test_simple_eq_select_and(self):

        table = object_state

        stmt = table.select().where(table.c.zoid == table.c.tid)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state FROM object_state WHERE (zoid = tid)'
        )

        stmt = stmt.and_(table.c.zoid > 5)
        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state FROM object_state WHERE ((zoid = tid AND zoid > %(param_0)s))'
        )

    def test_simple_eq_select_literal(self):
        table = object_state

        # This is a useless query
        stmt = table.select().where(table.c.zoid == 7)

        self.assertEqual(
            str(stmt),
            'SELECT zoid, tid, state FROM object_state WHERE (zoid = %(param_0)s)'
        )

        self.assertEqual(
            stmt.compiled().params,
            {'param_0': 7})

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
            'USING (zoid, tid) WHERE (zoid = %(oid)s)'
        )

        class H(object):
            keep_history = False

        stmt = stmt.bind(H())

        self.assertEqual(
            str(stmt),
            'SELECT zoid, state '
            'FROM object_state '
            'WHERE (zoid = %(oid)s)'
        )

    def test_bind(self):
        select = objects.select(objects.c.tid, objects.c.zoid).where(
            objects.c.tid > bindparam('tid')
        )
        # Unbound we assume history
        self.assertEqual(
            str(select),
            'SELECT tid, zoid FROM current_object WHERE (tid > %(tid)s)'
        )
        from .._sql import _DefaultDialect
        select = select.bind(42)
        context = _DefaultDialect(42)

        self.assertEqual(select.context, context)
        self.assertEqual(select.table.context, context)
        self.assertEqual(select._where.context, context)
        self.assertEqual(select._where.expression.context, context)
        # Bound to the wrong thing we assume history
        self.assertEqual(
            str(select),
            'SELECT tid, zoid FROM current_object WHERE (tid > %(tid)s)'
        )

        # Bound to history-free we use history free
        class H(object):
            keep_history = False
        select = select.bind(H())

        self.assertEqual(
            str(select),
            'SELECT tid, zoid FROM object_state WHERE (tid > %(tid)s)'
        )

    def test_bind_descriptor(self):
        class Context(object):
            keep_history = True
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
