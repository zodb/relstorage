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

from relstorage.tests import TestCase

from ..query import Query as _BaseQuery
from ..query import CompiledQuery

class MockDialect(object):

    def bind(self, context): # pylint:disable=unused-argument
        return self

class Query(_BaseQuery):

    def compiled(self):
        return self

class TestQuery(TestCase):

    def test_name_discovery(self):
        # If a __name__ isn't assigned when a query is a
        # class property and used as a non-data-descriptor,
        # it finds it.

        class C(object):
            dialect = MockDialect()

            q1 = Query()
            q2 = Query()
            q_over = Query()

        class D(C):

            q3 = Query()
            q_over = Query()

        inst = D()

        # Undo the effects of Python 3.6's __set_name__.
        # pylint:disable=non-str-assignment-to-dunder-name,attribute-defined-outside-init
        D.q1.__name__ = None
        D.q2.__name__ = None
        D.q3.__name__ = None
        C.q_over.__name__ = None
        D.q_over.__name__ = None

        # get them to trigger them to search their name
        getattr(inst, 'q1')
        getattr(inst, 'q2')
        getattr(inst, 'q3')
        getattr(inst, 'q_over')


        self.assertEqual(C.q1.__name__, 'q1')
        self.assertEqual(C.q2.__name__, 'q2')
        self.assertEqual(D.q3.__name__, 'q3')
        self.assertIsNone(C.q_over.__name__)
        self.assertEqual(D.q_over.__name__, 'q_over')


class TestCompiledQuery(TestCase):

    def test_stmt_cache_on_bad_cursor(self):

        unique_execute_stmt = []

        class MockStatement(object):
            class dialect(object):
                class compiler(object):
                    def __init__(self, _):
                        "Does nothing"
                    def compile(self):
                        return 'stmt', ()
                    def can_prepare(self):
                        # We have to prepare if we want to try the cache
                        return True
                    def prepare(self):
                        o = object()
                        unique_execute_stmt.append(o)
                        return "prepare", o, lambda params: params

        executed = []

        class Cursor(object):
            __slots__ = ('__weakref__',)

            @property
            def connection(self):
                return self

            def execute(self, stmt):
                executed.append(stmt)

        cursor = Cursor()

        query = CompiledQuery(MockStatement())
        query.execute(cursor)

        self.assertLength(unique_execute_stmt, 1)
        self.assertLength(executed, 2)
        self.assertEqual(executed, [
            "prepare",
            unique_execute_stmt[0],
        ])

        query.execute(cursor)
        self.assertLength(unique_execute_stmt, 1)
        self.assertLength(executed, 3)
        self.assertEqual(executed, [
            "prepare",
            unique_execute_stmt[0],
            unique_execute_stmt[0],
        ])
