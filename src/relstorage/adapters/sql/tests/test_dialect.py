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
Tests for dialects.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.tests import TestCase

from .. import dialect

class TestMissingDialect(TestCase):

    def test_boolean(self):
        d = dialect._MissingDialect()
        self.assertFalse(d)


class TestCompiler(TestCase):

    def test_prepare_no_datatypes(self):

        class C(dialect.Compiler):
            def _next_prepared_stmt_name(self, query):
                return 'my_stmt'

            def _find_datatypes_for_prepared_query(self):
                return ()

        compiler = C(None, dialect.DefaultDialect())

        stmt, execute, convert = compiler.prepare()

        self.assertEqual(
            stmt,
            'PREPARE my_stmt AS '
        )

        self.assertEqual(
            execute,
            'EXECUTE my_stmt'
        )

        # We get the default dictionary converter even if we
        # don't need it.

        self.assertEqual(
            [],
            convert({'a': 42})
        )

    def test_prepare_named_datatypes(self):

        compiler = dialect.Compiler(None, dialect.DefaultDialect())
        compiler.placeholders[object()] = 'name'

        _s, _x, convert = compiler.prepare()

        self.assertEqual(
            [64],
            convert({'name': 64})
        )


class TestDialectAware(TestCase):

    def test_bind_none(self):

        aware = dialect.DialectAware()

        with self.assertRaisesRegex(TypeError, 'no dialect found'):
            aware.bind(None)

    def test_bind_dialect(self):
        class Dialect(dialect.DefaultDialect):
            def bind(self, context):
                raise AssertionError("Not supposed to re-bind")

        d = Dialect()

        aware = dialect.DialectAware()
        new_aware = aware.bind(d)
        self.assertIsNot(aware, new_aware)
        self.assertIs(new_aware.dialect, d)
