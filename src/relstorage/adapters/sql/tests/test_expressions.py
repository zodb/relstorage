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
Tests for expressions.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.tests import TestCase

from .. import expressions

class TestBinaryExpression(TestCase):

    def test_str(self):

        exp = expressions.BinaryExpression('=', 'lhs', 'rhs')
        self.assertEqual(
            str(exp),
            'lhs = rhs'
        )


class TestEmptyExpression(TestCase):

    exp = expressions.EmptyExpression()

    def test_boolean(self):
        self.assertFalse(self.exp)

    def test_str(self):
        self.assertEqual(str(self.exp), '')

    def test_and(self):
        self.assertIs(self.exp.and_(self), self)


class TestAnd(TestCase):

    def test_resolve(self):
        # This will wrap them in literal nodes, which
        # do nothing when resolved.

        exp = expressions.And('a', 'b')

        resolved = exp.resolve_against(None)

        self.assertIsInstance(resolved, expressions.And)
        self.assertIs(resolved.lhs, exp.lhs)
        self.assertIs(resolved.rhs, exp.rhs)
