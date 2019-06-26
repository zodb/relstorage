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

from ZODB.POSException import Unsupported

from relstorage.tests import TestCase
from relstorage.tests import MockOptions

from .. import mover

class MockDriver(object):
    pass

class TestFunctions(TestCase):

    def _prepare1(self, query, name='prepped', datatypes=()):
        return mover.to_prepared_queries(name, [query], datatypes)[0]

    def test_prepared_no_param_no_datatype(self):
        q = 'SELECT foo FROM bar'
        p = self._prepare1(q)
        self.assertEqual(
            'PREPARE prepped AS ' + q,
            p
        )

    def test_prepared_one_param_no_datatype(self):
        q = 'SELECT foo FROM bar WHERE foo = %s'
        p = self._prepare1(q)
        self.assertEqual(
            'PREPARE prepped AS SELECT foo FROM bar WHERE foo = $1',
            p
        )

    def test_prepared_one_param_one_datatype(self):
        q = 'SELECT foo FROM bar WHERE foo = %s'
        p = self._prepare1(q, datatypes=['int'])
        self.assertEqual(
            'PREPARE prepped (int) AS SELECT foo FROM bar WHERE foo = $1',
            p
        )

    def test_prepared_two_param_two_datatype(self):
        q = 'SELECT foo FROM bar WHERE foo = %s and biz = %s'
        p = self._prepare1(q, datatypes=['int', 'bigint'])
        self.assertEqual(
            'PREPARE prepped (int, bigint) AS SELECT foo FROM bar WHERE foo = $1 '
            'and biz = $2',
            p
        )

    maxDiff = None

    def test_prepare_load_current(self):
        self.assertEqual(
            mover.PostgreSQLObjectMover._prepare_load_current_queries,
            [
                'PREPARE load_current (BIGINT) AS SELECT state, tid\n'
                '        FROM current_object\n'
                '        JOIN object_state USING(zoid, tid)\n'
                '        WHERE zoid = $1',
                'PREPARE load_current (BIGINT) AS SELECT state, tid\n'
                '        FROM object_state\n'
                '        WHERE zoid = $1'
            ]
        )

class TestPostgreSQLObjectMover(TestCase):

    def _getClass(self):
        return mover.PostgreSQLObjectMover

    def _makeOne(self, **options):
        return self._getClass()(MockDriver(),
                                MockOptions.from_args(**options))

    _expected_move_from_temp_hf_insert_query = 'EXECUTE move_from_temp(%s)'

    def test_prep_statements_hf(self):
        inst = self._makeOne(keep_history=False)
        self.assertEqual(
            inst._move_from_temp_hf_insert_query,
            self._expected_move_from_temp_hf_insert_query
        )

    def test_prep_statements_hp(self):
        inst = self._makeOne(keep_history=True)
        with self.assertRaises(Unsupported):
            getattr(inst, '_move_from_temp_hf_insert_query')


class TestPG8000ObjectMover(TestPostgreSQLObjectMover):

    def setUp(self):
        super(TestPG8000ObjectMover, self).setUp()
        raw = mover.PostgreSQLObjectMover._move_from_temp_hf_insert_query_raw
        self._expected_move_from_temp_hf_insert_query = raw

    def _getClass(self):
        return mover.PG8000ObjectMover
