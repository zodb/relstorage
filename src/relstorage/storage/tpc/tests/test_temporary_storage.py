# -*- coding: utf-8 -*-
"""
Tests for temporary_storage.py

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

class TestTemporaryStorage(unittest.TestCase):

    def _makeOne(self):
        from ..temporary_storage import TemporaryStorage
        return TemporaryStorage()

    def setUp(self):
        super(TestTemporaryStorage, self).setUp()
        from .. import temporary_storage
        temporary_storage.id = lambda _: 0xDEADBEEF

    def tearDown(self):
        from .. import temporary_storage
        del temporary_storage.id
        super(TestTemporaryStorage, self).tearDown()

    _EMPTY_STR = '<relstorage.storage.tpc.temporary_storage.TemporaryStorage at 0xdeadbeef len: 0>'

    def test_empty_str(self):
        temp = self._makeOne()
        s = str(temp)
        self.assertEqual(
            s,
            self._EMPTY_STR
        )

    def test_closed_str(self):
        temp = self._makeOne()
        temp.close()
        s = str(temp)
        self.assertEqual(s, self._EMPTY_STR)

    def test_str_with_data(self):
        from textwrap import dedent
        self.maxDiff = None
        temp = self._makeOne()
        temp.store_temp(6547, b'defghijkl', 23)
        temp.store_temp(1, b'abc')
        temp.store_temp(2, b'def', 42)

        s = str(temp)

        self.assertEqual(
            s,
            dedent("""\
            <relstorage.storage.tpc.temporary_storage.TemporaryStorage at 0xdeadbeef len: 3>
            ================================================================================
            | OID                      | Length                   | Previous TID            |
            ================================================================================
                                    1  |                        3 |                        0
                                    2  |                        3 |                       42
                                 6547  |                        9 |                       23
            """
                   )
        )
