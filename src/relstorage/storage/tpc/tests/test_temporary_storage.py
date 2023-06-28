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
        from ..temporary_storage import HPTPCTemporaryStorage
        return HPTPCTemporaryStorage()

    def setUp(self):
        super().setUp()
        from .. import temporary_storage
        setattr(temporary_storage, 'id', lambda _: 0xDEADBEEF)

    def tearDown(self):
        from .. import temporary_storage
        delattr(temporary_storage, 'id')
        super().tearDown()

    _EMPTY_STR = '<HPTPCTemporaryStorage at 0xdeadbeef count=0 bytes=0>'

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
            <HPTPCTemporaryStorage at 0xdeadbeef count=3 bytes=15>
            ======================================================
            | OID             | Length          | Previous TID   |
            ======================================================
                           1  |               3 |               0
                           2  |               3 |              42
                        6547  |               9 |              23
            """
                   )
        )
