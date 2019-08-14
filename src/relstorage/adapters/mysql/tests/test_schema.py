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
from relstorage.tests import MockCursor

from ..schema import MySQLVersionDetector as MVD

class TestMySQLVersionDetector(TestCase):

    def test_5(self):
        mvd = MVD()
        cursor = MockCursor()
        cursor.results.append((b'5.7.27-log',))
        vi = mvd.get_version_info(cursor)
        self.assertEqual(vi, (5, 7, 27))
        self.assertEqual(mvd.get_major_version(cursor), 5)
        self.assertFalse(mvd.supports_nowait(None))
        self.assertEqual(mvd.get_version(cursor), '5.7.27-log')
        self.assertGreater(mvd.get_version_info(cursor), (5, 7, 26))
        self.assertGreater(mvd.get_version_info(cursor), (5, 7, 20))
        self.assertTrue(mvd.supports_good_stored_procs(None))

        self.assertEqual([('SELECT version()', None)], cursor.executed)

    def test_8(self):
        mvd = MVD()
        cursor = MockCursor()
        cursor.results.append((b'8.0.19-standard',))
        vi = mvd.get_version_info(cursor)
        self.assertEqual(vi, (8, 0, 19))
        self.assertEqual(mvd.get_major_version(cursor), 8)
        self.assertTrue(mvd.supports_nowait(None))
        self.assertTrue(mvd.supports_transaction_isolation(None))
        self.assertEqual(mvd.get_version(cursor), '8.0.19-standard')
        self.assertGreater(mvd.get_version_info(cursor), (5, 7, 20))
        self.assertEqual([('SELECT version()', None)], cursor.executed)
