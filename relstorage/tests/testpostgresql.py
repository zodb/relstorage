##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""Tests of relstorage.adapters.postgresql"""

import logging
import unittest

from reltestbase import RelStorageTests
from relstorage.adapters.postgresql import PostgreSQLAdapter


class PostgreSQLTests(RelStorageTests):
    def make_adapter(self):
        return PostgreSQLAdapter()

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(PostgreSQLTests, "check"))
    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")

