##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""Combines the tests of all adapters"""

import unittest

from testpostgresql import test_suite as postgresql_test_suite
from testmysql import test_suite as mysql_test_suite
from testoracle import test_suite as oracle_test_suite

def make_suite():
    suite = unittest.TestSuite()
    suite.addTest(postgresql_test_suite())
    suite.addTest(mysql_test_suite())
    suite.addTest(oracle_test_suite())
    return suite
