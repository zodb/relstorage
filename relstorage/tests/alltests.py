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
from __future__ import absolute_import
import unittest
import logging

from .testpostgresql import test_suite as postgresql_test_suite
from .testmysql import test_suite as mysql_test_suite
from .testoracle import test_suite as oracle_test_suite

def make_suite():
    suite = unittest.TestSuite()
    suite.addTest(postgresql_test_suite())
    suite.addTest(mysql_test_suite())
    suite.addTest(oracle_test_suite())
    return suite

if __name__ == '__main__':
    logging.basicConfig()
    # We get constant errors about failing to lock a blob file,
    # which really bloats the CI logs, so turn those off.
    logging.getLogger('zc.lockfile').setLevel(logging.CRITICAL)
    unittest.main(defaultTest='make_suite')
