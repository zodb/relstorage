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
"""Tests of relstorage.adapters.mysql"""

import logging
import unittest
import reltestbase


class UseMySQLAdapter:
    def make_adapter(self):
        from relstorage.adapters.mysql import MySQLAdapter
        return MySQLAdapter(
            db='relstoragetest',
            user='relstoragetest',
            passwd='relstoragetest',
            )

class MySQLTests(UseMySQLAdapter, reltestbase.RelStorageTests):
    pass

class MySQLToFile(UseMySQLAdapter, reltestbase.ToFileStorage):
    pass

class FileToMySQL(UseMySQLAdapter, reltestbase.FromFileStorage):
    pass


def test_suite():
    try:
        import MySQLdb
    except ImportError, e:
        import warnings
        warnings.warn("MySQLdb is not importable, so MySQL tests disabled")
        return unittest.TestSuite()

    suite = unittest.TestSuite()
    for klass in [MySQLTests, MySQLToFile, FileToMySQL]:
        suite.addTest(unittest.makeSuite(klass, "check"))
    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")

