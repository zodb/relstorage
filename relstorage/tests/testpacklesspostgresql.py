##############################################################################
#
# Copyright (c) 2008-2009 Zope Foundation and Contributors.
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
"""Tests of relstorage.adapters.packless.postgresql"""

import logging
import unittest

from relstorage.tests import reltestbase
from relstorage.tests import packlesstestbase
from relstorage.adapters.packless.postgresql import PacklessPostgreSQLAdapter


class UsePacklessPostgreSQLAdapter:
    def make_adapter(self):
        return PacklessPostgreSQLAdapter(
        'dbname=relstoragetestp user=relstoragetest password=relstoragetest')

class PacklessPostgreSQLTests(
        UsePacklessPostgreSQLAdapter,
        packlesstestbase.PacklessRelStorageTests):
    pass

class PacklessPGToFile(
        UsePacklessPostgreSQLAdapter, reltestbase.ToFileStorage):
    pass

class PacklessFileToPG(
        UsePacklessPostgreSQLAdapter, reltestbase.FromFileStorage):
    pass


def test_suite():
    suite = unittest.TestSuite()
    for klass in [PacklessPostgreSQLTests, PacklessPGToFile, PacklessFileToPG]:
        suite.addTest(unittest.makeSuite(klass, "check"))
    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")

