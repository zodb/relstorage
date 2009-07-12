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
"""Tests of relstorage.adapters.postgresql"""

import logging
import unittest

import reltestbase
from relstorage.adapters.postgresql import PostgreSQLAdapter


class UsePostgreSQLAdapter:
    def make_adapter(self):
        return PostgreSQLAdapter(
            'dbname=relstoragetest user=relstoragetest password=relstoragetest')

class PostgreSQLTests(UsePostgreSQLAdapter, reltestbase.RelStorageTests):
    pass

class PGToFile(UsePostgreSQLAdapter, reltestbase.ToFileStorage):
    pass

class FileToPG(UsePostgreSQLAdapter, reltestbase.FromFileStorage):
    pass

db_names = {
    'data': 'relstoragetest',
    '1': 'relstoragetest',
    '2': 'relstoragetest2',
    'dest': 'relstoragetest2',
    }

def test_suite():
    suite = unittest.TestSuite()
    for klass in [PostgreSQLTests, PGToFile, FileToPG]:
        suite.addTest(unittest.makeSuite(klass, "check"))

    try:
        from ZODB.tests.testblob import storage_reusable_suite
    except ImportError:
        # ZODB < 3.9
        pass
    else:
        def create_storage(name, blob_dir):
            from relstorage.relstorage import RelStorage
            adapter = PostgreSQLAdapter(
                'dbname=%s user=relstoragetest password=relstoragetest' %
                db_names[name])
            storage = RelStorage(adapter, name=name, create=True,
                blob_dir=blob_dir)
            storage.zap_all()
            return storage

        suite.addTest(storage_reusable_suite(
            'PostgreSQL', create_storage,
            test_blob_storage_recovery=True,
            test_packing=True,
            ))

    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")

