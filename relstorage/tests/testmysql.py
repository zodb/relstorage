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

from relstorage.tests.hftestbase import HistoryFreeFromFileStorage
from relstorage.tests.hftestbase import HistoryFreeRelStorageTests
from relstorage.tests.hftestbase import HistoryFreeToFileStorage
from relstorage.tests.hptestbase import HistoryPreservingFromFileStorage
from relstorage.tests.hptestbase import HistoryPreservingRelStorageTests
from relstorage.tests.hptestbase import HistoryPreservingToFileStorage
import logging
import os
import unittest

class UseMySQLAdapter:
    def make_adapter(self):
        from relstorage.adapters.mysql import MySQLAdapter
        if self.keep_history:
            db = 'relstoragetest'
        else:
            db = 'relstoragetest_hf'
        return MySQLAdapter(
            keep_history=self.keep_history,
            db=db,
            user='relstoragetest',
            passwd='relstoragetest',
            )

class HPMySQLTests(UseMySQLAdapter, HistoryPreservingRelStorageTests):
    pass

class HPMySQLToFile(UseMySQLAdapter, HistoryPreservingToFileStorage):
    pass

class HPMySQLFromFile(UseMySQLAdapter, HistoryPreservingFromFileStorage):
    pass

class HFMySQLTests(UseMySQLAdapter, HistoryFreeRelStorageTests):
    pass

class HFMySQLToFile(UseMySQLAdapter, HistoryFreeToFileStorage):
    pass

class HFMySQLFromFile(UseMySQLAdapter, HistoryFreeFromFileStorage):
    pass

db_names = {
    'data': 'relstoragetest',
    '1': 'relstoragetest',
    '2': 'relstoragetest2',
    'dest': 'relstoragetest2',
    }

def test_suite():
    suite = unittest.TestSuite()
    for klass in [
            HPMySQLTests,
            HPMySQLToFile,
            HPMySQLFromFile,
            HFMySQLTests,
            HFMySQLToFile,
            HFMySQLFromFile,
            ]:
        suite.addTest(unittest.makeSuite(klass, "check"))

    try:
        import ZODB.blob
    except ImportError:
        # ZODB < 3.8
        pass
    else:
        from relstorage.tests.blob.testblob import storage_reusable_suite
        for keep_history in (False, True):
            def create_storage(name, blob_dir, keep_history=keep_history):
                from relstorage.storage import RelStorage
                from relstorage.adapters.mysql import MySQLAdapter
                db = db_names[name]
                if not keep_history:
                    db += '_hf'
                adapter = MySQLAdapter(
                    keep_history=keep_history,
                    db=db,
                    user='relstoragetest',
                    passwd='relstoragetest',
                    )
                storage = RelStorage(adapter, name=name, create=True,
                    blob_dir=os.path.abspath(blob_dir))
                storage.zap_all()
                return storage

            if keep_history:
                prefix = 'HPMySQL'
                pack_test_name = 'blob_packing.txt'
            else:
                prefix = 'HFMySQL'
                pack_test_name = 'blob_packing_history_free.txt'

            suite.addTest(storage_reusable_suite(
                prefix, create_storage,
                test_blob_storage_recovery=True,
                test_packing=True,
                test_undo=keep_history,
                pack_test_name=pack_test_name,
                ))

    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")

