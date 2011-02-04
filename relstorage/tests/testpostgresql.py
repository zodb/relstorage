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

from relstorage.options import Options
from relstorage.tests.hftestbase import HistoryFreeFromFileStorage
from relstorage.tests.hftestbase import HistoryFreeRelStorageTests
from relstorage.tests.hftestbase import HistoryFreeToFileStorage
from relstorage.tests.hptestbase import HistoryPreservingFromFileStorage
from relstorage.tests.hptestbase import HistoryPreservingRelStorageTests
from relstorage.tests.hptestbase import HistoryPreservingToFileStorage
import logging
import os
import unittest


base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UsePostgreSQLAdapter:
    def make_adapter(self):
        from relstorage.adapters.postgresql import PostgreSQLAdapter
        if self.keep_history:
            db = base_dbname
        else:
            db = base_dbname + '_hf'
        return PostgreSQLAdapter(
            dsn='dbname=%s user=relstoragetest password=relstoragetest' % db,
            options=Options(keep_history=self.keep_history),
            )


class ZConfigTests:

    def checkConfigureViaZConfig(self):
        replica_conf = os.path.join(os.path.dirname(__file__), 'replicas.conf')
        if self.keep_history:
            dbname = base_dbname
        else:
            dbname = base_dbname + '_hf'
        dsn = (
            "dbname='%s' user='relstoragetest' password='relstoragetest'"
            % dbname)
        conf = """
        %%import relstorage
        <zodb main>
            <relstorage>
            name xyz
            read-only false
            keep-history %s
            replica-conf %s
            <postgresql>
                dsn %s
            </postgresql>
            </relstorage>
        </zodb>
        """ % (
            self.keep_history and 'true' or 'false',
            replica_conf,
            dsn,
            )

        schema_xml = """
        <schema>
        <import package="ZODB"/>
        <section type="ZODB.database" name="main" attribute="database"/>
        </schema>
        """
        import ZConfig
        from StringIO import StringIO
        schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
        config, handler = ZConfig.loadConfigFile(schema, StringIO(conf))

        db = config.database.open()
        try:
            storage = getattr(db, 'storage', None)
            if storage is None:
                # ZODB < 3.9
                storage = db._storage
            self.assertEqual(storage.isReadOnly(), False)
            self.assertEqual(storage.getName(), "xyz")
            adapter = storage._adapter
            from relstorage.adapters.postgresql import PostgreSQLAdapter
            self.assert_(isinstance(adapter, PostgreSQLAdapter))
            self.assertEqual(adapter._dsn, dsn)
            self.assertEqual(adapter.keep_history, self.keep_history)
            self.assertEqual(
                adapter.connmanager.replica_selector.replica_conf,
                replica_conf)
        finally:
            db.close()


class HPPostgreSQLTests(UsePostgreSQLAdapter, HistoryPreservingRelStorageTests,
        ZConfigTests):
    pass

class HPPostgreSQLToFile(UsePostgreSQLAdapter, HistoryPreservingToFileStorage):
    pass

class HPPostgreSQLFromFile(UsePostgreSQLAdapter,
        HistoryPreservingFromFileStorage):
    pass

class HFPostgreSQLTests(UsePostgreSQLAdapter, HistoryFreeRelStorageTests,
        ZConfigTests):
    pass

class HFPostgreSQLToFile(UsePostgreSQLAdapter, HistoryFreeToFileStorage):
    pass

class HFPostgreSQLFromFile(UsePostgreSQLAdapter, HistoryFreeFromFileStorage):
    pass

db_names = {
    'data': base_dbname,
    '1': base_dbname,
    '2': base_dbname + '2',
    'dest': base_dbname + '2',
    }

def test_suite():
    try:
        import psycopg2
    except ImportError, e:
        import warnings
        warnings.warn(
            "psycopg2 is not importable, so PostgreSQL tests disabled")
        return unittest.TestSuite()

    suite = unittest.TestSuite()
    for klass in [
            HPPostgreSQLTests,
            HPPostgreSQLToFile,
            HPPostgreSQLFromFile,
            HFPostgreSQLTests,
            HFPostgreSQLToFile,
            HFPostgreSQLFromFile,
            ]:
        suite.addTest(unittest.makeSuite(klass, "check"))

    try:
        import ZODB.blob
    except ImportError:
        # ZODB < 3.8
        pass
    else:
        from relstorage.tests.blob.testblob import storage_reusable_suite
        for shared_blob_dir in (False, True):
            for keep_history in (False, True):
                def create_storage(name, blob_dir,
                        shared_blob_dir=shared_blob_dir,
                        keep_history=keep_history):
                    from relstorage.storage import RelStorage
                    from relstorage.adapters.postgresql import PostgreSQLAdapter
                    db = db_names[name]
                    if not keep_history:
                        db += '_hf'
                    dsn = ('dbname=%s user=relstoragetest '
                            'password=relstoragetest' % db)
                    options = Options(
                        keep_history=keep_history,
                        shared_blob_dir=shared_blob_dir,
                        blob_dir=os.path.abspath(blob_dir),
                    )
                    adapter = PostgreSQLAdapter(dsn=dsn, options=options)
                    storage = RelStorage(adapter, name=name, options=options)
                    storage.zap_all()
                    return storage

                prefix = 'PostgreSQL%s%s' % (
                    (shared_blob_dir and 'Shared' or 'Unshared'),
                    (keep_history and 'WithHistory' or 'NoHistory'),
                )

                # If the blob directory is a cache, don't test packing,
                # since packing can not remove blobs from all caches.
                test_packing = shared_blob_dir

                if keep_history:
                    pack_test_name = 'blob_packing.txt'
                else:
                    pack_test_name = 'blob_packing_history_free.txt'

                suite.addTest(storage_reusable_suite(
                    prefix, create_storage,
                    test_blob_storage_recovery=True,
                    test_packing=test_packing,
                    test_undo=keep_history,
                    pack_test_name=pack_test_name,
                    ))

    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")

