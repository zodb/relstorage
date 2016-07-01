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
"""Tests of relstorage.adapters.oracle"""

from relstorage.options import Options
from relstorage.tests.hftestbase import HistoryFreeFromFileStorage
from relstorage.tests.hftestbase import HistoryFreeRelStorageTests
from relstorage.tests.hftestbase import HistoryFreeToFileStorage
from relstorage.tests.hptestbase import HistoryPreservingFromFileStorage
from relstorage.tests.hptestbase import HistoryPreservingRelStorageTests
from relstorage.tests.hptestbase import HistoryPreservingToFileStorage
import logging
import os
import sys
import unittest


base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UseOracleAdapter(object):

    def make_adapter(self, options):
        from relstorage.adapters.oracle import OracleAdapter
        dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
        if self.keep_history:
            db = base_dbname
        else:
            db = base_dbname + '_hf'
        return OracleAdapter(
            user=db,
            password='relstoragetest',
            dsn=dsn,
            options=options,
        )


class ZConfigTests(object):

    def checkConfigureViaZConfig(self):
        import tempfile
        dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
        fd, replica_conf = tempfile.mkstemp()
        os.write(fd, dsn.encode("ascii"))
        os.close(fd)
        try:
            if self.keep_history:
                dbname = base_dbname
            else:
                dbname = base_dbname + '_hf'
            conf = u"""
            %%import relstorage
            <zodb main>
              <relstorage>
                name xyz
                read-only false
                keep-history %s
                replica-conf %s
                blob-chunk-size 10MB
                <oracle>
                  user %s
                  password relstoragetest
                  dsn %s
                </oracle>
              </relstorage>
            </zodb>
            """ % (
                self.keep_history and 'true' or 'false',
                replica_conf,
                dbname,
                dsn,
                )

            schema_xml = u"""
            <schema>
            <import package="ZODB"/>
            <section type="ZODB.database" name="main" attribute="database"/>
            </schema>
            """
            import ZConfig
            from io import StringIO
            schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
            config, _handler = ZConfig.loadConfigFile(schema, StringIO(conf))

            db = config.database.open()
            try:
                storage = db.storage
                self.assertEqual(storage.isReadOnly(), False)
                self.assertEqual(storage.getName(), "xyz")
                adapter = storage._adapter
                from relstorage.adapters.oracle import OracleAdapter
                self.assertIsInstance(adapter, OracleAdapter)
                self.assertEqual(adapter._user, dbname)
                self.assertEqual(adapter._password, 'relstoragetest')
                self.assertEqual(adapter._dsn, dsn)
                self.assertEqual(adapter._twophase, False)
                self.assertEqual(adapter.keep_history, self.keep_history)
                self.assertEqual(
                    adapter.connmanager.replica_selector.replica_conf,
                    replica_conf)
                self.assertEqual(storage._options.blob_chunk_size, 10485760)
            finally:
                db.close()
        finally:
            os.remove(replica_conf)


class HPOracleTests(UseOracleAdapter, HistoryPreservingRelStorageTests,
                    ZConfigTests):
    pass

class HPOracleToFile(UseOracleAdapter, HistoryPreservingToFileStorage):
    pass

class HPOracleFromFile(UseOracleAdapter, HistoryPreservingFromFileStorage):
    pass

class HFOracleTests(UseOracleAdapter, HistoryFreeRelStorageTests,
                    ZConfigTests):
    pass

class HFOracleToFile(UseOracleAdapter, HistoryFreeToFileStorage):
    pass

class HFOracleFromFile(UseOracleAdapter, HistoryFreeFromFileStorage):
    pass

db_names = {
    'data': base_dbname,
    '1': base_dbname,
    '2': base_dbname + '2',
    'dest': base_dbname + '2',
    }

def test_suite():
    import relstorage.adapters.oracle as _adapter
    try:
        _adapter.select_driver()
    except ImportError:
        import warnings
        warnings.warn("cx_Oracle is not importable, so Oracle tests disabled")
        return unittest.TestSuite()

    suite = unittest.TestSuite()
    for klass in [
            HPOracleTests,
            HPOracleToFile,
            HPOracleFromFile,
            HFOracleTests,
            HFOracleToFile,
            HFOracleFromFile,
    ]:
        suite.addTest(unittest.makeSuite(klass, "check"))

    import ZODB.blob
    from .util import RUNNING_ON_CI
    if RUNNING_ON_CI or os.environ.get("RS_ORCL_SMALL_BLOB"):
        # cx_Oracle blob support can only address up to sys.maxint on
        # 32-bit systems, 4GB otherwise. This takes a great deal of time, however,
        # so allow tuning it down.
        from relstorage.adapters.mover import ObjectMover
        assert hasattr(ObjectMover, 'oracle_blob_chunk_maxsize')
        ObjectMover.oracle_blob_chunk_maxsize = 1024 * 1024 * 100
        large_blob_size = ObjectMover.oracle_blob_chunk_maxsize * 2
    else:
        large_blob_size = min(sys.maxsize, 1<<32)


    from relstorage.tests.blob.testblob import storage_reusable_suite
    dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
    from relstorage.tests.util import shared_blob_dir_choices
    for shared_blob_dir in shared_blob_dir_choices:
        for keep_history in (False, True):
            def create_storage(name, blob_dir,
                               shared_blob_dir=shared_blob_dir,
                               keep_history=keep_history, **kw):
                from relstorage.storage import RelStorage
                from relstorage.adapters.oracle import OracleAdapter
                db = db_names[name]
                if not keep_history:
                    db += '_hf'
                options = Options(
                    keep_history=keep_history,
                    shared_blob_dir=shared_blob_dir,
                    blob_dir=os.path.abspath(blob_dir),
                    **kw)
                adapter = OracleAdapter(
                    user=db,
                    password='relstoragetest',
                    dsn=dsn,
                    options=options,
                )
                storage = RelStorage(adapter, name=name, options=options)
                storage.zap_all()
                return storage

            prefix = 'Oracle%s%s' % (
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

            blob_size = large_blob_size

            suite.addTest(storage_reusable_suite(
                prefix, create_storage,
                test_blob_storage_recovery=True,
                test_packing=test_packing,
                test_undo=keep_history,
                pack_test_name=pack_test_name,
                test_blob_cache=(not shared_blob_dir),
                large_blob_size=(not shared_blob_dir) and blob_size + 100,
            ))

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")
