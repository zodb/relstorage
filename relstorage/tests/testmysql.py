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
from __future__ import absolute_import
from relstorage.options import Options
from relstorage.tests.hftestbase import HistoryFreeFromFileStorage
from relstorage.tests.hftestbase import HistoryFreeRelStorageTests
from relstorage.tests.hftestbase import HistoryFreeToFileStorage
from relstorage.tests.hptestbase import HistoryPreservingFromFileStorage
from relstorage.tests.hptestbase import HistoryPreservingRelStorageTests
from relstorage.tests.hptestbase import HistoryPreservingToFileStorage
from .util import skipOnCI
from .reltestbase import AbstractRSDestZodbConvertTests
from .reltestbase import AbstractRSSrcZodbConvertTests
import logging
import os
import unittest


base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UseMySQLAdapter(object):

    def make_adapter(self, options):
        from relstorage.adapters.mysql import MySQLAdapter
        if self.keep_history:
            db = base_dbname
        else:
            db = base_dbname + '_hf'
        return MySQLAdapter(
            options=options,
            db=db,
            user='relstoragetest',
            passwd='relstoragetest',
        )


class ZConfigTests(object):

    def checkConfigureViaZConfig(self):
        replica_conf = os.path.join(os.path.dirname(__file__), 'replicas.conf')
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
            <mysql>
                driver auto
                db %s
                user relstoragetest
                passwd relstoragetest
            </mysql>
            </relstorage>
        </zodb>
        """ % (
            self.keep_history and 'true' or 'false',
            replica_conf,
            dbname,
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
        config, _ = ZConfig.loadConfigFile(schema, StringIO(conf))

        db = config.database.open()
        try:
            storage = db.storage
            self.assertEqual(storage.isReadOnly(), False)
            self.assertEqual(storage.getName(), "xyz")
            adapter = storage._adapter
            from relstorage.adapters.mysql import MySQLAdapter
            self.assertIsInstance(adapter, MySQLAdapter)
            self.assertEqual(adapter._params, {
                'passwd': 'relstoragetest',
                'db': dbname,
                'user': 'relstoragetest',
                })
            self.assertEqual(adapter.keep_history, self.keep_history)
            self.assertEqual(
                adapter.connmanager.replica_selector.replica_conf,
                replica_conf)
            self.assertEqual(storage._options.blob_chunk_size, 10485760)
        finally:
            db.close()

class _MySQLCfgMixin(object):

    def _relstorage_contents(self):
        return """
                <mysql>
                   db relstoragetest
                   user relstoragetest
                   passwd relstoragetest
                </mysql>
        """

class HPMySQLDestZODBConvertTests(UseMySQLAdapter, _MySQLCfgMixin, AbstractRSDestZodbConvertTests):
    pass

class HPMySQLSrcZODBConvertTests(UseMySQLAdapter, _MySQLCfgMixin, AbstractRSSrcZodbConvertTests):
    pass


class HPMySQLTests(UseMySQLAdapter, HistoryPreservingRelStorageTests,
                   ZConfigTests):
    @skipOnCI("Travis MySQL goes away error 2006")
    def check16MObject(self):
        # NOTE: If your mySQL goes away, check the server's value for
        # `max_allowed_packet`, you probably need to increase it.
        # JAM uses 64M.
        # http://dev.mysql.com/doc/refman/5.7/en/packet-too-large.html
        super(HPMySQLTests, self).check16MObject()

class HPMySQLToFile(UseMySQLAdapter, HistoryPreservingToFileStorage):
    pass

class HPMySQLFromFile(UseMySQLAdapter, HistoryPreservingFromFileStorage):
    pass

class HFMySQLTests(UseMySQLAdapter, HistoryFreeRelStorageTests,
                   ZConfigTests):

    @skipOnCI("Travis MySQL goes away error 2006")
    def check16MObject(self):
        # See note in HPMySQLTests.check16MObject.
        super(HFMySQLTests, self).check16MObject()

class HFMySQLToFile(UseMySQLAdapter, HistoryFreeToFileStorage):
    pass

class HFMySQLFromFile(UseMySQLAdapter, HistoryFreeFromFileStorage):
    pass

db_names = {
    'data': base_dbname,
    '1': base_dbname,
    '2': base_dbname + '2',
    'dest': base_dbname + '2',
    }

def test_suite():
    import relstorage.adapters.mysql as _adapter
    try:
        _adapter.select_driver()
    except ImportError:
        import warnings
        warnings.warn("No MySQL driver is available, so MySQL tests disabled")
        return unittest.TestSuite()

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

    suite.addTest(unittest.makeSuite(HPMySQLDestZODBConvertTests))
    suite.addTest(unittest.makeSuite(HPMySQLSrcZODBConvertTests))

    import ZODB.blob
    from relstorage.tests.blob.testblob import storage_reusable_suite
    from relstorage.tests.util import shared_blob_dir_choices
    for shared_blob_dir in shared_blob_dir_choices:
        for keep_history in (False, True):
            def create_storage(name, blob_dir,
                               shared_blob_dir=shared_blob_dir,
                               keep_history=keep_history, **kw):
                from relstorage.storage import RelStorage
                from relstorage.adapters.mysql import MySQLAdapter
                db = db_names[name]
                if not keep_history:
                    db += '_hf'
                options = Options(
                    keep_history=keep_history,
                    shared_blob_dir=shared_blob_dir,
                    blob_dir=os.path.abspath(blob_dir),
                    **kw)
                adapter = MySQLAdapter(
                    options=options,
                    db=db,
                    user='relstoragetest',
                    passwd='relstoragetest',
                )
                storage = RelStorage(adapter, name=name, options=options)
                storage.zap_all()
                return storage

            prefix = 'MySQL%s%s' % (
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

            # MySQL is limited to the blob_chunk_size as there is no
            # native blob streaming support.
            blob_size = Options().blob_chunk_size

            suite.addTest(storage_reusable_suite(
                prefix, create_storage,
                test_blob_storage_recovery=True,
                test_packing=test_packing,
                test_undo=keep_history,
                pack_test_name=pack_test_name,
                test_blob_cache=(not shared_blob_dir),
                large_blob_size=(not shared_blob_dir) and blob_size + 100
            ))

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
