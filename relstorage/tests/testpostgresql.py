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
from __future__ import absolute_import
from relstorage.options import Options
from relstorage.tests.hftestbase import HistoryFreeFromFileStorage
from relstorage.tests.hftestbase import HistoryFreeRelStorageTests
from relstorage.tests.hftestbase import HistoryFreeToFileStorage
from relstorage.tests.hptestbase import HistoryPreservingFromFileStorage
from relstorage.tests.hptestbase import HistoryPreservingRelStorageTests
from relstorage.tests.hptestbase import HistoryPreservingToFileStorage
from .reltestbase import AbstractRSDestZodbConvertTests
from .reltestbase import AbstractRSSrcZodbConvertTests
import logging
import os
import unittest

# pylint:disable=no-member,too-many-ancestors


base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UsePostgreSQLAdapter(object):

    def make_adapter(self, options):
        from relstorage.adapters.postgresql import PostgreSQLAdapter
        if self.keep_history:
            db = base_dbname
        else:
            db = base_dbname + '_hf'
        return PostgreSQLAdapter(
            dsn='dbname=%s user=relstoragetest password=relstoragetest' % db,
            options=options,
        )


class ZConfigTests(object):

    def checkConfigureViaZConfig(self):
        replica_conf = os.path.join(os.path.dirname(__file__), 'replicas.conf')
        if self.keep_history:
            dbname = base_dbname
        else:
            dbname = base_dbname + '_hf'
        dsn = (
            "dbname='%s' user='relstoragetest' password='relstoragetest'"
            % dbname)
        conf = u"""
        %%import relstorage
        <zodb main>
            <relstorage>
            name xyz
            read-only false
            keep-history %s
            replica-conf %s
            blob-chunk-size 10MB
            <postgresql>
                driver auto
                dsn %s
            </postgresql>
            </relstorage>
        </zodb>
        """ % (
            self.keep_history and 'true' or 'false',
            replica_conf,
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
        config, _ = ZConfig.loadConfigFile(schema, StringIO(conf))

        db = config.database.open()
        try:
            storage = db.storage
            self.assertEqual(storage.isReadOnly(), False)
            self.assertEqual(storage.getName(), "xyz")
            adapter = storage._adapter
            from relstorage.adapters.postgresql import PostgreSQLAdapter
            self.assertIsInstance(adapter, PostgreSQLAdapter)
            self.assertEqual(adapter._dsn, dsn)
            self.assertEqual(adapter.keep_history, self.keep_history)
            self.assertEqual(
                adapter.connmanager.replica_selector.replica_conf,
                replica_conf)
            self.assertEqual(storage._options.blob_chunk_size, 10485760)
        finally:
            db.close()


class _PgSQLCfgMixin(object):

    def _relstorage_contents(self):
        return """
                <postgresql>
                   dsn dbname='relstoragetest' user='relstoragetest' password='relstoragetest'
                </postgresql>
        """

class HPPostgreSQLDestZODBConvertTests(UsePostgreSQLAdapter, _PgSQLCfgMixin, AbstractRSDestZodbConvertTests):
    pass

class HPPostgreSQLSrcZODBConvertTests(UsePostgreSQLAdapter, _PgSQLCfgMixin, AbstractRSSrcZodbConvertTests):
    pass

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
    # pylint:disable=too-many-locals
    import relstorage.adapters.postgresql as _adapter
    try:
        _adapter.select_driver()
    except ImportError:
        import warnings
        warnings.warn("No PostgreSQL driver is available, so PostgreSQL tests disabled")
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

    suite.addTest(unittest.makeSuite(HPPostgreSQLDestZODBConvertTests))
    suite.addTest(unittest.makeSuite(HPPostgreSQLSrcZODBConvertTests))

    from .util import RUNNING_ON_CI
    if RUNNING_ON_CI or os.environ.get("RS_PG_SMALL_BLOB"):
        # Avoid creating 2GB blobs to be friendly to neighbors
        # and to run fast (2GB blobs take about 4 minutes on Travis
        # CI as-of June 2016)
        # XXX: This is dirty.
        from relstorage.adapters.postgresql.mover import PostgreSQLObjectMover as ObjectMover
        assert hasattr(ObjectMover, 'postgresql_blob_chunk_maxsize')
        ObjectMover.postgresql_blob_chunk_maxsize = 1024 * 1024 * 10
        large_blob_size = ObjectMover.postgresql_blob_chunk_maxsize * 2
    else:
        large_blob_size = 1<<31

    from relstorage.tests.blob.testblob import storage_reusable_suite
    from relstorage.tests.util import shared_blob_dir_choices
    for shared_blob_dir in shared_blob_dir_choices:
        for keep_history in (False, True):
            def create_storage(name, blob_dir,
                               shared_blob_dir=shared_blob_dir,
                               keep_history=keep_history, **kw):
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
                    **kw)
                adapter = PostgreSQLAdapter(dsn=dsn, options=options)
                storage = RelStorage(adapter, name=name, options=options)
                storage.zap_all(slow=True)
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
                test_blob_cache=(not shared_blob_dir),
                # PostgreSQL blob chunks are max 2GB in size
                large_blob_size=(not shared_blob_dir) and (large_blob_size) + 100,
            ))

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
