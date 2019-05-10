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

import logging
import os
import unittest

from relstorage.storage import RelStorage
from relstorage.options import Options
from relstorage.adapters.postgresql import PostgreSQLAdapter

from .hftestbase import HistoryFreeFromFileStorage
from .hftestbase import HistoryFreeRelStorageTests
from .hftestbase import HistoryFreeToFileStorage
from .hptestbase import HistoryPreservingFromFileStorage
from .hptestbase import HistoryPreservingRelStorageTests
from .hptestbase import HistoryPreservingToFileStorage
from .reltestbase import AbstractRSDestZodbConvertTests
from .reltestbase import AbstractRSSrcZodbConvertTests


# pylint:disable=no-member,too-many-ancestors


base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UsePostgreSQLAdapter(object):

    def make_adapter(self, options):
        if self.keep_history:
            db = base_dbname
        else:
            db = base_dbname + '_hf'
        return PostgreSQLAdapter(
            dsn='dbname=%s user=relstoragetest password=relstoragetest' % db,
            options=options,
        )


class ZConfigTests(object):

    driver_name = None # Override

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
                driver %s
                dsn %s
            </postgresql>
            </relstorage>
        </zodb>
        """ % (
            'true' if self.keep_history else 'false',
            replica_conf,
            self.driver_name,
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
            self.assertFalse(storage.isReadOnly())
            self.assertEqual(storage.getName(), "xyz")
            adapter = storage._adapter
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
            driver %s
            dsn dbname='relstoragetest' user='relstoragetest' password='relstoragetest'
        </postgresql>
        """ % (self.driver_name,)

class HPPostgreSQLDestZODBConvertTests(UsePostgreSQLAdapter,
                                       _PgSQLCfgMixin,
                                       AbstractRSDestZodbConvertTests):
    pass

class HPPostgreSQLSrcZODBConvertTests(UsePostgreSQLAdapter,
                                      _PgSQLCfgMixin,
                                      AbstractRSSrcZodbConvertTests):
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
    from relstorage.adapters.postgresql import drivers
    from relstorage.adapters.interfaces import DriverNotAvailableError

    suite = unittest.TestSuite()
    for driver_name in drivers.known_driver_names():
        try:
            drivers.select_driver(driver_name)
        except DriverNotAvailableError:
            available = False
        else:
            available = True
        add_driver_to_suite(driver_name, suite, available)
    return suite

def new_class_for_driver(driver_name, base, is_available):
    klass = type(
        base.__name__ + '_' + driver_name,
        (base,),
        {'driver_name': driver_name}
    )
    klass = unittest.skipUnless(is_available, "Driver %s is not installed" % (driver_name,))(klass)
    return klass

def add_driver_to_suite(driver_name, suite, is_available):
    for klass in [
            HPPostgreSQLTests,
            HPPostgreSQLToFile,
            HPPostgreSQLFromFile,
            HFPostgreSQLTests,
            HFPostgreSQLToFile,
            HFPostgreSQLFromFile,
    ]:
        klass = new_class_for_driver(driver_name, klass, is_available)
        suite.addTest(unittest.makeSuite(klass, "check"))

    suite.addTest(unittest.makeSuite(
        new_class_for_driver(driver_name,
                             HPPostgreSQLDestZODBConvertTests,
                             is_available)))
    suite.addTest(unittest.makeSuite(
        new_class_for_driver(driver_name,
                             HPPostgreSQLSrcZODBConvertTests,
                             is_available)))

    from .util import USE_SMALL_BLOBS
    if USE_SMALL_BLOBS:
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
                if not is_available:
                    raise unittest.SkipTest("Driver %s is not installed" % (driver_name,))
                assert driver_name not in kw
                kw['driver'] = driver_name
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

            prefix = 'PostgreSQL%s%s_%s' % (
                'Shared' if shared_blob_dir else 'Unshared',
                'WithHistory' if keep_history else 'NoHistory',
                driver_name,
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
                storage_is_available=is_available
            ))

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
