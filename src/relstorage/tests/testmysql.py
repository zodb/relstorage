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

import logging
import os
import unittest

from relstorage.adapters.mysql import MySQLAdapter
from relstorage.options import Options
from relstorage.storage import RelStorage
from relstorage.tests.hftestbase import HistoryFreeFromFileStorage
from relstorage.tests.hftestbase import HistoryFreeRelStorageTests
from relstorage.tests.hftestbase import HistoryFreeToFileStorage
from relstorage.tests.hptestbase import HistoryPreservingFromFileStorage
from relstorage.tests.hptestbase import HistoryPreservingRelStorageTests
from relstorage.tests.hptestbase import HistoryPreservingToFileStorage

from .reltestbase import AbstractRSDestZodbConvertTests
from .reltestbase import AbstractRSSrcZodbConvertTests
from .util import skipOnCI

# pylint:disable=no-member,too-many-ancestors

base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UseMySQLAdapter(object):

    def make_adapter(self, options):
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

    driver_name = None # Override

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
            cache-local-dir-read-count 12
            cache-local-dir-write-max-size 10MB
            <mysql>
                driver %s
                db %s
                user relstoragetest
                passwd relstoragetest
            </mysql>
            </relstorage>
        </zodb>
        """ % (
            'true' if self.keep_history else 'false',
            replica_conf,
            self.driver_name,
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
            driver %s
            db relstoragetest
            user relstoragetest
            passwd relstoragetest
        </mysql>
        """ % (self.driver_name,)

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

        # This fails if the driver is umysqldb.
        try:
            super(HPMySQLTests, self).check16MObject()
        except Exception as e:
            if e.args == (0, 'Query too big'):
                raise unittest.SkipTest("Fails with umysqldb")
            raise

class HPMySQLToFile(UseMySQLAdapter, HistoryPreservingToFileStorage):
    pass

class HPMySQLFromFile(UseMySQLAdapter, HistoryPreservingFromFileStorage):
    pass

class HFMySQLTests(UseMySQLAdapter, HistoryFreeRelStorageTests,
                   ZConfigTests):

    @skipOnCI("Travis MySQL goes away error 2006")
    def check16MObject(self):
        # See note in HPMySQLTests.check16MObject.
        try:
            super(HFMySQLTests, self).check16MObject()
        except Exception as e:
            if e.args == (0, 'Query too big'):
                raise unittest.SkipTest("Fails with umysqldb")
            raise


class HFMySQLToFile(UseMySQLAdapter, HistoryFreeToFileStorage):
    pass

class HFMySQLFromFile(UseMySQLAdapter, HistoryFreeFromFileStorage):
    pass

class TestOIDAllocator(unittest.TestCase):

    def test_bad_rowid(self):
        from relstorage.adapters.mysql.oidallocator import MySQLOIDAllocator
        class Cursor(object):
            def execute(self, s):
                pass
            lastrowid = None

        oids = MySQLOIDAllocator(KeyError)
        self.assertRaises(KeyError, oids.new_oids, Cursor())

db_names = {
    'data': base_dbname,
    '1': base_dbname,
    '2': base_dbname + '2',
    'dest': base_dbname + '2',
}

def test_suite():
    from relstorage.adapters.mysql import drivers
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

    suite.addTest(unittest.makeSuite(TestOIDAllocator))
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
            HPMySQLTests,
            HPMySQLToFile,
            HPMySQLFromFile,
            HFMySQLTests,
            HFMySQLToFile,
            HFMySQLFromFile,
    ]:
        klass = new_class_for_driver(driver_name, klass, is_available)
        suite.addTest(unittest.makeSuite(klass, "check"))

    suite.addTest(unittest.makeSuite(
        new_class_for_driver(driver_name,
                             HPMySQLDestZODBConvertTests,
                             is_available)))
    suite.addTest(unittest.makeSuite(
        new_class_for_driver(driver_name,
                             HPMySQLSrcZODBConvertTests,
                             is_available)))


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

            prefix = 'MySQL%s%s_%s' % (
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

            # MySQL is limited to the blob_chunk_size as there is no
            # native blob streaming support. (Note: this depends on
            # the max_allowed_packet size on the server as well as the driver; both
            # values default to 1MB. But umysqldb needs 1.3MB max_allowed_packet size
            # to send multiple 1MB chunks. So keep it small.)
            blob_size = Options().blob_chunk_size

            suite.addTest(storage_reusable_suite(
                prefix, create_storage,
                test_blob_storage_recovery=True,
                test_packing=test_packing,
                test_undo=keep_history,
                pack_test_name=pack_test_name,
                test_blob_cache=(not shared_blob_dir),
                large_blob_size=(not shared_blob_dir) and blob_size + 100,
                storage_is_available=is_available,
            ))

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
