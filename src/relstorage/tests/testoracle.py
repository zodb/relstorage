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
import logging
import os
import sys
import unittest

from .util import AbstractTestSuiteBuilder

class UseOracleAdapter(object):

    keep_history = False
    base_dbname = None

    def make_adapter(self, options, db=None):
        from relstorage.adapters.oracle import OracleAdapter
        dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
        if db is None:
            if self.keep_history:
                db = self.base_dbname
            else:
                db = self.base_dbname + '_hf'
        return OracleAdapter(
            user=db,
            password='relstoragetest',
            dsn=dsn,
            options=options,
        )


class ZConfigTests(object):
    # pylint:disable=no-member
    driver_name = None # Override
    base_dbname = None

    def checkConfigureViaZConfig(self):
        # pylint:disable=too-many-locals
        import tempfile
        dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
        fd, replica_conf = tempfile.mkstemp()
        os.write(fd, dsn.encode("ascii"))
        os.close(fd)
        try:
            if self.keep_history:
                dbname = self.base_dbname
            else:
                dbname = self.base_dbname + '_hf'
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
                  driver %s
                  user %s
                  password relstoragetest
                  dsn %s
                </oracle>
              </relstorage>
            </zodb>
            """ % (
                'true' if self.keep_history else 'false',
                replica_conf,
                self.driver_name,
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



class OracleTestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'Oracle'

    def __init__(self):
        from relstorage.adapters.oracle import drivers
        super(OracleTestSuiteBuilder, self).__init__(
            drivers,
            UseOracleAdapter,
            None,
        )

    def _compute_large_blob_size(self, use_small_blobs):
        if use_small_blobs:
            # cx_Oracle blob support can only address up to sys.maxint on
            # 32-bit systems, 4GB otherwise. This takes a great deal of time, however,
            # so allow tuning it down.
            from relstorage.adapters.oracle.mover import OracleObjectMover as ObjectMover
            assert hasattr(ObjectMover, 'oracle_blob_chunk_maxsize')
            ObjectMover.oracle_blob_chunk_maxsize = 1024 * 1024 * 10
            large_blob_size = ObjectMover.oracle_blob_chunk_maxsize * 2
        else:
            large_blob_size = min(sys.maxsize, 1<<32)
        return large_blob_size


def test_suite():
    return OracleTestSuiteBuilder().test_suite()

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
