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

from relstorage.adapters.postgresql import PostgreSQLAdapter

from .util import AbstractTestSuiteBuilder

# pylint:disable=no-member


base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')


class UsePostgreSQLAdapter(object):

    def make_adapter(self, options, db=None):
        if db is None:
            if self.keep_history:
                db = self.base_dbname
            else:
                db = self.base_dbname + '_hf'
        return PostgreSQLAdapter(
            dsn='dbname=%s user=relstoragetest password=relstoragetest' % db,
            options=options,
        )


class ZConfigTests(object):

    driver_name = None # Override
    base_dbname = None

    def checkConfigureViaZConfig(self):
        replica_conf = os.path.join(os.path.dirname(__file__), 'replicas.conf')
        if self.keep_history:
            dbname = self.base_dbname
        else:
            dbname = self.base_dbname + '_hf'
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


class PostgreSQLTestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'PostgreSQL'

    def __init__(self):
        from relstorage.adapters.postgresql import drivers
        super(PostgreSQLTestSuiteBuilder, self).__init__(
            drivers,
            UsePostgreSQLAdapter,
            _PgSQLCfgMixin,
        )

    def _compute_large_blob_size(self, use_small_blobs):
        if use_small_blobs:
            # Avoid creating 2GB blobs to be friendly to neighbors
            # and to run fast (2GB blobs take about 4 minutes on Travis
            # CI as-of June 2016)
            # XXX: This is dirty.
            from relstorage.adapters.postgresql.mover import PostgreSQLObjectMover as ObjectMover
            assert hasattr(ObjectMover, 'postgresql_blob_chunk_maxsize')
            ObjectMover.postgresql_blob_chunk_maxsize = 1024 * 1024 * 10
            large_blob_size = ObjectMover.postgresql_blob_chunk_maxsize * 2
        else:
            large_blob_size = 1 << 31
        return large_blob_size


def test_suite():
    return PostgreSQLTestSuiteBuilder().test_suite()

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
