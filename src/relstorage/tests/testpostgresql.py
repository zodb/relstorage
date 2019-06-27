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
import unittest

from ZODB.tests import StorageTestBase

from relstorage.adapters.postgresql import PostgreSQLAdapter

from .util import AbstractTestSuiteBuilder
from . import StorageCreatingMixin
from . import TestCase

class PostgreSQLAdapterMixin(object):

    def make_adapter(self, options, db=None):
        return PostgreSQLAdapter(
            dsn=self.__get_adapter_zconfig_dsn(db),
            options=options,
        )

    def get_adapter_class(self):
        return PostgreSQLAdapter

    def __get_adapter_zconfig_dsn(self, dbname=None):
        if dbname is None:
            if self.keep_history:
                dbname = self.base_dbname
            else:
                dbname = self.base_dbname + '_hf'
        dsn = (
            "dbname='%s' user='relstoragetest' password='relstoragetest'"
            % dbname
        )
        # psycopg2cffi can have a unix socket path hardcoded in it,
        # and that path may not be right
        if 'cffi' in self.driver_name.lower():
            dsn += " host='127.0.0.1'"
        return dsn

    def get_adapter_zconfig(self):
        return u"""
        <postgresql>
            driver %s
            dsn %s
        </postgresql>
        """ % (
            self.driver_name,
            self.__get_adapter_zconfig_dsn()
        )

    def verify_adapter_from_zconfig(self, adapter):
        self.assertEqual(adapter._dsn, self.__get_adapter_zconfig_dsn())

class TestBlobMerge(PostgreSQLAdapterMixin,
                    StorageCreatingMixin,
                    TestCase,
                    StorageTestBase.StorageTestBase):
    # pylint:disable=too-many-ancestors

    def test_merge_blobs_on_open(self):
        from ZODB.DB import DB
        from ZODB.blob import Blob
        import transaction
        storage = self._closing(self.make_storage(
            blob_dir='blobs', shared_blob_dir=False))
        db = self._closing(DB(storage))
        conn = db.open()

        blob = Blob()
        base_chunk = b"This is my base blob."
        with blob.open('w') as f:
            f.write(base_chunk)

        conn.root().blob = blob
        transaction.commit()

        # Insert some extra chunks. Get them big to be sure we loop
        # properly
        second_chunk = b'second chunk' * 800
        cursor = conn._storage._store_cursor
        cursor.execute("""
        INSERT INTO blob_chunk (zoid, chunk_num, tid, chunk)
        SELECT zoid, 1, tid, lo_from_bytea(0, %s)
        FROM blob_chunk WHERE chunk_num = 0;
        """, (second_chunk,))
        third_chunk = b'third chunk' * 900
        cursor.execute("""
        INSERT INTO blob_chunk (zoid, chunk_num, tid, chunk)
        SELECT zoid, 2, tid, lo_from_bytea(0, %s)
        FROM blob_chunk WHERE chunk_num = 0;
        """, (third_chunk,))

        cursor.execute('SELECT COUNT(*) FROM blob_chunk')
        self.assertEqual(3, cursor.fetchone()[0])
        cursor.connection.commit()
        # Now open again and find everything put together.
        # But we need to use a new blob dir, because
        # we changed data behind its back.
        conn.close()
        db.close()

        storage = self._closing(self.make_storage(blob_dir='blobs2',
                                                  shared_blob_dir=False,
                                                  zap=False))
        db = self._closing(DB(storage))
        conn = db.open()

        blob = conn.root().blob
        with blob.open('r') as f:
            data = f.read()

        cursor = conn._storage._load_cursor
        cursor.execute('SELECT COUNT(*) FROM blob_chunk')
        self.assertEqual(1, cursor.fetchone()[0])

        self.assertEqual(data, base_chunk + second_chunk + third_chunk)
        conn.close()
        db.close()

# Timing shows that we spend 6.9s opening database connections to a
# local PostgreSQL 11 server when using Python 3.7 and psycopg2 2.8
# during a total test run of 2:27. I had thought that maybe connection
# pooling would speed the test run up, but that doesn't seem to be the
# case.

class PostgreSQLTestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'PostgreSQL'

    def __init__(self):
        from relstorage.adapters.postgresql import drivers
        super(PostgreSQLTestSuiteBuilder, self).__init__(
            drivers,
            PostgreSQLAdapterMixin,
            extra_test_classes=(TestBlobMerge,)
        )

    def _compute_large_blob_size(self, use_small_blobs):
        if use_small_blobs:
            # Avoid creating 2GB blobs to be friendly to neighbors
            # and to run fast (2GB blobs take about 4 minutes on Travis
            # CI as-of June 2016).
            # RS 3.0 no longer needs to chunk.
            large_blob_size = 20 * 1024 * 1024
        else:
            # Something bigger than 2GB (signed 32 bit int)
            large_blob_size = (1 << 31) + 200 * 1024 * 1024
        return large_blob_size


def test_suite():
    return PostgreSQLTestSuiteBuilder().test_suite()


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
