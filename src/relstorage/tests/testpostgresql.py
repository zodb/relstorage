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

from relstorage.adapters.postgresql import PostgreSQLAdapter

from .util import AbstractTestSuiteBuilder


class PostgreSQLAdapterMixin(object):

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

    def get_adapter_class(self):
        return PostgreSQLAdapter

    def __get_adapter_zconfig_dsn(self):
        if self.keep_history:
            dbname = self.base_dbname
        else:
            dbname = self.base_dbname + '_hf'
        dsn = (
            "dbname='%s' user='relstoragetest' password='relstoragetest'"
            % dbname
        )
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
