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

from relstorage.adapters.oracle import OracleAdapter

from .util import AbstractTestSuiteBuilder

class OracleAdapterMixin(object):

    keep_history = False
    base_dbname = None
    driver_name = None

    def make_adapter(self, options, db=None):
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

    def get_adapter_class(self):
        return OracleAdapter

    def __get_adapter_zconfig_dsn(self):
        dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
        return dsn

    def get_adapter_zconfig(self):
        if self.keep_history:
            dbname = self.base_dbname
        else:
            dbname = self.base_dbname + '_hf'
        return u"""
        <oracle>
            driver %s
            user %s
            password relstoragetest
            dsn %s
        </oracle>
        """ % (
            self.driver_name,
            dbname,
            self.__get_adapter_zconfig_dsn()
        )

    def verify_adapter_from_zconfig(self, adapter):
        if self.keep_history:
            dbname = self.base_dbname
        else:
            dbname = self.base_dbname + '_hf'

        self.assertEqual(adapter._user, dbname)
        self.assertEqual(adapter._password, 'relstoragetest')
        self.assertEqual(adapter._dsn, self.__get_adapter_zconfig_dsn())
        self.assertEqual(adapter._twophase, False)

    def get_adapter_zconfig_replica_conf(self):
        import tempfile
        dsn = self.__get_adapter_zconfig_dsn()
        fd, replica_conf = tempfile.mkstemp('.conf', 'rstest_oracle_replica')
        self.addCleanup(os.remove, replica_conf)
        os.write(fd, dsn.encode("ascii"))
        os.close(fd)
        return replica_conf


class OracleTestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'Oracle'

    def __init__(self):
        from relstorage.adapters.oracle import drivers
        super(OracleTestSuiteBuilder, self).__init__(
            drivers,
            OracleAdapterMixin,
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
