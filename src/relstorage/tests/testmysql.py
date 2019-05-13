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
import unittest

from relstorage.adapters.mysql import MySQLAdapter
from relstorage.options import Options


from .util import skipOnCI
from .util import AbstractTestSuiteBuilder


class MySQLAdapterMixin(object):

    def make_adapter(self, options, db=None):
        if db is None:
            if self.keep_history:
                db = self.base_dbname
            else:
                db = self.base_dbname + '_hf'
        return MySQLAdapter(
            options=options,
            db=db,
            user='relstoragetest',
            passwd='relstoragetest',
        )

    def get_adapter_class(self):
        return MySQLAdapter

    def get_adapter_zconfig(self):
        if self.keep_history:
            dbname = self.base_dbname
        else:
            dbname = self.base_dbname + '_hf'
        return u"""
        <mysql>
            driver %s
            db %s
            user relstoragetest
            passwd relstoragetest
        </mysql>
        """ % (
            self.driver_name,
            dbname,
        )

    def verify_adapter_from_zconfig(self, adapter):
        if self.keep_history:
            dbname = self.base_dbname
        else:
            dbname = self.base_dbname + '_hf'

        self.assertEqual(adapter._params, {
            'passwd': 'relstoragetest',
            'db': dbname,
            'user': 'relstoragetest',
        })


class TestOIDAllocator(unittest.TestCase):

    def test_bad_rowid(self):
        from relstorage.adapters.mysql.oidallocator import MySQLOIDAllocator
        class Cursor(object):
            def execute(self, s):
                pass
            lastrowid = None

        oids = MySQLOIDAllocator(KeyError)
        self.assertRaises(KeyError, oids.new_oids, Cursor())


class MySQLTestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'MySQL'

    def __init__(self):
        from relstorage.adapters.mysql import drivers
        super(MySQLTestSuiteBuilder, self).__init__(
            drivers,
            MySQLAdapterMixin
        )

    def _compute_large_blob_size(self, use_small_blobs):
        # MySQL is limited to the blob_chunk_size as there is no
        # native blob streaming support. (Note: this depends on
        # the max_allowed_packet size on the server as well as the driver; both
        # values default to 1MB. But umysqldb needs 1.3MB max_allowed_packet size
        # to send multiple 1MB chunks. So keep it small.)
        return Options().blob_chunk_size

    def _make_check_class_HistoryFreeRelStorageTests(self, base, _):
        class Tests(MySQLAdapterMixin,
                    base):
            @skipOnCI("Travis MySQL goes away error 2006")
            def check16MObject(self):
                # NOTE: If your mySQL goes away, check the server's value for
                # `max_allowed_packet`, you probably need to increase it.
                # JAM uses 64M.
                # http://dev.mysql.com/doc/refman/5.7/en/packet-too-large.html

                # This fails if the driver is umysqldb.
                try:
                    base.check16MObject(self)
                except Exception as e:
                    if e.args == (0, 'Query too big'):
                        raise unittest.SkipTest("Fails with umysqldb")
                    raise
        return Tests

    # pylint:disable=line-too-long
    _make_check_class_HistoryPreservingRelStorageTests = _make_check_class_HistoryFreeRelStorageTests

    def test_suite(self):
        suite = super(MySQLTestSuiteBuilder, self).test_suite()
        suite.addTest(unittest.makeSuite(TestOIDAllocator))
        return suite

def test_suite():
    return MySQLTestSuiteBuilder().test_suite()


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
