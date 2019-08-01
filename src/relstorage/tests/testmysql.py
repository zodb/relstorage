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
import time
import unittest

from ZODB.utils import u64 as bytes8_to_int64
from ZODB.tests import StorageTestBase

from relstorage.adapters.mysql import MySQLAdapter
from relstorage.options import Options
from relstorage._util import timestamp_at_unixtime

from . import StorageCreatingMixin
from . import TestCase

from .util import skipOnCI
from .util import AbstractTestSuiteBuilder
from .util import DEFAULT_DATABASE_SERVER_HOST


class MySQLAdapterMixin(object):

    def __get_db_name(self):
        if self.keep_history:
            db = self.base_dbname
        else:
            db = self.base_dbname + '_hf'
        return db

    def __get_adapter_options(self, dbname=None):
        dbname = dbname or self.__get_db_name()
        assert isinstance(dbname, str), (dbname, type(dbname))
        return {
            'db': dbname,
            'user': 'relstoragetest',
            'passwd': 'relstoragetest',
            'host': DEFAULT_DATABASE_SERVER_HOST,
        }

    def make_adapter(self, options, db=None):
        return MySQLAdapter(
            options=options,
            **self.__get_adapter_options(db)
        )

    def get_adapter_class(self):
        return MySQLAdapter

    def get_adapter_zconfig(self):
        options = self.__get_adapter_options()
        options['driver'] = self.driver_name
        formatted_options = '\n'.join(
            '     %s %s' % (k, v)
            for k, v in options.items()
        )

        return u"""
        <mysql>
            %s
        </mysql>
        """ % (formatted_options)

    def verify_adapter_from_zconfig(self, adapter):
        self.assertEqual(adapter._params, self.__get_adapter_options())


class TestGenerateTID(MySQLAdapterMixin,
                      StorageCreatingMixin,
                      TestCase,
                      StorageTestBase.StorageTestBase):
    # pylint:disable=too-many-ancestors

    def setUp(self):
        super(TestGenerateTID, self).setUp()
        self._storage = self._closing(self.make_storage())

    def test_extract_parts(self):
        unix_time = 1564063129.1277142

        query = """
        SELECT EXTRACT(year FROM ts) as YEAR,
               EXTRACT(month FROM ts) AS month,
               EXTRACT(day FROM ts) AS day,
               EXTRACT(hour FROM ts) AS hour,
               EXTRACT(minute FROM ts) AS minute,
               %s MOD 60 AS seconds
        FROM (
            SELECT FROM_UNIXTIME(%s) + 0.0 AS ts
        ) t
        """
        cursor = self._storage._load_connection.cursor
        cursor.execute(query, (unix_time, unix_time))
        year, month, day, hour, minute, seconds = cursor.fetchone()
        self.assertEqual(year, 2019)
        self.assertEqual(month, 7)
        self.assertEqual(day, 25)
        self.assertEqual(hour, 13) # If this is not 13, the time_zone is incorrect
        self.assertEqual(minute, 58)
        self.assertEqual(
            round(float(seconds), 6),
            49.127714)

    def test_known_time(self):
        now = 1564054182.277615
        gmtime = (2019, 7, 25, 11, 29, 42, 3, 206, 0)

        self.assertEqual(
            time.gmtime(now),
            gmtime
        )

        ts_now = timestamp_at_unixtime(now)
        self.assertEqual(
            ts_now.raw(),
            b'\x03\xd1Oq\xb4bn\x00'
        )

        self.test_current_time(now)

    def test_current_time(self, now=None):
        if now is None:
            now = time.time()
        storage = self._storage
        ts_now = timestamp_at_unixtime(now)
        __traceback_info__ = now, ts_now, ts_now.raw()

        expected_tid_int = bytes8_to_int64(ts_now.raw())

        cursor = storage._load_connection.cursor

        cursor.execute('CALL make_tid_for_epoch(%s, @tid)', (now,))
        cursor.execute('SELECT @tid')
        tid, = cursor.fetchall()[0]

        self.assertEqual(
            tid,
            expected_tid_int
        )



class MySQLTestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'MySQL'

    def __init__(self):
        from relstorage.adapters.mysql import drivers
        super(MySQLTestSuiteBuilder, self).__init__(
            drivers,
            MySQLAdapterMixin,
            extra_test_classes=(TestGenerateTID,)
        )

    def _compute_large_blob_size(self, use_small_blobs):
        # MySQL is limited to the blob_chunk_size as there is no
        # native blob streaming support. (Note: this depends on the
        # max_allowed_packet size on the server as well as the driver;
        # both values default to 1MB. So keep it small.)
        return Options().blob_chunk_size

    def _make_check_class_HistoryFreeRelStorageTests(self, bases, name):
        @skipOnCI("Travis MySQL goes away error 2006")
        def check16MObject(self):
            # NOTE: If your mySQL goes away, check the server's value for
            # `max_allowed_packet`, you probably need to increase it.
            # JAM uses 64M.
            # http://dev.mysql.com/doc/refman/5.7/en/packet-too-large.html
            bases[0].check16MObject(self)

        def checkMyISAMTablesProduceErrorWhenNoCreate(self):
            from ZODB.POSException import StorageError
            def cb(_conn, cursor):
                cursor.execute('ALTER TABLE new_oid ENGINE=MyISAM;')
            self._storage._adapter.connmanager.open_and_call(cb)
            # Now open a new storage that's not allowed to create
            with self.assertRaisesRegex(
                    StorageError,
                    'MyISAM is no longer supported.*new_oid'
            ):
                self.open(create_schema=False)

        def checkMyISAMTablesAutoMigrate(self):
            # Verify we have a broken state.
            self.checkMyISAMTablesProduceErrorWhenNoCreate()
            # Now a storage that can alter a table will do so.
            storage = self.open()
            storage.close()
            storage = self.open(create_schema=False)
            storage.close()

        klass_dict = {
            f.__name__: f
            for f in (
                check16MObject,
                checkMyISAMTablesProduceErrorWhenNoCreate,
                checkMyISAMTablesAutoMigrate,
            )
        }

        return self._default_make_check_class(bases, name, klass_dict=klass_dict)

    # pylint:disable=line-too-long
    _make_check_class_HistoryPreservingRelStorageTests = _make_check_class_HistoryFreeRelStorageTests


def test_suite():
    return MySQLTestSuiteBuilder().test_suite()


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
