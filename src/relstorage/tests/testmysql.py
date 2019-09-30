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

    # The MySQL schema adapter uses DROP TABLE
    # and then CREATE TABLE to zap when ``zap_all(slow=True)``.
    # This is *much* faster than ``DELETE FROM`` on large
    # databases (since we can't use truncate.). But for small databases,
    # it adds lots of extra overhead to re-create those tables all the
    # time, and ``DELETE FROM`` is the way to go.
    zap_slow = True

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

        # Problematic values due to rounding
        # of minutes due to seconds
        for now, gmtime in (
                (1565774811.9655108,
                 (2019, 8, 14, 9, 26, 51, 2, 226, 0)),
                (1565767799.607957,
                 (2019, 8, 14, 7, 29, 59, 2, 226, 0)),
                (1565775177.915336,
                 (2019, 8, 14, 9, 32, 57, 2, 226, 0)),
                (1565775299.106127,
                 (2019, 8, 14, 9, 34, 59, 2, 226, 0)),
                (1565775479.180209,
                 (2019, 8, 14, 9, 37, 59, 2, 226, 0)),
        ):
            self.assertEqual(time.gmtime(now), gmtime)
            self.test_current_time(now)

    def test_current_time(self, now=None):
        from persistent.timestamp import TimeStamp
        from relstorage._util import int64_to_8bytes
        if now is None:
            now = time.time()
        storage = self._storage
        ts_now = timestamp_at_unixtime(now)

        expected_tid_int = bytes8_to_int64(ts_now.raw())
        __traceback_info__ = now, now % 60.0, time.gmtime(now), ts_now, expected_tid_int

        cursor = storage._load_connection.cursor

        cursor.execute('CALL make_tid_for_epoch(%s, @tid)', (now,))
        cursor.execute('SELECT @tid')
        tid, = cursor.fetchall()[0]

        tid_as_timetime = TimeStamp(int64_to_8bytes(tid)).timeTime()
        __traceback_info__ += (tid_as_timetime - ts_now.timeTime(),)

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

    def _make_check_class_HistoryFreeRelStorageTests(self, bases, name, klass_dict=None):
        bases = (GenericMySQLTestsMixin, ) + bases

        klass_dict = {}

        return self._default_make_check_class(bases, name, klass_dict=klass_dict)

    # pylint:disable=line-too-long
    def _make_check_class_HistoryPreservingRelStorageTests(self, bases, name, klass_dict=None):
        return self._make_check_class_HistoryFreeRelStorageTests(bases, name, klass_dict)


class GenericMySQLTestsMixin(object):

    @skipOnCI("Travis MySQL goes away error 2006")
    def check16MObject(self):
        # NOTE: If your mySQL goes away, check the server's value for
        # `max_allowed_packet`, you probably need to increase it.
        # JAM uses 64M.
        # http://dev.mysql.com/doc/refman/5.7/en/packet-too-large.html
        super(GenericMySQLTestsMixin, self).check16MObject()

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

    def checkIsolationLevels(self):

        def assert_storage(storage):
            load_cur = storage._load_connection.cursor
            store_cur = storage._store_connection.cursor
            version_detector = storage._adapter.version_detector
            if not version_detector.supports_transaction_isolation(load_cur):
                raise unittest.SkipTest("Needs MySQL better than %s" % (
                    version_detector.get_version(load_cur)
                ))

            for cur, ex_iso, ex_ro, ex_timeout in (
                    # Timeout for load is mysql default.
                    [load_cur, 'REPEATABLE-READ', True, 50],
                    [store_cur, 'READ-COMMITTED', False, self.DEFAULT_COMMIT_LOCK_TIMEOUT],
            ):
                cur.execute("""
                SELECT @@transaction_isolation,
                       @@transaction_read_only,
                       @@innodb_lock_wait_timeout
                """)
                row, = cur.fetchall()
                iso, ro, timeout = row
                __traceback_info__ = row
                iso = iso.decode('ascii') if not isinstance(iso, str) else iso
                self.assertEqual(iso, ex_iso)
                self.assertEqual(ro, ex_ro)
                self.assertEqual(timeout, ex_timeout)

        # By default
        assert_storage(self._storage)

        # In a new instance, and after we do a transaction with it.
        from ZODB.DB import DB
        import transaction

        db = self._closing(DB(self._storage))
        conn = self._closing(db.open())
        assert_storage(conn._storage)

        conn.root()['obj'] = 1
        transaction.commit()

        assert_storage(conn._storage)


def test_suite():
    return MySQLTestSuiteBuilder().test_suite()


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger("zc.lockfile").setLevel(logging.CRITICAL)
    unittest.main(defaultTest="test_suite")
