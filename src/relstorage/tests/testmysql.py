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
