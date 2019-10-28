# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

"""Integration tests for sqlite support."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import tempfile
import os.path
import unittest

from relstorage.adapters.sqlite.adapter import Sqlite3Adapter
from relstorage.options import Options

from relstorage.tests.util import AbstractTestSuiteBuilder
from relstorage.tests.util import RUNNING_ON_TRAVIS
from relstorage.tests.util import RUNNING_ON_APPVEYOR
from relstorage._compat import PYPY
from relstorage._compat import PY3

class Sqlite3AdapterMixin(object):

    def __get_db_name(self):
        if self.keep_history:
            db = self.base_dbname
        else:
            db = self.base_dbname + '_hf'
        return db

    def __get_adapter_options(self, dbname=None):
        dbname = dbname or self.__get_db_name()
        assert isinstance(dbname, str), (dbname, type(dbname))
        # Our layers tend to change the temporary directory,
        # and then destroy it when the layer is torn down.
        # So our files don't persist. This can show a different set of
        # bugs than re-using an existing schema that gets zapped.
        data_dir = os.path.join(
            #"/tmp",
            tempfile.gettempdir(),
            dbname)

        return {
            'data_dir': data_dir,
            'pragmas': {}
        }

    def make_adapter(self, options, db=None):
        return Sqlite3Adapter(
            options=options,
            **self.__get_adapter_options(db)
        )

    def get_adapter_class(self):
        return Sqlite3Adapter

    def get_adapter_zconfig(self):
        options = self.__get_adapter_options()
        options['data-dir'] = options['data_dir']
        del options['data_dir']
        del options['pragmas']
        options['driver'] = self.driver_name
        formatted_options = '\n'.join(
            '     %s %s' % (k, v)
            for k, v in options.items()
        )

        return u"""
        <sqlite3>
            %s
            gevent_yield_interval 42
            <pragmas>
               journal_mode memory
               cache_size 8mb
            </pragmas>
        </sqlite3>
        """ % (formatted_options)

    def verify_adapter_from_zconfig(self, adapter):
        self.assertEqual(adapter.connmanager.path,
                         os.path.join(
                             self.__get_adapter_options()['data_dir'], 'main.sqlite3'))
        if 'gevent' in adapter.connmanager.driver.__name__:
            self.assertEqual(adapter.connmanager.driver.yield_to_gevent_instruction_interval,
                             42)
        conn, _ = adapter.connmanager.open()
        try:
            cur = conn.execute('pragma cache_size')
            size, = cur.fetchall()[0]
            self.assertEqual(size, 8388608)
            # But we weren't allowed to change journal_mode
            cur = conn.execute('pragma journal_mode')
            journal, = cur.fetchall()[0]
            self.assertEqual(journal, 'wal')
        finally:
            conn.close()


class Sqlite3TestSuiteBuilder(AbstractTestSuiteBuilder):

    __name__ = 'Sqlite3'
    RAISED_EXCEPTIONS = Exception

    def __init__(self):
        from relstorage.adapters.sqlite import drivers
        super(Sqlite3TestSuiteBuilder, self).__init__(
            drivers,
            Sqlite3AdapterMixin,
            extra_test_classes=()
        )

    def _compute_large_blob_size(self, use_small_blobs):
        return Options().blob_chunk_size

    __BASE_SKIPPED_TESTS = (
        # These were both seen on Travis with PyPy3.6 7.1.1, sqlite 3.11.
        # I can't reproduce locally.
        ('checkAutoReconnect', PYPY and PY3 and RUNNING_ON_TRAVIS,
         "Somehow still winds up closed"),
        ('checkAutoReconnectOnSync', PYPY and PY3 and RUNNING_ON_TRAVIS,
         "Somehow still winds up closed"),
    )

    def __add_skips(self, klass, extra_skips=()):
        for mname, skip, message in self.__BASE_SKIPPED_TESTS + extra_skips:
            meth = getattr(klass, mname)
            meth = unittest.skipIf(skip, message)(meth)
            setattr(klass, mname, meth)

    def _make_check_class_HistoryPreservingRelStorageTests(self, bases,
                                                           name, klass_dict=None):
        klass = self._default_make_check_class(bases, name, klass_dict=klass_dict)
        skips = (
            # For some reason this fails to get the undo log. Something
            # to do with the way we manage the connection? Seen on Python 3.7
            # with sqlite 3.28
            ('checkPackUnlinkedFromRoot', RUNNING_ON_APPVEYOR,
             "Fails to get undo log"),
            # Ditto, but on Python 2.7 with sqlite 3.14
            ('checkTransactionalUndoAfterPackWithObjectUnlinkFromRoot', RUNNING_ON_APPVEYOR,
             "Fails to get undo log"),
        )
        self.__add_skips(klass, skips)
        return klass

    def _make_check_class_HistoryFreeRelStorageTests(self, bases,
                                                     name, klass_dict=None):
        klass = self._default_make_check_class(bases, name, klass_dict=klass_dict)
        self.__add_skips(klass)
        return klass

def test_suite():
    return Sqlite3TestSuiteBuilder().test_suite()
