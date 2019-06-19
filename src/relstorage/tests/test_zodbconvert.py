##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
from __future__ import absolute_import
from __future__ import print_function

import functools
import gc
import os
import tempfile
import unittest
from contextlib import contextmanager

import transaction
from zc.zlibstorage import ZlibStorage
from ZODB.DB import DB
from ZODB.FileStorage import FileStorage

from relstorage.zodbconvert import main


def skipIfZapNotSupportedByDest(func):
    @functools.wraps(func)
    def test(self):
        if not self.zap_supported_by_dest:
            raise unittest.SkipTest("zap_all not supported")
        func(self)
    return test

class AbstractZODBConvertBase(unittest.TestCase):
    cfgfile = None

    # Set to True in a subclass if the destination can be zapped
    zap_supported_by_dest = False

    def setUp(self):
        super(AbstractZODBConvertBase, self).setUp()
        self._to_close = []

    def tearDown(self):
        for i in self._to_close:
            i.close()
        self._to_close = []
        # XXX: On PyPy with psycopg2cffi, running these two tests will
        # result in a hang:
        # HPPostgreSQLDestZODBConvertTests.test_clear_empty_dest
        # HPPostgreSQLDestZODBConvertTests.test_clear_full_dest
        # test_clear_full_dest will hang in the zodbconvert call to
        # zap_all(), in the C code of the PG driver. Presumably some
        # connection with some lock got left open and was preventing
        # the TRUNCATE statements from taking out a lock. The same
        # tests do not hang with psycopg2cffi on C Python. Manually
        # running the gc (twice!) here fixes the issue. Note that this
        # only started when we wrapped the destination storage in
        # ZlibStorage (which copies methods into its own dict) so
        # there's something weird going on with the GC. Seen in PyPy
        # 2.5.0 and 5.3.
        gc.collect()
        gc.collect()
        super(AbstractZODBConvertBase, self).tearDown()

    def _closing(self, thing):
        self._to_close.append(thing)
        return thing

    def _create_src_storage(self):
        raise NotImplementedError()

    def _create_dest_storage(self):
        raise NotImplementedError()

    def _create_src_db(self):
        return self._closing(DB(self._closing(self._create_src_storage())))

    def _create_dest_db(self):
        return self._closing(DB(self._closing(self._create_dest_storage())))

    @contextmanager
    def __conn(self, name):
        db = getattr(self, '_create_' + name + '_db')()
        conn = db.open()
        try:
            yield conn
        finally:
            conn.close()
            db.close()

    def _src_conn(self):
        return self.__conn('src')

    def _dest_conn(self):
        return self.__conn('dest')

    def __write_value_for_key_in_db(self, val, key, db_conn_func):
        with db_conn_func() as conn:
            conn.root()[key] = val
            transaction.commit()

    def _write_value_for_key_in_src(self, x, key='x'):
        self.__write_value_for_key_in_db(x, key, self._src_conn)

    def _write_value_for_key_in_dest(self, x, key='x'):
        self.__write_value_for_key_in_db(x, key, self._dest_conn)

    def _check_value_of_key_in_dest(self, x, key='x'):
        with self._dest_conn() as conn2:
            db_x = conn2.root().get(key)
            self.assertEqual(db_x, x)

    def test_convert(self):
        self._write_value_for_key_in_src(10)
        main(['', self.cfgfile])
        self._check_value_of_key_in_dest(10)


    def test_dry_run(self):
        self._write_value_for_key_in_src(10)
        main(['', '--dry-run', self.cfgfile])
        self._check_value_of_key_in_dest(None)

    def test_incremental(self):
        self._write_value_for_key_in_src(10)
        main(['', self.cfgfile])
        self._check_value_of_key_in_dest(10)

        self._write_value_for_key_in_src("hi")
        main(['', '--incremental', self.cfgfile])
        self._check_value_of_key_in_dest("hi")

    def test_incremental_empty_src_dest(self):
        # Should work and not raise a POSKeyError
        main(['', '--incremental', self.cfgfile])
        self._check_value_of_key_in_dest(None)

    @skipIfZapNotSupportedByDest
    def test_clear_empty_dest(self):
        x = 10
        self._write_value_for_key_in_src(x)
        main(['', '--clear', self.cfgfile])
        self._check_value_of_key_in_dest(x)

    @skipIfZapNotSupportedByDest
    def test_clear_full_dest(self):
        self._write_value_for_key_in_dest(999)
        self._write_value_for_key_in_dest(666, key='y')
        self._write_value_for_key_in_dest(8675309, key='z')

        self._write_value_for_key_in_src(1, key='x')
        self._write_value_for_key_in_src(2, key='y')
        # omit z

        main(['', '--clear', self.cfgfile])

        self._check_value_of_key_in_dest(1, key='x')
        self._check_value_of_key_in_dest(2, key='y')
        self._check_value_of_key_in_dest(None, key='z')

    def test_no_overwrite(self):
        db = self._create_src_db() # create the root object
        db.close()
        db = self._create_dest_db() # create the root object
        db.close()
        self.assertRaises(SystemExit, main, ['', self.cfgfile])

class FSZODBConvertTests(AbstractZODBConvertBase):

    def setUp(self):
        super(FSZODBConvertTests, self).setUp()

        fd, self.srcfile = tempfile.mkstemp()
        os.close(fd)
        os.remove(self.srcfile)

        fd, self.destfile = tempfile.mkstemp()
        os.close(fd)
        os.remove(self.destfile)

        cfg = self._cfg_header() + self._cfg_source() + self._cfg_dest()
        self._write_cfg(cfg)

    def _cfg_header(self):
        return ""

    def _cfg_source(self):
        return """
        <filestorage source>
            path %s
        </filestorage>
        """ % self.srcfile

    def _cfg_dest(self):
        return """
        <filestorage destination>
            path %s
        </filestorage>
        """ % self.destfile

    def _write_cfg(self, cfg):
        fd, self.cfgfile = tempfile.mkstemp()
        os.write(fd, cfg.encode('ascii'))
        os.close(fd)

    def tearDown(self):
        if os.path.exists(self.destfile):
            os.remove(self.destfile)
        if os.path.exists(self.srcfile):
            os.remove(self.srcfile)
        if os.path.exists(self.cfgfile):
            os.remove(self.cfgfile)
        super(FSZODBConvertTests, self).tearDown()

    def _create_src_storage(self):
        return FileStorage(self.srcfile)

    def _create_dest_storage(self):
        return FileStorage(self.destfile)

    def test_storage_has_data(self):
        from relstorage.zodbconvert import storage_has_data
        src = FileStorage(self.srcfile, create=True)
        self.assertFalse(storage_has_data(src))
        db = DB(src)  # add the root object
        db.close()
        self.assertTrue(storage_has_data(src))

class ZlibWrappedZODBConvertTests(FSZODBConvertTests):

    def _cfg_header(self):
        return "%import zc.zlibstorage\n"

    def _cfg_source(self):
        return ("\n<zlibstorage source>"
                + super(ZlibWrappedZODBConvertTests, self)._cfg_source()
                + "</zlibstorage>")

    def _cfg_dest(self):
        return ("\n<zlibstorage destination>"
                + super(ZlibWrappedZODBConvertTests, self)._cfg_dest()
                + "</zlibstorage>")

    def _create_src_storage(self):
        return ZlibStorage(super(ZlibWrappedZODBConvertTests, self)._create_src_storage())

    def _create_dest_storage(self):
        return ZlibStorage(super(ZlibWrappedZODBConvertTests, self)._create_dest_storage())


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FSZODBConvertTests))
    suite.addTest(unittest.makeSuite(ZlibWrappedZODBConvertTests))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
