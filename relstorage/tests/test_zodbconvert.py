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
from __future__ import print_function
import unittest

class ZODBConvertTests(unittest.TestCase):

    def setUp(self):
        import os
        import tempfile

        fd, self.srcfile = tempfile.mkstemp()
        os.close(fd)
        os.remove(self.srcfile)

        fd, self.destfile = tempfile.mkstemp()
        os.close(fd)
        os.remove(self.destfile)

        cfg = """
        <filestorage source>
            path %s
        </filestorage>
        <filestorage destination>
            path %s
        </filestorage>
        """ % (self.srcfile, self.destfile)

        fd, self.cfgfile = tempfile.mkstemp()
        os.write(fd, cfg)
        os.close(fd)

    def tearDown(self):
        import os
        if os.path.exists(self.destfile):
            os.remove(self.destfile)
        if os.path.exists(self.srcfile):
            os.remove(self.srcfile)
        if os.path.exists(self.cfgfile):
            os.remove(self.cfgfile)

    def test_storage_has_data(self):
        from ZODB.DB import DB
        from relstorage.zodbconvert import storage_has_data
        from ZODB.FileStorage import FileStorage
        src = FileStorage(self.srcfile, create=True)
        self.assertFalse(storage_has_data(src))
        db = DB(src)  # add the root object
        db.close()
        self.assertTrue(storage_has_data(src))

    def test_convert(self):
        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage
        from relstorage.zodbconvert import main
        import transaction

        src = FileStorage(self.srcfile)
        db = DB(src)
        conn = db.open()
        conn.root()['x'] = 10
        transaction.commit()
        conn.close()
        db.close()

        main(['', self.cfgfile])

        dest = FileStorage(self.destfile)
        db2 = DB(dest)
        conn2 = db2.open()
        self.assertEqual(conn2.root().get('x'), 10)
        conn2.close()
        db2.close()

    def test_dry_run(self):
        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage
        from relstorage.zodbconvert import main
        import transaction

        src = FileStorage(self.srcfile)
        db = DB(src)
        conn = db.open()
        conn.root()['x'] = 10
        transaction.commit()
        conn.close()
        db.close()

        main(['', '--dry-run', self.cfgfile])

        dest = FileStorage(self.destfile)
        db2 = DB(dest)
        conn2 = db2.open()
        self.assertEqual(conn2.root().get('x'), None)
        conn2.close()
        db2.close()

    def test_incremental(self):
        from ZODB.DB import DB
        from ZODB.utils import u64, readable_tid_repr
        from ZODB.FileStorage import FileStorage
        from relstorage.zodbconvert import main
        import transaction

        def write_value_for_x_in_src(x):
            src = FileStorage(self.srcfile)
            db = DB(src)
            conn = db.open()
            conn.root()['x'] = x
            transaction.commit()
            conn.close()
            db.close()

        def check_value_of_x_in_dest(x):
            dest = FileStorage(self.destfile)
            db2 = DB(dest)
            conn2 = db2.open()
            db_x = conn2.root().get('x')
            conn2.close()
            db2.close()
            self.assertEqual(db_x, x)

        x = 10
        write_value_for_x_in_src(x)
        main(['', self.cfgfile])
        check_value_of_x_in_dest(x)

        x = "hi"
        write_value_for_x_in_src(x)
        main(['', '--incremental', self.cfgfile])
        check_value_of_x_in_dest(x)


    def test_no_overwrite(self):
        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage
        from relstorage.zodbconvert import main
        from relstorage.zodbconvert import storage_has_data
        import transaction

        src = FileStorage(self.srcfile)
        db = DB(src)  # create the root object
        db.close()

        dest = FileStorage(self.destfile)
        db = DB(dest)  # create the root object
        db.close()

        self.assertRaises(SystemExit, main, ['', self.cfgfile])

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBConvertTests))
    return suite
