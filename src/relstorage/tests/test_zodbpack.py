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

import unittest


class ZODBPackScriptTests(unittest.TestCase):

    def setUp(self):
        import os
        import tempfile

        fd, self.db_fn = tempfile.mkstemp()
        os.close(fd)

        cfg = """
        <filestorage>
            path %s
        </filestorage>
        """ % self.db_fn

        fd, self.cfg_fn = tempfile.mkstemp()
        os.write(fd, cfg.encode('ascii'))
        os.close(fd)

    def tearDown(self):
        import os
        if os.path.exists(self.db_fn):
            os.remove(self.db_fn)
        if os.path.exists(self.cfg_fn):
            os.remove(self.cfg_fn)

    def test_pack_with_0_day(self):
        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage
        from ZODB.POSException import POSKeyError
        import time
        import transaction
        from relstorage.zodbpack import main

        storage = FileStorage(self.db_fn, create=True)
        db = DB(storage)
        conn = db.open()
        conn.root()['x'] = 1
        transaction.commit()
        oid = b'\0' * 8
        state, serial = storage.load(oid, '')
        time.sleep(0.1)
        conn.root()['x'] = 2
        transaction.commit()
        conn.close()
        self.assertEqual(state, storage.loadSerial(oid, serial))
        db.close()
        storage = None

        main(['', '--days=0', self.cfg_fn])

        # packing should have removed the old state.
        storage = FileStorage(self.db_fn)
        self.assertRaises(POSKeyError, storage.loadSerial, oid, serial)
        storage.close()

    def test_pack_defaults(self):
        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage
        import time
        import transaction
        from relstorage.zodbpack import main

        storage = FileStorage(self.db_fn, create=True)
        db = DB(storage)
        conn = db.open()
        conn.root()['x'] = 1
        transaction.commit()
        oid = b'\0' * 8
        state, serial = storage.load(oid, '')
        time.sleep(0.1)
        conn.root()['x'] = 2
        transaction.commit()
        conn.close()
        self.assertEqual(state, storage.loadSerial(oid, serial))
        db.close()
        storage = None

        main(['', '--days=1', self.cfg_fn])

        # packing should not have removed the old state.
        storage = FileStorage(self.db_fn)
        self.assertEqual(state, storage.loadSerial(oid, serial))
        storage.close()


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBPackScriptTests))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
