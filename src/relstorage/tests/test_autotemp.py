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

class AutoTemporaryFileTests(unittest.TestCase):

    def getClass(self):
        from relstorage.autotemp import AutoTemporaryFile
        return AutoTemporaryFile

    def test_defaults(self):
        t = self.getClass()()
        self.assertEqual(t._threshold, 10*1024*1024)

    def test_write_and_read_limited(self):
        t = self.getClass()()
        t.write(b'abc')
        self.assertEqual(t.tell(), 3)
        t.seek(0)
        self.assertEqual(t.tell(), 0)
        self.assertEqual(t.read(2), b'ab')
        self.assertEqual(t.tell(), 2)

    def test_write_and_read_unlimited(self):
        t = self.getClass()()
        t.write(b'abc')
        t.seek(0)
        self.assertEqual(t.read(), b'abc')

    def test_convert_to_temporary_file(self):
        t = self.getClass()(threshold=4)
        try:
            self.assertEqual(t._threshold, 4)
            t.write(b'abc')
            self.assertEqual(t._threshold, 4)
            t.write(b'd')
            self.assertEqual(t._threshold, 0)
            t.write(b'e')
            t.seek(0)
            self.assertEqual(t.read(), b'abcde')
        finally:
            t.close()

    def test_overwrite_during_conversion(self):
        t = self.getClass()(threshold=4)
        try:
            t.write(b'abc')
            self.assertEqual(t._threshold, 4)
            t.seek(1)
            t.write(b'0')
            self.assertEqual(t._threshold, 4)
            t.write(b'1')
            self.assertEqual(t._threshold, 4)
            t.write(b'23')
            self.assertEqual(t._threshold, 0)
            t.seek(0)
            self.assertEqual(t.read(), b'a0123')
        finally:
            t.close()

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AutoTemporaryFileTests))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
