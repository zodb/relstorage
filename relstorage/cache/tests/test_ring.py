##############################################################################
#
# Copyright (c) 2015 Zope Foundation and Contributors.
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

from .. import ring

#pylint: disable=R0904,W0212,E1101

class DummyPersistent(object):
    _p_oid = None

    __next_oid = 0

    @classmethod
    def _next_oid(cls):
        cls.__next_oid += 1
        return cls.__next_oid

    def __init__(self, oid=None):
        if oid is None:
            self._p_oid = self._next_oid()

    def __repr__(self):
        return "<Dummy %r>" % self._p_oid

class _Ring_Base(object):

    def _getTargetClass(self):
        """Return the type of the ring to test"""
        raise NotImplementedError()

    def _makeOne(self):
        return self._getTargetClass()()

    def test_empty_len(self):
        self.assertEqual(0, len(self._makeOne()))

    def test_empty_contains(self):
        r = self._makeOne()
        self.assertFalse(DummyPersistent() in r)

    def test_empty_iter(self):
        self.assertEqual([], list(self._makeOne()))

    def test_add_one_len1(self):
        r = self._makeOne()
        p = DummyPersistent()
        r.add(p)
        self.assertEqual(1, len(r))

    def test_add_one_contains(self):
        r = self._makeOne()
        p = DummyPersistent()
        r.add(p)
        self.assertTrue(p in r)

    def test_delete_one_len0(self):
        r = self._makeOne()
        p = DummyPersistent()
        r.add(p)
        r.delete(p)
        self.assertEqual(0, len(r))

    def test_delete_one_multiple(self):
        r = self._makeOne()
        p = DummyPersistent()
        r.add(p)
        r.delete(p)
        self.assertEqual(0, len(r))
        self.assertFalse(p in r)

        self.assertRaises(KeyError, r.delete, p)
        self.assertEqual(0, len(r))
        self.assertFalse(p in r)

    def test_delete_from_wrong_ring(self):
        r1 = self._makeOne()
        r2 = self._makeOne()
        p1 = DummyPersistent()
        p2 = DummyPersistent()

        r1.add(p1)
        r2.add(p2)

        self.assertRaises(KeyError, r2.delete, p1)

        self.assertEqual(1, len(r1))
        self.assertEqual(1, len(r2))

        self.assertEqual([p1], list(r1))
        self.assertEqual([p2], list(r2))

    def test_move_to_head(self):
        r = self._makeOne()
        p1 = DummyPersistent()
        p2 = DummyPersistent()
        p3 = DummyPersistent()

        r.add(p1)
        r.add(p2)
        r.add(p3)

        self.assertEqual([p1, p2, p3], list(r))
        self.assertEqual(3, len(r))

        r.move_to_head(p1)
        self.assertEqual([p2, p3, p1], list(r))

        r.move_to_head(p3)
        self.assertEqual([p2, p1, p3], list(r))

        r.move_to_head(p3)
        self.assertEqual([p2, p1, p3], list(r))

    def test_delete_all(self):
        r = self._makeOne()
        p1 = DummyPersistent()
        p2 = DummyPersistent()
        p3 = DummyPersistent()

        r.add(p1)
        r.add(p2)
        r.add(p3)
        self.assertEqual([p1, p2, p3], list(r))

        r.delete_all([(0, p1), (2, p3)])
        self.assertEqual([p2], list(r))
        self.assertEqual(1, len(r))

class DequeRingTests(unittest.TestCase, _Ring_Base):

    def _getTargetClass(self):
        return ring._DequeRing

_add_to_suite = [DequeRingTests]

if ring._CFFIRing:
    class CFFIRingTests(unittest.TestCase, _Ring_Base):

        def _getTargetClass(self):
            return ring._CFFIRing

    _add_to_suite.append(CFFIRingTests)

def test_suite():
    return unittest.TestSuite([unittest.makeSuite(x) for x in _add_to_suite])

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
