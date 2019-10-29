# -*- coding: utf-8 -*-
"""
Tests for oidallocator.py
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.tests import TestCase

from ..oidallocator import AbstractRangedOIDAllocator


class MockOIDAllocator(AbstractRangedOIDAllocator):

    def _set_min_oid_from_range(self, cursor, n):
        raise NotImplementedError

    def new_oids(self, cursor):
        raise NotImplementedError

class TestOIDAllocator(TestCase):

    alloc = MockOIDAllocator()

    def test_range_around1(self, meth='_oid_range_around', convert=lambda x: x):
        # By default we get a list, no need to convert
        meth = getattr(self.alloc, meth) if not callable(meth) else meth
        oids = meth(1)
        oids = convert(oids)
        self.assertEqual(
            oids,
            [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1])

    def test_range_around1_list(self):
        # Range may or may not actually return a list, depending on the version,
        # so we convert
        from ..oidallocator import _oid_range_around_assume_list
        self.test_range_around1(_oid_range_around_assume_list, list)

    def test_range_around1_iterable(self):
        from ..oidallocator import _oid_range_around_iterable
        self.test_range_around1(_oid_range_around_iterable)
