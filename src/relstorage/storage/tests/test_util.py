# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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

"""
Tests for storage implementation utilities.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ZODB.POSException import ReadOnlyError

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.tests import TestCase

from ..interfaces import IStaleAware

from .. import util

class Foo(object):

    @util.stale_aware
    def method(self, a1, a2=42):
        "Nothing"

    @util.stale_aware
    @util.writable_storage_method
    def writable(self, a1, a2=42):
        return a1, a2


class TestStaleAware(TestCase):

    def test_provides(self):

        method = Foo().method

        assert_that(method, validly_provides(IStaleAware))

    def test_stale(self):

        method = Foo().method

        stale = method.stale(KeyError) # pylint:disable=no-member
        assert_that(stale, validly_provides(IStaleAware))

        with self.assertRaises(KeyError):
            stale()

        stale_again = stale.stale(ValueError)
        self.assertIs(stale_again, stale)

        not_stale = stale.no_longer_stale()
        self.assertIs(not_stale, method)
        not_stale2 = not_stale.no_longer_stale()
        self.assertIs(not_stale2, method)

    def test_writable_under_stale(self):

        class Storage(object):

            read_only = False
            _read_only_error = ReadOnlyError

            def isReadOnly(self):
                return self.read_only

        storage = Storage()
        util.copy_storage_methods(storage, Foo())
        # pylint:disable=no-member
        self.assertEqual(
            (42, 13),
            storage.writable(42, a2=13))

        w = storage.writable.stale(KeyError)
        with self.assertRaises(KeyError):
            w(42)


        storage = Storage()
        storage.read_only = True

        util.copy_storage_methods(storage, Foo())

        with self.assertRaises(ReadOnlyError):
            storage.writable(42, a=36)

        # read-only trumps stale.
        self.assertFalse(hasattr(storage.writable, 'stale'))
