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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import unittest

from zope.interface import implementer

from ..interfaces import DriverNotAvailableError
from ..interfaces import NoDriversAvailableError
from ..interfaces import UnknownDriverError
from ..interfaces import IDBDriverOptions

from .. import drivers as abstract_drivers

@implementer(IDBDriverOptions)
class MockDrivers(object):
    database_type = 'mock'

    def __init__(self):
        self.driver_order = []

    def known_driver_factories(self):
        return self.driver_order

    def select_driver(self, driver_name=None):
        return abstract_drivers._select_driver_by_name(driver_name, self)

MockFactory = abstract_drivers._ClassDriverFactory

class TestAbstractDrivers(unittest.TestCase):

    def test_mock(self):
        from hamcrest import assert_that
        from nti.testing.matchers import validly_provides
        drivers = MockDrivers()
        assert_that(drivers, validly_provides(IDBDriverOptions))

    def test_select_auto_no_drivers(self):
        drivers = MockDrivers()
        with self.assertRaises(NoDriversAvailableError):
            drivers.select_driver()

    def test_select_auto(self):
        drivers = MockDrivers()
        drivers.driver_order.append(MockFactory(lambda: 42))
        d = drivers.select_driver()
        self.assertEqual(d, 42)

    def test_select_auto_not_available(self):
        class BadDriver(object):
            def __init__(self):
                raise DriverNotAvailableError('Bad')

        drivers = MockDrivers()
        drivers.driver_order.append(MockFactory(BadDriver))

        with self.assertRaises(NoDriversAvailableError) as exc:
            drivers.select_driver()

        self.assertIn(
            "Driver 'auto' is not available "
            "(reason={'BadDriver': \"DriverNotAvailableError: Driver 'Bad' is not available.\"}). "
            "Options: 'BadDriver' (Module: '<unknown>'; Available: False).",
            str(exc.exception))

    def test_select_unknown_name(self):
        drivers = MockDrivers()
        with self.assertRaises(UnknownDriverError):
            abstract_drivers._select_driver_by_name('DNE', drivers)

    def test_case_insensitive_names(self):
        drivers = MockDrivers()

        class Driver(object):
            __name__ = 'MixedCase'

        drivers.driver_order.append(MockFactory(Driver))

        self.assertIsInstance(drivers.select_driver('mixedcase'), Driver)
        self.assertIsInstance(drivers.select_driver('MIXEDCASE'), Driver)
        self.assertIsInstance(drivers.select_driver('MixedCase'), Driver)


class IDBDriverSupportsCriticalTestMixin(object):

    def _makeOne(self):
        raise NotImplementedError

    def test_enter_critical_phase(self):
        driver = self._makeOne()
        conn = driver.connect()

        self.assertFalse(conn.is_in_critical_phase())
        driver.enter_critical_phase_until_transaction_end(conn, None)
        self.assertTrue(driver.is_in_critical_phase(conn, None))
        # idempotent
        conn.enter_critical_phase_until_transaction_end()
        self.assertTrue(conn.is_in_critical_phase())

    def test_enter_critical_phase_exit_on_commit(self):
        driver = self._makeOne()
        conn = driver.connect()

        # exits on commit
        driver.enter_critical_phase_until_transaction_end(conn, None)
        conn.commit()
        self.assertFalse(driver.is_in_critical_phase(conn, None))

    def test_enter_critical_phase_exit_on_rollback(self):
        driver = self._makeOne()
        conn = driver.connect()

        # exits on commit
        driver.enter_critical_phase_until_transaction_end(conn, None)
        conn.rollback()
        self.assertFalse(driver.is_in_critical_phase(conn, None))
