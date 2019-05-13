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

from relstorage.adapters.mysql import drivers

from ..interfaces import DriverNotAvailableError
from ..interfaces import NoDriversAvailableError
from ..interfaces import UnknownDriverError

from .. import _abstract_drivers as abstract_drivers

class TestAbstractDrivers(unittest.TestCase):

    def test_select_auto(self):
        try:
            d = abstract_drivers._select_driver_by_name(None, drivers)
        except NoDriversAvailableError: # pylint: no cover
            # Theoretically possible
            pass
        else:
            self.assertIsNotNone(d)

    def test_select_auto_not_available(self):

        class BadDriver(object):
            def __init__(self):
                raise DriverNotAvailableError('Bad')

        class Options(object):
            driver_order = [BadDriver]

        with self.assertRaises(NoDriversAvailableError):
            abstract_drivers._select_driver_by_name(None, Options)


    def test_select_unknown_name(self):
        with self.assertRaises(UnknownDriverError):
            abstract_drivers._select_driver_by_name('DNE', drivers)
