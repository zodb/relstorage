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

from .. import interfaces

class TestExceptions(unittest.TestCase):

    def test_DriverNotAvailableError_str(self, klass=interfaces.DriverNotAvailableError):
        from relstorage.adapters.mysql import drivers
        name = 'auto'

        ex = klass(name, drivers)

        for s in str(ex), repr(ex):
            self.assertIn(
                "%s: Driver 'auto' is not available. Options: " % (
                    klass.__name__,
                ),
                s
            )
            self.assertIn(
                " 'MySQLdb' (Module: 'MySQLdb'; Available:",
                s
            )

    def test_UnknownDriverError_str(self):
        self.test_DriverNotAvailableError_str(interfaces.UnknownDriverError)

    def test_NoDriversAvailableError_str(self):
        self.test_DriverNotAvailableError_str(interfaces.NoDriversAvailableError)
