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

from hamcrest import assert_that
from nti.testing.matchers import validly_provides
from zope.schema.interfaces import IObject

from relstorage.options import Options
from relstorage.tests import TestCase

from .. import interfaces

class AdapterTestBase(TestCase):

    driver = 'auto'

    def _makeOne(self, options):
        raise NotImplementedError

    def test_implements(self):
        options = Options(driver=self.driver)
        adapter = self._makeOne(options)
        assert_that(adapter, validly_provides(interfaces.IRelStorageAdapter))

        # pylint:disable=no-value-for-parameter
        for attr_name, field in interfaces.IRelStorageAdapter.namesAndDescriptions():
            if IObject.providedBy(field):
                attr_iface = field.schema
                attr_val = getattr(adapter, attr_name)
                # Look for functions/methods in its dict and check for __wrapped__ attributes;
                # these come from perfmetrics @metricmethod and that breaks validation.
                for k in dir(type(attr_val)):
                    if k not in attr_iface:
                        continue
                    v = getattr(attr_val, k)
                    if callable(v) and hasattr(v, '__wrapped__'):
                        orig = v.__wrapped__
                        if hasattr(orig, '__get__'):
                            # Must be Python 3, we got the raw unbound function, we need to bind it
                            # and add the self parameter.
                            orig = orig.__get__(type(attr_val), attr_val)
                        try:
                            setattr(attr_val, k, orig)
                        except AttributeError:
                            # Must be slotted.
                            continue
                assert_that(attr_val, validly_provides(attr_iface))


def test_suite():
    # Nothing to see here.
    import unittest
    return unittest.TestSuite()
