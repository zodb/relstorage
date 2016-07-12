##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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
from __future__ import print_function, absolute_import


import doctest

import unittest
import re

import ZODB.tests.util
import zope.testing.setupstack
import zope.testing.renormalizing


# Mostly taken from ZEO.test.test_cache and modified.

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(
        doctest.DocFileSuite(
            'cache_trace_analysis.rst',
            setUp=zope.testing.setupstack.setUpDirectory,
            tearDown=zope.testing.setupstack.tearDown,
            checker=ZODB.tests.util.checker + \
                zope.testing.renormalizing.RENormalizing([
                    (re.compile(r'31\.3%'), '31.2%'),
                ]),
            )
        )
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
