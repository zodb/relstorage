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
from __future__ import absolute_import
from __future__ import print_function

import doctest
import re
import unittest
import shutil

import ZODB.tests.util
import zope.testing.renormalizing
import zope.testing.setupstack

def test_suite():
    suite = unittest.TestSuite()
    try:
        __import__('ZEO')
    except ImportError:
        class NoTest(unittest.TestCase):
            @unittest.skip("ZEO not installed")
            def test_cache_trace_analysis(self):
                "Does nothing"
        suite.addTest(unittest.makeSuite(NoTest))
    else:
        # setupstack doesn't ignore problems when files can't be
        # found

        zope.testing.setupstack.rmtree = lambda p: shutil.rmtree(p, True)
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
