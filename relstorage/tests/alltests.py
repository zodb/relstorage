##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""Combines all the tests"""
from __future__ import absolute_import, print_function
import unittest
import logging

def make_suite():
    suite = unittest.TestSuite()
    modules = [
        'test_autotemp',
        'test_blobhelper',
        'test_cache',
        'test_treemark',
        'test_zodbconvert',
        'test_zodbpack',
        'test_zodburi',
        'testpostgresql',
        'testmysql',
        'testoracle',
    ]
    for mod_name in modules:
        mod = __import__(mod_name, globals())
        print(mod)
        suite.addTest(mod.test_suite())

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    # We get constant errors about failing to lock a blob file,
    # which really bloats the CI logs, so turn those off.
    logging.getLogger('zc.lockfile').setLevel(logging.CRITICAL)
    unittest.main(defaultTest='make_suite')
