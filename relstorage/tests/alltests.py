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
    test_modules = [
        'relstorage.adapters.tests.test_batch',
        'relstorage.adapters.tests.test_connmanager',
        'relstorage.adapters.tests.test_replica',
        'relstorage.tests.test_autotemp',
        'relstorage.tests.test_blobhelper',
        'relstorage.tests.test_cache',
        'relstorage.tests.test_treemark',
        'relstorage.tests.test_zodbconvert',
        'relstorage.tests.test_zodbpack',
        'relstorage.tests.test_zodburi',
        'relstorage.tests.testpostgresql',
        'relstorage.tests.testmysql',
        'relstorage.tests.testoracle',
    ]

    for mod_name in test_modules:
        mod = __import__(mod_name, globals(),
                         fromlist=['chicken'] if '.' in mod_name else [])
        suite.addTest(mod.test_suite())

    return suite

if __name__ == '__main__':
    logging.basicConfig()
    # We get constant errors about failing to lock a blob file,
    # which really bloats the CI logs, so turn those off.
    logging.getLogger('zc.lockfile').setLevel(logging.CRITICAL)
    unittest.main(defaultTest='make_suite')
