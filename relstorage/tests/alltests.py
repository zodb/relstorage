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

import logging
import sys
import unittest
import importlib

_include_db = True

def make_suite():
    suite = unittest.TestSuite()
    test_modules = [
        'relstorage.adapters.tests.test_batch',
        'relstorage.adapters.tests.test_connmanager',
        'relstorage.adapters.tests.test_replica',
        'relstorage.tests.test_autotemp',
        'relstorage.tests.test_blobhelper',
        'relstorage.tests.test_cache',
        'relstorage.tests.test_cache_stats',
        'relstorage.tests.test_treemark',
        'relstorage.tests.test_zodbconvert',
        'relstorage.tests.test_zodbpack',
        'relstorage.tests.test_zodburi',
    ]
    db_test_modules = [
        'relstorage.tests.testpostgresql',
        'relstorage.tests.testmysql',
        'relstorage.tests.testoracle',
    ]

    if _include_db:
        test_modules += db_test_modules

    for mod_name in test_modules:
        mod = importlib.import_module(mod_name)
        suite.addTest(mod.test_suite())

    return suite

if __name__ == '__main__':
    if '--no-db' in sys.argv:
        _include_db = False
        sys.argv.remove('--no-db')
    logging.basicConfig(level=logging.CRITICAL,
                        format='%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d:%(process)d][%(threadName)s] %(message)s')
    # We get constant errors about failing to lock a blob file,
    # which really bloats the CI logs, so turn those off.
    logging.getLogger('zc.lockfile').setLevel(logging.CRITICAL)
    unittest.main(defaultTest='make_suite')
