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
_only_mysql = False
_only_pgsql = False
_only_oracle = False

def make_suite():
    suite = unittest.TestSuite()
    test_modules = [
        'relstorage.adapters.tests.test_batch',
        'relstorage.adapters.tests.test_connmanager',
        'relstorage.adapters.tests.test_replica',
        'relstorage.cache.tests.test_cache',
        'relstorage.cache.tests.test_cache_stats',
        'relstorage.tests.test_autotemp',
        'relstorage.tests.test_blobhelper',
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
    elif _only_mysql:
        test_modules.extend(x for x in db_test_modules if 'mysql' in x)
    elif _only_pgsql:
        test_modules.extend(x for x in db_test_modules if 'postgresql' in x)
    elif _only_oracle:
        test_modules.extend(x for x in db_test_modules if 'oracle' in x)

    for mod_name in test_modules:
        mod = importlib.import_module(mod_name)
        suite.addTest(mod.test_suite())

    return suite

if __name__ == '__main__':
    if '--no-db' in sys.argv:
        _include_db = False
        sys.argv.remove('--no-db')
    if '--only-mysql' in sys.argv:
        _include_db = False
        _only_mysql = True
        sys.argv.remove('--only-mysql')
    if '--only-pgsql' in sys.argv:
        _include_db = False
        _only_pgsql = True
        sys.argv.remove('--only-pgsql')
    if '--only-oracle' in sys.argv:
        _include_db = False
        _only_oracle = True
        sys.argv.remove('--only-oracle')

    logging.basicConfig(level=logging.CRITICAL,
                        format='%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d:%(process)d][%(threadName)s] %(message)s')
    # We get constant errors about failing to lock a blob file,
    # which really bloats the CI logs, so turn those off.
    logging.getLogger('zc.lockfile').setLevel(logging.CRITICAL)
    unittest.main(defaultTest='make_suite')
