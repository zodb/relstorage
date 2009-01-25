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
"""Tests of relstorage.adapters.oracle"""

import logging
import os
import re
import unittest

import reltestbase
from relstorage.adapters.oracle import OracleAdapter


def getOracleParams():
    # Expect an environment variable that specifies how to connect.
    # A more secure way of providing the password would be nice,
    # if anyone wants to tackle it.
    connect_string = os.environ.get('ORACLE_CONNECT')
    if not connect_string:
        raise KeyError("An ORACLE_CONNECT environment variable is "
            "required to run OracleTests")
    mo = re.match('([^/]+)/([^@]+)@(.*)', connect_string)
    if mo is None:
        raise KeyError("The ORACLE_CONNECT environment variable must "
            "be of the form 'user/password@dsn'")
    user, password, dsn = mo.groups()
    return user, password, dsn


class UseOracleAdapter:
    def make_adapter(self):
        user, password, dsn = getOracleParams()
        return OracleAdapter(user, password, dsn)

class OracleTests(UseOracleAdapter, reltestbase.RelStorageTests):
    pass

class OracleToFile(UseOracleAdapter, reltestbase.ToFileStorage):
    pass

class FileToOracle(UseOracleAdapter, reltestbase.FromFileStorage):
    pass


def test_suite():
    suite = unittest.TestSuite()
    for klass in [OracleTests, OracleToFile, FileToOracle]:
        suite.addTest(unittest.makeSuite(klass, "check"))
    return suite

if __name__=='__main__':
    logging.basicConfig()
    unittest.main(defaultTest="test_suite")
