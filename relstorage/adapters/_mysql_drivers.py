# -*- coding: utf-8 -*-
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
"""
MySQL IDBDriver implementations.
"""

from __future__ import print_function, absolute_import

import os
import sys

from zope.interface import moduleProvides
from zope.interface import implementer

from .interfaces import IDBDriver, IDBDriverOptions

from ._abstract_drivers import _standard_exceptions


database_type = 'mysql'
suggested_drivers = []
driver_map = {}
preferred_driver_name = None

moduleProvides(IDBDriverOptions)

try:
    import MySQLdb
except ImportError:
    pass
else:

    @implementer(IDBDriver)
    class MySQLdbDriver(object):
        __name__ = 'MySQLdb'
        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(MySQLdb)
        use_replica_exceptions = (MySQLdb.OperationalError,)
        Binary = staticmethod(MySQLdb.Binary)
        connect = staticmethod(MySQLdb.connect)
    del MySQLdb

    driver = MySQLdbDriver()
    driver_map[driver.__name__] = driver

    preferred_driver_name = driver.__name__
    del driver

try:
    import pymysql
except ImportError:
    pass
else:  # pragma: no cover
    import pymysql.err

    @implementer(IDBDriver)
    class PyMySQLDriver(object):
        __name__ = 'PyMySQL'

        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(pymysql)
        use_replica_exceptions = (pymysql.OperationalError,)
        # Under PyMySql through at least 0.6.6, closing an already closed
        # connection raises a plain pymysql.err.Error.
        # It can also raise a DatabaseError, and sometimes
        # an IOError doesn't get mapped to a type
        close_exceptions += (
            pymysql.err.Error,
            IOError,
            pymysql.err.DatabaseError
            )

        disconnected_exceptions += (
            IOError, # This one can escape mapping;
            # This one has only been seen as its subclass,
            # InternalError, as (0, 'Socket receive buffer full'),
            # which should probably be taken as disconnect
            pymysql.err.DatabaseError,
        )

        connect = staticmethod(pymysql.connect)
        Binary = staticmethod(pymysql.Binary)

    if getattr(sys, 'pypy_version_info', (9, 9, 9)) < (5, 3, 1):
        import pymysql.converters
        # PyPy up through 5.3.0 has a bug that raises spurious
        # MemoryErrors when run under PyMySQL >= 0.7.
        # (https://bitbucket.org/pypy/pypy/issues/2324/bytearray-replace-a-bc-raises-memoryerror)
        # (This is fixed in 5.3.1)
        # Patch around it.

        if hasattr(pymysql.converters, 'escape_string'):
            orig_escape_string = pymysql.converters.escape_string
            def escape_string(value, mapping=None):
                if isinstance(value, bytearray) and not value:
                    return value
                return orig_escape_string(value, mapping)
            pymysql.converters.escape_string = escape_string

        del pymysql.converters

    del pymysql.err
    del pymysql

    driver = PyMySQLDriver()
    driver_map[driver.__name__] = driver


    if hasattr(sys, 'pypy_version_info') or not preferred_driver_name:
        preferred_driver_name = driver.__name__
    del driver

if os.environ.get("RS_MY_DRIVER"): # pragma: no cover
    preferred_driver_name = os.environ["RS_MY_DRIVER"]
    print("Forcing MySQL driver to ", preferred_driver_name)
