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
Oracle IDBDriver implementations.
"""

from __future__ import print_function, absolute_import


from zope.interface import moduleProvides
from zope.interface import implementer

from ..interfaces import IDBDriver, IDBDriverOptions
from .._abstract_drivers import _standard_exceptions


database_type = 'oracle'
suggested_drivers = []
driver_map = {}
preferred_driver_name = None

moduleProvides(IDBDriverOptions)

try:
    import cx_Oracle
except ImportError:
    pass
else:  # pragma: no cover

    @implementer(IDBDriver)
    class cx_OracleDriver(object): # noqa
        __name__ = 'cx_Oracle'
        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(cx_Oracle)
        disconnected_exceptions += (cx_Oracle.DatabaseError,)
        close_exceptions += (cx_Oracle.DatabaseError,)

        use_replica_exceptions = (cx_Oracle.OperationalError,)
        Binary = staticmethod(cx_Oracle.Binary)
        connect = staticmethod(cx_Oracle.connect)

        # Extensions
        DatabaseError = cx_Oracle.DatabaseError
        NUMBER = cx_Oracle.NUMBER
        BLOB = cx_Oracle.BLOB
        LOB = cx_Oracle.LOB
        LONG_BINARY = cx_Oracle.LONG_BINARY
        BINARY = cx_Oracle.BINARY
        STRING = cx_Oracle.STRING
        version = cx_Oracle.version

    driver = cx_OracleDriver()
    driver_map[driver.__name__] = driver

    preferred_driver_name = driver.__name__
    del driver
    del cx_Oracle
