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

from __future__ import absolute_import
from __future__ import print_function

import warnings

from zope.interface import implementer


from ..drivers import AbstractModuleDriver
from ..drivers import implement_db_driver_options
from ..interfaces import IDBDriver
from .dialect import OracleDialect

database_type = 'oracle'

__all__ = [
    'cx_OracleDriver',
]

@implementer(IDBDriver)
class cx_OracleDriver(AbstractModuleDriver):
    __name__ = 'cx_Oracle'
    MODULE_NAME = __name__
    dialect = OracleDialect()

    def __init__(self):
        super(cx_OracleDriver, self).__init__()

        cx_Oracle = self.driver_module

        self.disconnected_exceptions += (cx_Oracle.DatabaseError,)
        self.close_exceptions += (cx_Oracle.DatabaseError,)

        # Extensions
        self.DatabaseError = cx_Oracle.DatabaseError
        self.NUMBER = cx_Oracle.NUMBER
        self.BLOB = cx_Oracle.BLOB
        self.CLOB = cx_Oracle.CLOB
        self.LOB = cx_Oracle.LOB
        self.LONG_BINARY = cx_Oracle.LONG_BINARY
        self.LONG_STRING = cx_Oracle.LONG_STRING
        self.BINARY = cx_Oracle.BINARY
        self.STRING = cx_Oracle.STRING
        self.version = cx_Oracle.version

    def exception_is_deadlock(self, exc):
        warnings.warn("exception_is_deadlock() unimplemented for cx_Oracle")


implement_db_driver_options(
    __name__,
    '.drivers'
)
