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
PyMySQL IDBDriver implementations.
"""
from __future__ import print_function, absolute_import

from zope.interface import implementer

from relstorage.adapters.interfaces import IDBDriver
from relstorage.adapters._abstract_drivers import AbstractModuleDriver


@implementer(IDBDriver)
class PyMySQLDriver(AbstractModuleDriver):
    __name__ = 'PyMySQL'

    def __init__(self):
        super(PyMySQLDriver, self).__init__()

        from pymysql import err as pymysql_err

        # Under PyMySql through at least 0.6.6, closing an already closed
        # connection raises a plain pymysql.err.Error.
        # It can also raise a DatabaseError, and sometimes
        # an IOError doesn't get mapped to a type
        self.close_exceptions += (
            pymysql_err.Error,
            IOError,
            pymysql_err.DatabaseError
        )

        self.disconnected_exceptions += (
            IOError, # This one can escape mapping;
            # This one has only been seen as its subclass,
            # InternalError, as (0, 'Socket receive buffer full'),
            # which should probably be taken as disconnect
            pymysql_err.DatabaseError,
        )

    def get_driver_module(self):
        import pymysql
        return pymysql
