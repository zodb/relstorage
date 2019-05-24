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
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage.adapters.interfaces import IDBDriver

from . import AbstractMySQLDriver

__all__ = [
    'PyMySQLDriver',
]

@implementer(IDBDriver)
class PyMySQLDriver(AbstractMySQLDriver):
    __name__ = 'PyMySQL'
    MODULE_NAME = 'pymysql'
    PRIORITY = 2
    PRIORITY_PYPY = 1
    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = True

    def __init__(self):
        super(PyMySQLDriver, self).__init__()

        pymysql_err = self.driver_module

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
