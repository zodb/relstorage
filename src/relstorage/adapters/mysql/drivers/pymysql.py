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

from relstorage._util import Lazy
from relstorage.adapters.interfaces import IDBDriver

from . import AbstractMySQLDriver

__all__ = [
    'PyMySQLDriver',
]

# The default conversion looses precision in floating point values,
# truncating them at 5 digits and using an exponent, which also loses
# precision. All other drivers send the full representation. See
# pymysql.converters:escape_float. The version here is based on
# what mysqlclient has.
def escape_float(value, _mapping):
    s = repr(value)
    if s in ('inf', 'nan'):
        raise TypeError("%s can not be used with MySQL" % s)
    if 'e' not in s:
        s += 'e0'
    return s

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

        self._conversions = self.driver_module.converters.conversions.copy()
        self._conversions[float] = escape_float

    def connect(self, *args, **kwargs):
        kwargs['conv'] = self._conversions
        # For bytes and bytearrays, put the "_binary" character set introducer
        # in front. If we haven't set the ``connection_charset`` to binary --- and we can't
        # because that breaks JSON types --- then this is needed for sending state data
        # to avoid warnings about invalid character sequences.
        kwargs['binary_prefix'] = True
        return AbstractMySQLDriver.connect(self, *args, **kwargs)

    @Lazy
    def _server_side_cursor(self):
        # This appears like it might only read one row at a time, which is probably not
        # great. But the usual solution, falling back to fetchmany(),
        # doesn't do any good: fetchmany is implemented in terms of fetchone().
        from pymysql.cursors import SSCursor # pylint:disable=no-name-in-module,import-error

        return SSCursor
