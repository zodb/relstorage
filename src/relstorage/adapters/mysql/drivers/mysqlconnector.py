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
MySQL Connector/Python IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage._compat import PY2
from relstorage.adapters._abstract_drivers import AbstractModuleDriver
from relstorage.adapters.interfaces import IDBDriver

__all__ = [
    'PyMySQLConnectorDriver',
    'CMySQLConnectorDriver',
]

_base_name = 'MySQL Connector/Python'

@implementer(IDBDriver)
class PyMySQLConnectorDriver(AbstractModuleDriver):
    # See https://github.com/zodb/relstorage/issues/155
    __name__ = 'Py ' + _base_name

    MODULE_NAME = 'mysql.connector'
    PRIORITY = 4
    PRIORITY_PYPY = 2

    USE_PURE = True


    def __init__(self):
        super(PyMySQLConnectorDriver, self).__init__()
        del self.connect

    def connect(self, *args, **kwargs): # pylint:disable=method-hidden
        # It defaults to the (slower) pure-python version
        # NOTE: The C implementation doesn't support the prepared
        # operations.
        # NOTE: The C implementation returns bytes when the Py implementation
        # returns bytearray under Py2

        kwargs['use_pure'] = self.USE_PURE
        if PY2:
            # The docs say that strings are returned as unicode by default
            # an all platforms, but this is inconsistent. We need str anyway.
            kwargs['use_unicode'] = False
        con = self.driver_module.connect(*args, **kwargs)

        return con

    def set_autocommit(self, conn, value):
        # This implementation uses a property instead of a method.
        conn.autocommit = value

    def cursor(self, conn):
        # By default, the cursor won't buffer, so we don't know
        # how many rows there are. That's fine and within the DB-API spec.
        # The Python implementation is much faster if we don't ask it to.
        # The C connection doesn't accept the 'prepared' keyword.
        # You can't have both a buffered and prepared cursor,
        # but the prepared cursor doesn't gain us anything anyway.

        cursor = conn.cursor()
        return cursor

class CMySQLConnectorDriver(PyMySQLConnectorDriver):
    __name__ = 'C ' + _base_name

    AVAILABLE_ON_PYPY = False
    PRIORITY = 3
    PRIORITY_PYPY = 4

    USE_PURE = False

    def get_driver_module(self):
        mod = super(CMySQLConnectorDriver, self).get_driver_module()
        if not mod.HAVE_CEXT:
            raise ImportError("No C extension")
        return mod
