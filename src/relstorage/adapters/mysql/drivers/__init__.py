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
from __future__ import absolute_import
from __future__ import print_function

import sys

from zope.interface import moduleProvides

from ...interfaces import IDBDriverOptions
from .mysqlconnector import CMySQLConnectorDriver
from .mysqlconnector import PyMySQLConnectorDriver
from .mysqldb import MySQLdbDriver
from .pymysql import PyMySQLDriver
from .umysqldb import umysqldbDriver

database_type = 'mysql'

moduleProvides(IDBDriverOptions)


driver_map = {
    cls.__name__: cls
    for cls in (
        MySQLdbDriver,
        PyMySQLDriver,
        PyMySQLConnectorDriver,
        CMySQLConnectorDriver,
        umysqldbDriver
    )
}

if hasattr(sys, 'pypy_version_info'):
    driver_order = [
        PyMySQLDriver,
        PyMySQLConnectorDriver,
        MySQLdbDriver,
        CMySQLConnectorDriver,
        umysqldbDriver,
    ]
else:
    driver_order = [
        MySQLdbDriver,
        PyMySQLDriver,
        CMySQLConnectorDriver,
        PyMySQLConnectorDriver,
        umysqldbDriver,
    ]

def select_driver(driver_name=None):
    """
    Choose and return an IDBDriver
    """
    from ..._abstract_drivers import _select_driver_by_name
    return _select_driver_by_name(driver_name, sys.modules[__name__])

def known_driver_names():
    """
    Return an iterable of the potential driver names.

    The drivers may or may not be available.
    """
    return driver_map
