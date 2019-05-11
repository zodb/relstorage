# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
PostgreSQL IDBDriverOptions implementation.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

from zope.interface import moduleProvides

from ...interfaces import IDBDriverOptions
from .pg8000 import PG8000Driver
from .psycopg2 import Psycopg2Driver
from .psycopg2cffi import Psycopg2cffiDriver

database_type = 'postgresql'
driver_map = {}

moduleProvides(IDBDriverOptions)


driver_map = {
    cls.__name__: cls
    for cls in (PG8000Driver, Psycopg2Driver, Psycopg2cffiDriver)
}

if hasattr(sys, 'pypy_version_info'):
    driver_order = [
        Psycopg2cffiDriver,
        PG8000Driver,
        Psycopg2Driver,
    ]
else:
    driver_order = [
        Psycopg2Driver,
        Psycopg2cffiDriver,
        PG8000Driver,
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
