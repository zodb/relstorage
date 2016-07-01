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
PostgreSQL IDBDriver implementations.
"""

from __future__ import print_function, absolute_import

import sys
import os

from zope.interface import moduleProvides
from zope.interface import implementer

from .interfaces import IDBDriver, IDBDriverOptions
from ._abstract_drivers import _standard_exceptions


database_type = 'postgresql'
suggested_drivers = []
driver_map = {}
preferred_driver_name = None

moduleProvides(IDBDriverOptions)

def _create_connection(mod):
    class Psycopg2Connection(mod.extensions.connection):
        # The replica attribute holds the name of the replica this
        # connection is bound to.
        __slots__ = ('replica',)

    return Psycopg2Connection
try:
    import psycopg2
except ImportError:
    pass
else:

    @implementer(IDBDriver)
    class Psycopg2Driver(object):
        __name__ = 'psycopg2'
        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(psycopg2)
        use_replica_exceptions = (psycopg2.OperationalError,)
        Binary = psycopg2.Binary
        connect = _create_connection(psycopg2)

        extensions = psycopg2.extensions

    driver = Psycopg2Driver()
    driver_map[driver.__name__] = driver

    preferred_driver_name = driver.__name__
    del driver
    del psycopg2

try:
    import psycopg2cffi
except ImportError:
    pass
else:

    @implementer(IDBDriver)
    class Psycopg2cffiDriver(object):
        __name__ = 'psycopg2cffi'
        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(psycopg2cffi)
        use_replica_exceptions = (psycopg2cffi.OperationalError,)
        Binary = psycopg2cffi.Binary
        connect = _create_connection(psycopg2cffi)

        extensions = psycopg2cffi.extensions


    driver = Psycopg2cffiDriver()
    driver_map[driver.__name__] = driver


    if hasattr(sys, 'pypy_version_info') or not preferred_driver_name:
        preferred_driver_name = driver.__name__
    del driver
    del psycopg2cffi

if os.environ.get("RS_PG_DRIVER"):
    preferred_driver_name = os.environ["RS_PG_DRIVER"]
    print("Forcing postgres driver to ", preferred_driver_name)
