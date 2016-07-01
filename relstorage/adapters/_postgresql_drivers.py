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

        # extensions
        ISOLATION_LEVEL_READ_COMMITTED = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
        ISOLATION_LEVEL_SERIALIZABLE = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE


    driver = Psycopg2Driver()
    driver_map[driver.__name__] = driver

    preferred_driver_name = driver.__name__
    del driver
    del psycopg2

try:
    import psycopg2cffi
except ImportError:
    pass
else: # pragma: no cover

    @implementer(IDBDriver)
    class Psycopg2cffiDriver(object):
        __name__ = 'psycopg2cffi'
        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(psycopg2cffi)
        use_replica_exceptions = (psycopg2cffi.OperationalError,)
        Binary = psycopg2cffi.Binary
        connect = _create_connection(psycopg2cffi)

        # extensions
        ISOLATION_LEVEL_READ_COMMITTED = psycopg2cffi.extensions.ISOLATION_LEVEL_READ_COMMITTED
        ISOLATION_LEVEL_SERIALIZABLE = psycopg2cffi.extensions.ISOLATION_LEVEL_SERIALIZABLE


    driver = Psycopg2cffiDriver()
    driver_map[driver.__name__] = driver


    if hasattr(sys, 'pypy_version_info') or not preferred_driver_name:
        preferred_driver_name = driver.__name__
    del driver
    del psycopg2cffi

try:
    import pg8000
except ImportError:
    pass
else:
    import traceback

    class _ConnWrapper(object): # pragma: no cover
        def __init__(self, conn):
            self.__conn = conn
            self.__type = type(conn)
            self.__at = ''.join(traceback.format_stack())

        def __getattr__(self, name):
            return getattr(self.__conn, name)

        def __setattr__(self, name, value):
            if name in ('_ConnWrapper__conn', '_ConnWrapper__at', '_ConnWrapper__type'):
                object.__setattr__(self, name, value)
                return
            return setattr(self.__conn, name, value)

        def cursor(self):
            return _ConnWrapper(self.__conn.cursor())

        def __iter__(self):
            return self.__conn.__iter__()

        def close(self):
            if self.__conn is None:
                return
            try:
                self.__conn.close()
            finally:
                self.__conn = None

        def __del__(self):
            if self.__conn is not None:
                print("Failed to close", self, self.__type, " from:", self.__at, file=sys.stderr)
                print("Deleted at", ''.join(traceback.format_stack()))

    class _DoesNotRollbackIfNotInTransaction(pg8000.Connection):
        # pg8000, unlike psycopg2/cffi, will actually send a ROLLBACK
        # statement even if it's not in a transaction. This generates
        # warnings from the server "NOT IN TRANSACTION", which are annoying
        # (because we rolback() after every commit)
        # So this subclass doesn't do that.

        def rollback(self):
            if not self.in_transaction:
                return
            return super(_DoesNotRollbackIfNotInTransaction,self).rollback()

    @implementer(IDBDriver)
    class PG8000Driver(object):
        __name__ = 'pg8000'

        disconnected_exceptions, close_exceptions, lock_exceptions = _standard_exceptions(pg8000)
        # XXX TEsting
        disconnected_exceptions += (AttributeError,)
        use_replica_exceptions = (pg8000.OperationalError,)
        Binary = staticmethod(pg8000.Binary)
        _connect = staticmethod(pg8000.connect)

        _wrap = False

        def connect(self, dsn):
            # Parse the DSN into parts to pass as keywords.
            # TODO: We can do this is psycopg2 as well.
            kwds = {}
            parts = dsn.split(' ')
            for part in parts:
                key, value = part.split('=')
                value = value.strip("'\"")
                if key == 'dbname':
                    key = 'database'
                kwds[key] = value
            conn = self._connect(**kwds)
            assert conn.__class__ is _DoesNotRollbackIfNotInTransaction.__base__
            conn.__class__ = _DoesNotRollbackIfNotInTransaction
            return _ConnWrapper(conn) if self._wrap else conn

        ISOLATION_LEVEL_READ_COMMITTED = 'ISOLATION LEVEL READ COMMITTED'
        ISOLATION_LEVEL_SERIALIZABLE = 'ISOLATION LEVEL SERIALIZABLE'

    # XXX: global side-effect!
    pg8000.paramstyle = 'pyformat'

    driver = PG8000Driver()
    driver_map[driver.__name__] = driver

    if not preferred_driver_name:
        preferred_driver_name = driver.__name__

if os.environ.get("RS_PG_DRIVER"): # pragma: no cover
    preferred_driver_name = os.environ["RS_PG_DRIVER"]
    print("Forcing postgres driver to ", preferred_driver_name)
