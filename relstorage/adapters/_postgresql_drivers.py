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

        def connect_with_isolation(self, isolation, *args, **kwargs):
            conn = self.connect(*args, **kwargs)
            conn.set_isolation_level(isolation)
            return conn, conn.cursor()

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

        def connect_with_isolation(self, isolation, *args, **kwargs):
            conn = self.connect(*args, **kwargs)
            conn.set_isolation_level(isolation)
            return conn, conn.cursor()

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

    Binary = pg8000.Binary

    # Just enough lobject functionality for everything to work.
    # This is not threadsafe or useful outside of relstorage, it implements exactly
    # our requirements.

    class _WriteBlob(object):
        closed = False

        def __init__(self, conn):
            self._cursor = conn.cursor()
            self._offset = 0
            try:
                self._cursor.execute("SELECT lo_creat(-1)")
                row = self._cursor.fetchone()
                self.oid = row[0]
            except:
                self._cursor.close()
                raise

        def close(self):
            self._cursor.close()
            self.closed = True

        def write(self, data):
            self._cursor.execute("SELECT lo_put(%(oid)s, %(off)s, %(data)s)",
                                 {'oid': self.oid, 'off': self._offset, 'data': Binary(data)})
            self._offset += len(data)
            return len(data)

    class _UploadBlob(object):
        closed = False
        fetch_size = 1024 * 1024 * 9

        def __init__(self, conn, new_file):
            blob = _WriteBlob(conn)
            self.oid = blob.oid
            try:
                with open(new_file, 'rb') as f:
                    while 1:
                        data = f.read(self.fetch_size)
                        if not data:
                            break
                        blob.write(data)
            finally:
                blob.close()

        def close(self):
            self.closed = True

    class _ReadBlob(object):
        closed = False
        fetch_size = 1024 * 1024 * 9
        def __init__(self, conn, oid):
            self._cursor = conn.cursor()
            self.oid = oid
            self.offset = 0

        def export(self, filename):
            with open(filename, 'wb') as f:
                while 1:
                    data = self.read(self.fetch_size)
                    if not data:
                        break
                    f.write(data)
            self.close()

        def read(self, size):
            self._cursor.execute("SELECT lo_get(%(oid)s, %(off)s, %(cnt)s)",
                                 {'oid': self.oid, 'off': self.offset, 'cnt': size})
            row = self._cursor.fetchone()
            data = row[0]
            self.offset += len(data)
            return data

        def close(self):
            self._cursor.close()
            self.closed = True

    class _Connection(pg8000.Connection):
        def rollback(self):
            # pg8000, unlike psycopg2/cffi, will actually send a ROLLBACK
            # statement even if it's not in a transaction. This generates
            # warnings from the server "NOT IN TRANSACTION", which are annoying
            # (because we rolback() after every commit)
            # So this subclass doesn't do that.
            # This will be fixed in 1.10.7, see https://github.com/mfenniak/pg8000/pull/114

            # This is net perfectly correct because we don't hold the lock. But we
            # don't expect to be used by multiple threads.
            if not self.in_transaction:
                return
            return super(_Connection, self).rollback()

        def lobject(self, oid=0, mode='', new_oid=0, new_file=None):
            if oid == 0 and new_oid == 0 and mode == 'wb':
                if new_file:
                    # Upload the whole file right now.
                    return _UploadBlob(self, new_file)
                return _WriteBlob(self)
            if oid != 0 and mode == 'rb':
                return _ReadBlob(self, oid)
            raise AssertionError("Unsupported params", dict(locals()))

    from ._abstract_drivers import _ConnWrapper

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
            # We don't do this psycopg2 because a real DSN supports more options than
            # we do and we don't want to limit it.
            kwds = {}
            parts = dsn.split(' ')
            for part in parts:
                key, value = part.split('=')
                value = value.strip("'\"")
                if key == 'dbname':
                    key = 'database'
                kwds[key] = value
            conn = self._connect(**kwds)
            assert conn.__class__ is _Connection.__base__
            conn.__class__ = _Connection
            return _ConnWrapper(conn) if self._wrap else conn

        # Extensions

        ISOLATION_LEVEL_READ_COMMITTED = 'ISOLATION LEVEL READ COMMITTED'
        ISOLATION_LEVEL_SERIALIZABLE = 'ISOLATION LEVEL SERIALIZABLE'

        def connect_with_isolation(self, isolation, dsn):
            conn = self.connect(dsn)
            cursor = conn.cursor()
            cursor.execute('SET TRANSACTION %s' % isolation)
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION %s" % isolation)
            conn.commit()
            return conn, cursor


    # XXX: global side-effect!
    pg8000.paramstyle = 'pyformat'

    driver = PG8000Driver()
    driver_map[driver.__name__] = driver

    if not preferred_driver_name:
        preferred_driver_name = driver.__name__

if os.environ.get("RS_PG_DRIVER"): # pragma: no cover
    preferred_driver_name = os.environ["RS_PG_DRIVER"]
    print("Forcing postgres driver to ", preferred_driver_name)
