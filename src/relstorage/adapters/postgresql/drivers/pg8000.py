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
pg8000 IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from ..._abstract_drivers import AbstractModuleDriver
from ..._abstract_drivers import _ConnWrapper
from ...interfaces import IDBDriver

__all__ = [
    'PG8000Driver',
]

# Just enough lobject functionality for everything to work. This is
# not threadsafe or useful outside of relstorage, it implements
# exactly our requirements.

class _WriteBlob(object):
    closed = False

    def __init__(self, conn, binary):
        self._binary = binary
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
                             {'oid': self.oid, 'off': self._offset, 'data': self._binary(data)})
        self._offset += len(data)
        return len(data)

class _UploadBlob(object):
    closed = False
    fetch_size = 1024 * 1024 * 9

    def __init__(self, conn, new_file, binary):
        blob = _WriteBlob(conn, binary)
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

def _make_lobject_method_for_connection(self, binary):
    def lobject(oid=0, mode='', new_oid=0, new_file=None):
        if oid == 0 and new_oid == 0 and mode == 'wb':
            if new_file:
                # Upload the whole file right now.
                return _UploadBlob(self, new_file, binary)
            return _WriteBlob(self, binary)
        if oid != 0 and mode == 'rb':
            return _ReadBlob(self, oid)
        raise AssertionError("Unsupported params", dict(locals()))

    return lobject


@implementer(IDBDriver)
class PG8000Driver(AbstractModuleDriver):
    __name__ = 'pg8000'
    MODULE_NAME = __name__
    PRIORITY = 3
    PRIORITY_PYPY = 2

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = True

    def __init__(self):
        super(PG8000Driver, self).__init__()
        # XXX: global side-effect!
        self.driver_module.paramstyle = 'pyformat'

        # XXX: Testing. Can we remove?
        self.disconnected_exceptions += (AttributeError,)
        self._connect = self.driver_module.connect

    # For debugging
    _wrap = False

    def connect(self, dsn): # pylint:disable=arguments-differ
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
        conn.lobject = _make_lobject_method_for_connection(conn, self.Binary)
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
