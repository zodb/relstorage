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
psycpg2-like lobject support using the SQL interface,
for drivers and modes that don't support it natively.

For example, pg8000 provides no LOB support, and psycopg2 when in
asynchronous mode has no LOB support. This package provides just enough
functionality for our purposes. Activate it by deriving a class from
the driver's Connection class and this module's :class:`LobConnectionMixin`.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

__all__ = [
    'LobConnectionMixin',
]


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



class LobConnectionMixin(object):

    #: The driver module's ``Binary`` converter object.
    RSDriverBinary = None

    def lobject(self, oid=0, mode='', new_oid=0, new_file=None):
        if oid == 0 and new_oid == 0 and mode == 'wb':
            if new_file:
                # Upload the whole file right now.
                return _UploadBlob(self, new_file, self.RSDriverBinary)
            return _WriteBlob(self, self.RSDriverBinary)
        if oid != 0 and mode == 'rb':
            return _ReadBlob(self, oid)
        raise AssertionError("Unsupported params", dict(locals()))
