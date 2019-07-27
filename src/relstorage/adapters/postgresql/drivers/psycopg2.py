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
psycopg2 IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage._compat import PY3
from ...interfaces import IDBDriver
from . import AbstractPostgreSQLDriver


__all__ = [
    'Psycopg2Driver',
]


@implementer(IDBDriver)
class Psycopg2Driver(AbstractPostgreSQLDriver):
    __name__ = 'psycopg2'
    MODULE_NAME = __name__

    PRIORITY = 1
    PRIORITY_PYPY = 2

    def __init__(self):
        super(Psycopg2Driver, self).__init__()

        psycopg2 = self.get_driver_module()

        # pylint:disable=no-member

        self.Binary = psycopg2.Binary
        self.connect = self._create_connection(psycopg2)

        # extensions
        self.ISOLATION_LEVEL_READ_COMMITTED = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
        self.ISOLATION_LEVEL_SERIALIZABLE = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
        self.ISOLATION_LEVEL_REPEATABLE_READ = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
        self.STATUS_READY = psycopg2.extensions.STATUS_READY

    def _create_connection(self, mod):
        class Psycopg2Connection(mod.extensions.connection):
            # The replica attribute holds the name of the replica this
            # connection is bound to.
            __slots__ = ('replica',)

        return Psycopg2Connection

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        conn = self.connect(dsn)
        if isolation or deferrable or read_only:
            conn.set_session(isolation_level=isolation, readonly=read_only,
                             deferrable=deferrable)
        cursor = conn.cursor()
        if application_name:
            cursor.execute('SET SESSION application_name = %s', (application_name,))
            # Make it permanent, in case the connection rolls back.
            conn.commit()
        return conn, cursor

    # psycopg2 is smart enough to return memoryview or buffer on
    # Py3/Py2, respectively, for bytea columns. memoryview can't be
    # passed to bytes() on Py2 or Py3, but it can be passed to
    # cStringIO.StringIO() or io.BytesIO() --- unfortunately,
    # memoryviews, at least, don't like going to io.BytesIO() on
    # Python 3, and that's how we unpickle states. So while ideally
    # we'd like to keep it that way, to save a copy, we are forced to
    # make the copy. Plus there are tests that like to directly
    # compare bytes.

    if PY3:
        def binary_column_as_state_type(self, data):
            if data:
                # Calling 'bytes()' on a memoryview in Python 3 does
                # nothing useful.
                data = data.tobytes()
            return data
    else:
        def binary_column_as_state_type(self, data):
            if data:
                data = bytes(data)
            return data

    def connection_may_need_rollback(self, conn):
        # If we've immediately executed a 'BEGIN' command,
        # the connection will report itself in a transaction, but
        # unless we've actually executed some sort of statement the
        # database will still know we're not and could issue a warning.
        return conn.status != self.STATUS_READY

    def connection_may_need_commit(self, conn):
        return conn.status != self.STATUS_READY
