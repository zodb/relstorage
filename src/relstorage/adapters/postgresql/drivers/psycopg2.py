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

from ...interfaces import IDBDriver
from ...drivers import MemoryViewBlobDriverMixin

from . import AbstractPostgreSQLDriver
from ._lobject import LobConnectionMixin

__all__ = [
    'Psycopg2Driver',
    'GeventPsycopg2Driver',
]


@implementer(IDBDriver)
class Psycopg2Driver(MemoryViewBlobDriverMixin,
                     AbstractPostgreSQLDriver):
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

        self.TS_ACTIVE = psycopg2.extensions.TRANSACTION_STATUS_ACTIVE
        self.TS_INTRANS = psycopg2.extensions.TRANSACTION_STATUS_INTRANS
        self.TS_INERROR = psycopg2.extensions.TRANSACTION_STATUS_INERROR

        self.TS_NEEDS_COMMIT = (self.TS_ACTIVE, self.TS_INTRANS)
        self.TS_NOT_NEEDROLLBACK = psycopg2.extensions.TRANSACTION_STATUS_IDLE

    def _create_connection(self, mod, *extra_slots):
        if getattr(mod, 'RSPsycopg2Connection', self) is self:
            class RSPsycopg2Connection(mod.extensions.connection):
                # The replica attribute holds the name of the replica this
                # connection is bound to.
                __slots__ = ('replica',) + extra_slots

            mod.RSPsycopg2Connection = RSPsycopg2Connection

        return mod.RSPsycopg2Connection

    def _check_wait_callback(self):
        from psycopg2 import extensions # pylint:disable=no-name-in-module

        if extensions.get_wait_callback() is not None: # pragma: no cover
            raise ImportError("Wait callback installed")

    def get_driver_module(self):
        self._check_wait_callback()
        return super(Psycopg2Driver, self).get_driver_module()

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        # Even though the documentation claims that "Any other
        # connection parameter supported by the client library/server
        # can be passed either in the connection string or as a
        # keyword," it doesn't allow application_name as a kwarg. So we have to
        # resort to changing the dsn.
        if application_name and 'application_name' not in dsn:
            dsn = "%s application_name='%s'" % (dsn, application_name)
        conn = self.connect(dsn)
        assert not conn.autocommit
        if isolation or deferrable or read_only:
            conn.set_session(isolation_level=isolation, readonly=read_only,
                             deferrable=deferrable)

        return conn

    def cursor(self, conn, server_side=False):
        if server_side:
            cursor = conn.cursor(name=str(id(conn)))
            cursor.arraysize = self.cursor_arraysize
            cursor.itersize = self.cursor_arraysize
        else:
            cursor = super(Psycopg2Driver, self).cursor(conn)
        return cursor

    def debug_connection(self, conn, *extra): # pragma: no cover
        print(conn,
              'ts', conn.info.transaction_status,
              's', conn.status, 'tss', conn.info.status,
              'readonly', conn.readonly,
              "needs commit", self.connection_may_need_commit(conn),
              "needs rollback", self.connection_may_need_rollback(conn),
              *extra)

    def connection_may_need_rollback(self, conn):
        # If we've immediately executed a 'BEGIN' command,
        # the connection will report itself in a transaction, but
        # unless we've actually executed some sort of statement the
        # database will still know we're not and could issue a warning.
        result = conn.info.transaction_status != self.TS_NOT_NEEDROLLBACK or conn.readonly
        return result

    def connection_may_need_commit(self, conn):
        if conn.readonly:
            return False
        return conn.info.transaction_status in self.TS_NEEDS_COMMIT

    def sync_status_after_commit(self, conn):
        # Sadly we can't do anything except commit. The .status
        # variable is untouchable
        self.commit(conn)


class GeventPsycopg2Driver(Psycopg2Driver):
    __name__ = 'gevent ' + Psycopg2Driver.MODULE_NAME

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = False

    supports_copy = False

    def _create_connection(self, mod, *extra_slots):
        if getattr(mod, 'RSGeventPsycopg2Connection', self) is self:
            Base = super(GeventPsycopg2Driver, self)._create_connection(mod, *extra_slots)
            from relstorage.adapters.drivers import GeventConnectionMixin

            class RSGeventPsycopg2Connection(GeventConnectionMixin,
                                             LobConnectionMixin,
                                             Base):
                RSDriverBinary = self.Binary


            mod.RSGeventPsycopg2Connection = RSGeventPsycopg2Connection

        return mod.RSGeventPsycopg2Connection


    def get_driver_module(self):
        # Make sure we can use gevent; if we can't the ImportError
        # will prevent this driver from being used.
        __import__('gevent')
        return super(GeventPsycopg2Driver, self).get_driver_module()

    def _check_wait_callback(self):
        from psycopg2 import extensions # pylint:disable=no-name-in-module
        if extensions.get_wait_callback() is None: # pragma: no cover
            raise ImportError("No wait callback installed")

    # TODO: Implement enter_critical_phase_until_transaction_end


class _GeventPsycopg2WaitCallback(object):

    def __init__(self):
        from gevent.socket import wait_read
        from gevent.socket import wait_write
        self.wait_read = wait_read
        self.wait_write = wait_write
        # pylint:disable=import-error,no-name-in-module
        from psycopg2.extensions import POLL_OK
        from psycopg2.extensions import POLL_WRITE
        from psycopg2.extensions import POLL_READ
        self.poll_ok = POLL_OK
        self.poll_write = POLL_WRITE
        self.poll_read = POLL_READ

    def __call__(self, conn):
        while 1:
            state = conn.poll()
            if state == self.poll_ok:
                return

            if state == self.poll_read:
                conn.gevent_wait_read()
            else:
                conn.gevent_wait_write()


def _gevent_did_patch(_event):
    try:
        from psycopg2.extensions import set_wait_callback
    except ImportError:

        pass
    else:
        set_wait_callback(_GeventPsycopg2WaitCallback())
