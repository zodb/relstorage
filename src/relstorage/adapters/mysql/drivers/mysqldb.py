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
MySQLdb IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage.adapters.interfaces import IDBDriver
from relstorage.adapters.interfaces import IDBDriverSupportsCritical
from relstorage.adapters.drivers import AbstractModuleDriver

from relstorage._util import Lazy

from . import AbstractMySQLDriver
from . import IterateFetchmanyMixin
from ...drivers import GeventDriverMixin

__all__ = [
    'MySQLdbDriver',
    'GeventMySQLdbDriver'
]

@implementer(IDBDriver)
class MySQLdbDriver(AbstractMySQLDriver):
    __name__ = 'MySQLdb'

    MODULE_NAME = 'MySQLdb'
    PRIORITY = 1
    PRIORITY_PYPY = 3
    _GEVENT_CAPABLE = False

    def synchronize_cursor_for_rollback(self, cursor):
        AbstractModuleDriver.synchronize_cursor_for_rollback(self, cursor)

    @Lazy
    def _server_side_cursor(self):
        from MySQLdb.cursors import SSCursor # pylint:disable=no-name-in-module,import-error
        class Cursor(IterateFetchmanyMixin, SSCursor):
            pass
        return Cursor

    @Lazy
    def _strict_cursor(self):
        SSCursor = self._server_side_cursor
        # Using MySQLdb.cursors.SSCursor can get us some legitimate
        # errors (ProgrammingError: (2014, "Commands out of sync; you
        # can't run this command now")), although it adds some overhead
        # because of more database communication. And the docstring says you have to
        # call `close()` before making another query, but in practice that
        # doesn't seem to be the case. You must consume everything though.
        #
        # For extra strictness/debugging, we can wrap this with our
        # custom debugging cursor.
        if 0: # pylint:disable=using-constant-test
            # TODO: Make this configurable, and add this to
            # all drivers.
            from relstorage._util import NeedsFetchallBeforeCloseCursor

            def cursor_factory(conn):
                cur = SSCursor(conn) # pylint:disable=too-many-function-args
                cur = NeedsFetchallBeforeCloseCursor(cur)
                return cur
        else:
            cursor_factory = SSCursor

        return cursor_factory

    def connect(self, *args, **kwargs):
        if self.STRICT and 'cursorclass' not in kwargs:
            kwargs['cursorclass'] = self._strict_cursor
        return AbstractMySQLDriver.connect(self, *args, **kwargs)

    def connection_may_need_rollback(self, conn):
        if conn.readonly:
            return True
        return True

    def connection_may_need_commit(self, conn):
        if conn.readonly:
            return False
        return True

@implementer(IDBDriverSupportsCritical)
class GeventMySQLdbDriver(GeventDriverMixin,
                          MySQLdbDriver):
    __name__ = 'gevent MySQLdb'

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = False

    # If we have more rows than this, it will take multiple trips to
    # the socket and C to read them. OTOH, that's a rough indication
    # of how often we will yield to the event loop. Using fetchall()
    # will yield between fetching this many rows, but all the results
    # will still be returned to the caller for processing in one
    # batch. Plain iteration will yield after this many rows, and return
    # rows to the caller one at a time.
    # cursor_arraysize = 1024 # We inherit cursor_arraysize from the abstract driver.

    def __init__(self):
        super(GeventMySQLdbDriver, self).__init__()
        # Replace self._connect (which was MySQLdb.connect) with
        # direct call to our desired class.
        self._connect = self._get_connection_class()
        self._strict_cursor = self._connect.default_cursor
        self._server_side_cursor = self._connect.default_cursor

    _Connection = None

    def enter_critical_phase_until_transaction_end(self, connection, cursor):
        # pylint:disable=unused-argument
        connection.enter_critical_phase_until_transaction_end()

    def is_in_critical_phase(self, connection, cursor):
        return connection.is_in_critical_phase()

    def exit_critical_phase(self, connection, cursor):
        connection.exit_critical_phase()

    @classmethod
    def _get_connection_class(cls):
        from ._mysqldb_gevent import Connection
        return Connection
