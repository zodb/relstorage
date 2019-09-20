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

import os

from zope.interface import implementer

from relstorage.adapters.interfaces import IDBDriver
from relstorage.adapters._abstract_drivers import AbstractModuleDriver

from relstorage._util import Lazy
from relstorage._util import parse_boolean

from . import AbstractMySQLDriver
from . import IterateFetchmanyMixin

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

class GeventMySQLdbDriver(MySQLdbDriver):
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

    # Set this to false to allow switching on queries and fetches
    # after we've made some sort of lock call. If you rarely have conflicts, then this
    # provides the best throughput. Set it to true to stop gevent from switching
    # on network traffic when locked; if you have conflicts and load issues, by
    # allowing a single greenlet to commit faster, you may gain improvements.
    # NOTE: This does not currently work as intended.
    use_critical_phase_when_locked = parse_boolean(
        os.environ.get('RS_GEVENT_NO_SWITCH_WHEN_LOCKED', "false")
    )

    def __init__(self):
        super(GeventMySQLdbDriver, self).__init__()
        # Replace self._connect (which was MySQLdb.connect) with
        # direct call to our desired class.
        self._connect = self._get_connection_class()
        self._strict_cursor = self._connect.default_cursor

    def get_driver_module(self):
        # Make sure we can use gevent; if we can't the ImportError
        # will prevent this driver from being used.
        __import__('gevent')
        return super(GeventMySQLdbDriver, self).get_driver_module()

    _Connection = None

    def callproc_multi_result(self, cursor, proc, args=()):
        if self.use_critical_phase_when_locked and 'lock' in proc:
            # We're entering a critical phase. We actually *don't* want to yield,
            # we want to get our results back as fast as possible
            # because other people will be waiting on us.
            cursor.enter_critical_phase_until_transaction_end()
        return MySQLdbDriver.callproc_multi_result(self, cursor, proc, args)

    @classmethod
    def _get_connection_class(cls):
        from ._mysqldb_gevent import Connection
        return Connection

    @Lazy
    def _server_side_cursor(self):
        return self._get_connection_class().default_cursor
