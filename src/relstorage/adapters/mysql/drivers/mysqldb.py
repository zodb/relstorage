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

from relstorage._util import Lazy

from . import AbstractMySQLDriver

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

    fetchall_on_rollback = True

    @Lazy
    def _strict_cursor(self):
        from MySQLdb.cursors import SSCursor # pylint:disable=no-name-in-module,import-error
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
                cur = SSCursor(conn)
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

    # If we have more rows than this, it will take multiple trips
    # to the socket and C to read them. OTOH, that's an indication of how often
    # we expect to be switching.
    cursor_arraysize = 10

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

    @classmethod
    def _get_connection_class(cls):
        if cls._Connection is None:
            # pylint:disable=import-error,no-name-in-module,too-many-statements
            from MySQLdb.connections import Connection as Base
            from MySQLdb.cursors import SSCursor as BaseCursor

            from gevent import socket
            from gevent import get_hub
            wait = socket.wait # pylint:disable=no-member

            class Cursor(BaseCursor):

                def __init__(self, connection):
                    BaseCursor.__init__(self, connection)
                    self._fetch_row = self._gevent_fetch_row

                def _gevent_fetch_row(self, size=1):
                    # Somewhat surprisingly, if we just wait on read,
                    # we end up blocking forever.
                    wait(self.connection.gevent_rw_watcher, hub=self.connection.gevent_hub)
                    return BaseCursor._fetch_row(self, size)

                def fetchall(self):
                    self.connection.check_watchers()
                    result = []
                    fetch = self.fetchmany
                    while 1:
                        # Even if self.rowcount is 0 we must still call
                        # or we get the connection out of sync.
                        rows = fetch()
                        if not rows:
                            break
                        result.extend(rows)
                        if self.rownumber == self.rowcount:
                            # Avoid a useless extra trip at the end.
                            break
                    return result

                def close(self):
                    # Account for being closed in a different thread than what
                    # opened us.
                    if '_fetch_row' in self.__dict__:
                        del self._fetch_row
                    BaseCursor.close(self)

            # Prior to mysqlclient 1.4, there was a 'waiter' Connection
            # argument that could be used to do this, but it was removed.
            # So we implement it ourself.
            class Connection(Base):
                default_cursor = Cursor
                gevent_read_watcher = None
                gevent_write_watcher = None
                gevent_rw_watcher = None
                gevent_hub = None

                def check_watchers(self):
                    # We can be used from more than one thread in a sequential
                    # fashion.
                    hub = get_hub()
                    if hub is not self.gevent_hub:
                        self.__close_watchers()

                        fileno = self.fileno()
                        hub = self.gevent_hub = get_hub()
                        self.gevent_read_watcher = hub.loop.io(fileno, 1)
                        self.gevent_write_watcher = hub.loop.io(fileno, 2)
                        self.gevent_rw_watcher = hub.loop.io(fileno, 3)

                def __close_watchers(self):
                    if self.gevent_read_watcher is not None:
                        self.gevent_read_watcher.close()
                        self.gevent_write_watcher.close()
                        self.gevent_rw_watcher.close()
                        self.gevent_hub = None

                def query(self, query):
                    # From the mysqlclient implementation:
                    # "Since _mysql releases the GIL while querying, we need immutable buffer"
                    if isinstance(query, bytearray):
                        query = bytes(query)

                    self.check_watchers()

                    wait(self.gevent_write_watcher, hub=self.gevent_hub)
                    self.send_query(query)
                    wait(self.gevent_read_watcher, hub=self.gevent_hub)
                    self.read_query_result()

                # The default implementations of 'rollback' and
                # 'commit' use only C API functions `mysql_rollback`
                # and `mysql_commit`; it doesn't touch any internal
                # state. Those in turn simply call
                # `mysql_real_query("rollback")` and
                # `mysql_real_query("commit")`. That's a synchronous
                # function that waits for the result to be ready. We
                # don't want to block like that (commit could
                # potentially take some time.)

                def rollback(self):
                    self.query('rollback')

                def commit(self):
                    self.query('commit')

                def close(self):
                    self.__close_watchers()
                    Base.close(self)

            cls._Connection = Connection
        return cls._Connection
