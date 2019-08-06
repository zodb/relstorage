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
        # doesn't seem to be the case.
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

class GeventMySQLdbDriver(MySQLdbDriver):
    __name__ = 'gevent MySQLdb'

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = False

    def __init__(self):
        super(GeventMySQLdbDriver, self).__init__()
        # Replace self._connect (which was MySQLdb.connect) with
        # direct call to our desired class.
        self._connect = self._get_connection_class()

    def get_driver_module(self):
        # Make sure we can use gevent; if we can't the ImportError
        # will prevent this driver from being used.
        __import__('gevent')
        return super(GeventMySQLdbDriver, self).get_driver_module()

    _Connection = None

    @classmethod
    def _get_connection_class(cls):
        if cls._Connection is None:
            # pylint:disable=import-error,no-name-in-module
            from MySQLdb.connections import Connection as Base

            from gevent import socket
            wait_read = socket.wait_read # pylint:disable=no-member
            wait_write = socket.wait_write # pylint:disable=no-member

            # Prior to mysqlclient 1.4, there was a 'waiter' Connection
            # argument that could be used to do this, but it was removed.
            # So we implement it ourself.
            class Connection(Base):
                def query(self, query):
                    # From the mysqlclient implementation:
                    # "Since _mysql releases the GIL while querying, we need immutable buffer"
                    if isinstance(query, bytearray):
                        query = bytes(query)

                    fileno = self.fileno()
                    wait_write(fileno)
                    self.send_query(query)
                    wait_read(fileno)
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

            cls._Connection = Connection
        return cls._Connection
