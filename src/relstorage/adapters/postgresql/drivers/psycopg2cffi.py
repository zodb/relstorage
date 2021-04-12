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
psycopg2cffi IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from .psycopg2 import Psycopg2Driver

__all__ = [
    'Psycopg2cffiDriver',
    # TODO: A gevent driver seems like it should be possible?
]

class Psycopg2cffiDriver(Psycopg2Driver):
    __name__ = 'psycopg2cffi'
    MODULE_NAME = __name__
    PRIORITY = 2
    PRIORITY_PYPY = 1

    def _create_connection(self, mod): # pylint:disable=arguments-differ
        # The psycopg2cffi cursor doesn't go through the generic
        # ``execute()`` function for copy_to/from/expert, it goes
        # right to the low-level _pq_execute() method. As a
        # consequence, the connection object's state may not be right.
        # It doesn't know that a transaction has been started, and so
        # consequently the next time we *do* use ``execute()``, it begins a
        # new transaction. Because that clears temp tables, we're hosed.
        # Thus, to make copy play right, we need to keep the connection in the loop;
        # we can do that by making sure we call ``execute()`` before doing a copy,
        # or we can directly make the same low-level call that is missing;
        # we choose the later.

        if getattr(mod, 'RSPsycopg2cffiConnection', self) is self:
            class Cursor(mod.extensions.cursor):

                def copy_expert(self, *args, **kwargs):
                    self.connection._begin_transaction()
                    return super(Cursor, self).copy_expert(*args, **kwargs)

            class Connection(super(Psycopg2cffiDriver, self)._create_connection(mod, 'readonly')):
                def __init__(self, dsn, **kwargs):
                    super(Connection, self).__init__(dsn, **kwargs)
                    self.cursor_factory = Cursor
                    self.readonly = None

            mod.RSPsycopg2cffiConnection = Connection

        return mod.RSPsycopg2cffiConnection

    def _get_extension_module(self):
        from psycopg2cffi import extensions # pylint:disable=no-name-in-module,import-error
        return extensions

    # as of psycopg2cffi 2.8.1 connection has no '.info' attribute.

    def debug_connection(self, conn, *extra): # pragma: no cover
        print(conn, *extra)

    def connection_may_need_rollback(self, conn):
        return conn.status != self.STATUS_READY

    def connection_may_need_commit(self, conn):
        if conn.readonly:
            return False
        return self.connection_may_need_rollback(conn)

    def sync_status_after_hidden_commit(self, conn):
        conn.status = self.STATUS_READY
