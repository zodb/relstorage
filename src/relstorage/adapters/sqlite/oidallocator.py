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
"""sqlite3 adapter for RelStorage."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading

from zope.interface import implementer

from ..interfaces import IOIDAllocator
from ..oidallocator import AbstractTableOIDAllocator
from ..connections import StoreConnection
from ..connections import ClosedConnection

@implementer(IOIDAllocator)
class Sqlite3OIDAllocator(AbstractTableOIDAllocator):

    def __init__(self, driver, connmanager):
        super(Sqlite3OIDAllocator, self).__init__(driver)
        self.store_connection = StoreConnection(connmanager)
        self.lock = threading.Lock()
        self._use_count = 1

    def new_instance(self):
        with self.lock:
            self._use_count += 1
        return self

    def release(self):
        with self.lock:
            self._use_count -= 1
            if self._use_count <= 0:
                self.store_connection.drop()
                self.store_connection = ClosedConnection()

    def close(self):
        with self.lock:
            self._use_count = 0
            self.store_connection.drop()
            self.store_connection = ClosedConnection()

    def set_min_oid(self, cursor, oid_int):
        # Recall that the very first write to the database will cause
        # the file to be locked against further writes. So there's some
        # benefit in avoiding writes if they're not needed. Because we
        # keep this in a separate database as well, we can keep the connection
        # in autocommit mode.
        with self.lock:
            # We've left the underlying connection in autocommit mode.
            cursor = self.store_connection.get_cursor()
            cursor.execute(
                'SELECT seq FROM sqlite_sequence WHERE name = ?',
                ('new_oid',))
            rows = cursor.fetchall()
            if rows and rows[0][0] >= oid_int:
                return

            cursor.execute('UPDATE sqlite_sequence SET seq = MAX(?, seq) WHERE name = ?',
                           (oid_int, 'new_oid'))
            cursor.fetchall()
            if not cursor.rowcount:
                # If we've never actually put any values into new_oid,
                # then even though the ``sqlite_sequence`` table exists,
                # it won't have an entry for ``new_oid`` and the UPDATE
                # will do nothing.
                #
                # This is mostly an issue in tests, or if we've only copied
                # transactions.
                cursor.execute(
                    'INSERT INTO sqlite_sequence(name, seq) '
                    'VALUES (?, ?)',
                    ('new_oid', oid_int))
                cursor.fetchall()

    def new_oids(self, cursor):
        with self.lock:
            return AbstractTableOIDAllocator.new_oids(self, self.store_connection.get_cursor())

    def reset_oid(self, cursor):
        with self.lock:
            cursor = self.store_connection.get_cursor()
            AbstractTableOIDAllocator.reset_oid(self, cursor)
            cursor.execute('DELETE FROM sqlite_sequence WHERE name = ?', ('new_oid',))
