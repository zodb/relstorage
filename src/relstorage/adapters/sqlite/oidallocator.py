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

import sqlite3
import threading

from zope.interface import implementer

from relstorage._util import consume
from ..interfaces import IOIDAllocator
from ..oidallocator import AbstractRangedOIDAllocator

@implementer(IOIDAllocator)
class Sqlite3OIDAllocator(AbstractRangedOIDAllocator):
    # Even though we use the new_oid table like AbstractTableOIDAllocator
    # does, because we have to take exclusive locks *anyway*, we can
    # ensure that it only ever has a single increasing row.

    def __init__(self, db_path, driver):
        super(Sqlite3OIDAllocator, self).__init__()
        self.db_path = db_path
        self.lock = threading.Lock()
        self.driver = driver
        self._connection = None

    def new_instance(self):
        return self

    def release(self):
        self.close()

    def close(self):
        with self.lock:
            if self._connection:
                self._connection.close()
                self._connection = None

    # This is actually the last OID we returned. We begin allocating beginning with
    # the *next* integer.
    _new_oid_query = 'CREATE TABLE new_oid (zoid INTEGER PRIMARY KEY NOT NULL);'

    def _connect(self):
        if self._connection is None:
            conn = self.driver.connect_to_file(
                self.db_path,
                mmap_size=1024 * 1024 * 10, # try to keep the whole thing in memory.
                override_pragmas={
                    # We can always reconstruct the contents of this file from the database
                    # itself, and speed is utterly critical.
                    'journal_mode': 'off',
                    'synchronous': 'off',
                }
            )
            try:
                consume(conn.execute('SELECT count(*) from new_oid'))
            except sqlite3.OperationalError:
                conn.executescript(
                    self._new_oid_query + """
                INSERT OR REPLACE INTO new_oid
                SELECT MAX(x) FROM (
                    SELECT 0 x
                    UNION ALL
                    SELECT MAX(zoid)
                    FROM new_oid
                )
                """)
            self._connection = conn
        return self._connection

    def _set_min_oid_from_range(self, cursor, n):
        # Recall that the very first write to the database will cause
        # the file to be locked against further writes. So there's some
        # benefit in avoiding writes if they're not needed. Because we
        # keep this in a separate database as well, we can keep the connection
        # in autocommit mode.
        with self.lock:
            # We've left the underlying connection in autocommit mode.
            conn = self._connect()
            rows = conn.execute(
                'SELECT zoid FROM new_oid WHERE zoid < ?',
                (n,)).fetchall()
            if rows:
                # Narf, we need to execute a write transaction.
                consume(conn.execute(
                    'UPDATE new_oid SET zoid = :new WHERE zoid < :new',
                    {'new': n}))

    def new_oids(self, cursor=None):
        return self.new_oids_no_cursor()

    def new_oids_no_cursor(self):
        with self.lock:
            conn = self._connect()
            consume(conn.execute('BEGIN IMMEDIATE TRANSACTION'))
            row, = conn.execute('SELECT zoid FROM new_oid')
            conn.execute('UPDATE new_oid SET zoid = zoid + 1')
            conn.commit()
        return self._oid_range_around(row[0] + 1)

    def reset_oid(self, cursor=None):
        with self.lock:
            self._connect().execute('UPDATE new_oid SET zoid = 0')
