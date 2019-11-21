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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os.path

from ..connmanager import AbstractConnectionManager

logger = __import__('logging').getLogger(__name__)


class Sqlite3ConnectionManager(AbstractConnectionManager):
    """
    SQLite doesn't really have isolation levels in the traditional
    sense; as far as that goes, it always operates in SERIALIZABLE
    mode. Instead, the connection's ``isolation_level`` parameter
    determines how autocommit behaves and the interaction between it
    at the SQLite and Python levels::

        - If it is ``None``, then the Python Connection object has nothing
          to do with transactions. SQLite operates in its default autocommit
          mode, beginning and ending a (read or write, as needed)
          transaction around every statement execution.

        - If it is set to IMMEDIATE or DEFERRED, than the Python
          Connection object watches each statement sent
          to ``Cursor.execute`` or ``Cursor.executemany`` to see
          if it's a DML statement (INSERT/UPDATE/DELETE/REPLACE, and
          prior to Python 3.6, basically anything else except a
          SELECT). If it is, and the sqlite level is not already
          in a transaction, then the Connection begins a transaction
          of the specified type before executing the statement. The
          Connection COMMITs when commit() is called or when a DDL
          statement is executed (prior to Python 3.6).

          For SELECT statements, the connection remains in its current state,
          which would be autocommit (read  committed) unless a transaction
          has been opened.

    Now, those are all write statements that theoretically would begin
    a write transaction and take a database lock, so it would seem
    like there's no difference between IMMEDIATE and DEFERRED. But
    there is: recall that temporary tables are actually in a temporary
    *database*, so when you write to one of those, sqlite's internal
    transaction locks only apply to it. A DEFERRED transaction thus
    allows other connections to write to their own temporary tables,
    while an IMMEDIATE transaction blocks other from writing to their
    temporaries.

    We thus tell Python SQLite to operate in DEFERRED mode; for load
    connections, we must explicitly execute the BEGIN to get a
    consistent snapshot of the database, but for store, we can let the
    Python Connection detect when we execute a write operation and
    begin the transaction then, letting SQLite upgrade the locks to
    explicit mode when we attempt a write to the main database.

    Taking an exclusive lock on the main database can be accomplished
    with any UPDATE statement, even one that doesn't match any actual
    rows.
    """

    # super copies these into isolation_load/store
    # We use 'EXCLUSIVE' for the load connection isolation but we don't
    # ever actually expect to get to that. It just makes problems
    # more apparent should that happen. EXCLUSIVE and IMMEDIATE do the same
    # thing in WAL mode, but in other modes, EXCLUSIVE also prevents
    # read access to the database. DEFERRED is the default if you just use
    # BEGIN TRANSACTION.
    isolation_serializable = 'EXCLUSIVE'
    isolation_read_committed = 'DEFERRED'

    def __init__(self, driver, pragmas, path, options):
        """
        :param dict pragmas: A map from string pragma name to string
            pragma value. These will be executed at connection open
            time. Except for WAL, and a few other critical values, we
            allow changing just about all the settings, letting the
            user turn off just about all the safety features so that
            the user can tune to their liking. The user can also set the
            ``max_page_count`` to operate as a quota system.
        """
        self.path = path
        self.keep_history = options.keep_history
        self.pragmas = pragmas.copy()
        # Things that we really want to be on.
        nice_to_have_pragmas = {
            'foreign_keys': 1
        }
        nice_to_have_pragmas.update(pragmas)
        self.pragmas = nice_to_have_pragmas
        self.pragmas['journal_size_limit'] = None
        super(Sqlite3ConnectionManager, self).__init__(options, driver)

        assert self.isolation_load == type(self).isolation_serializable
        assert self.isolation_store == type(self).isolation_read_committed


    def open(self,
             isolation=None,
             read_only=False,
             deferrable=False,
             replica_selector=None,
             application_name=None,
             **kwargs):

        if not os.path.exists(os.path.dirname(self.path)) and self.options.create_schema:
            os.makedirs(os.path.dirname(self.path))

        conn = self.driver.connect_to_file(
            self.path,
            query_only=read_only,
            timeout=self.options.commit_lock_timeout,
            quick_check=False,
            isolation_level=isolation,
            extra_pragmas=self.pragmas)
        cur = conn.cursor()
        return conn, cur

    def _do_open_for_load(self):
        conn, cur = self.open(
            isolation=self.isolation_load,
            read_only=True
        )
        # If we don't explicitly begin a transaction block, we're in
        # autocommit-mode for SELECT, which is essentially REPEATABLE READ, i.e.,
        # our MVCC state moves forward each time we read and we don't have a consistent
        # view.
        cur.execute('BEGIN DEFERRED TRANSACTION')
        return conn, cur

    def restart_load(self, conn, cursor, needs_rollback=True):
        needs_rollback = True
        super(Sqlite3ConnectionManager, self).restart_load(conn, cursor, needs_rollback)
        assert not conn.in_transaction
        # See _do_open_for_load.
        cursor.execute('BEGIN DEFERRED TRANSACTION')

    def open_for_pre_pack(self):
        # This operates in auto-commit mode.
        conn, cur = self.open(isolation=None)
        assert conn.isolation_level is None
        return conn, cur

    def open_for_pack_lock(self):
        # We don't use a pack lock.
        return (None, None)
