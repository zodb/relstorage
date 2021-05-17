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

"""
PostgreSQL IDBDriverOptions implementation.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ...drivers import implement_db_driver_options
from ...drivers import AbstractModuleDriver
from ...sql import DefaultDialect

logger = __import__('logging').getLogger(__name__)

class PostgreSQLDialect(DefaultDialect):
    """
    The defaults are setup for PostgreSQL.
    """

class AbstractPostgreSQLDriver(AbstractModuleDriver):
    dialect = PostgreSQLDialect()
    # Can we bundle statements into a single string?
    # "SELECT 1; COMMIT;"
    supports_multiple_statement_execute = True

    # Can we use the COPY command (copy_export)?
    supports_copy = True

    # PostgreSQL is the database most likey to generate
    # server-sent messages. Log those using a logger that
    # includes that name.
    message_logger = logger

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        """
        Return a connection whose transactions will have the defined
        characteristics and which will use the given
        ``application_name``.

        The driver should pass the ``application_name`` as part of the
        connect packet, and *not* via ``SET SESSION application_name``.
        This is because the former is both faster and
        because the latter does not work when connecting to a hot
        standby (it triggers "cannot set transaction read-write mode
        during recovery"). If it cannot be passed at connect time,
        an alternative is ``SELECT set_config('application_name', %s, FALSE)``
        which does not seem to have this problem.
        """
        raise NotImplementedError

    def set_lock_timeout(self, cursor, timeout):
        # PG8000 needs a literal embedded in the string; prepared
        # statements can't be SET with a variable.
        cursor.execute('SET lock_timeout = %s', (timeout,))

    def get_messages(self, conn):
        notices = conn.notices or ()
        if notices:
            notices = list(notices)
            if isinstance(notices[0], dict):
                # pg8000
                notices = [d[b'M'] for d in notices]
                conn.notices.clear()
            else:
                del conn.notices[:]
        return notices


    def synchronize_cursor_for_rollback(self, cursor):
        """Does nothing."""

    def execute_multiple_statement_with_hidden_commit(self, conn, cursor, stmt, params):
        # Exit the critical phase now. We don't have a fine-grained
        # way of doing this between statements, so up front is the fastest we
        # can exit. That's fine, Python isn't involved during releasing the locks; we're
        # either going to get the commit lock and successfully commit, or we're going to
        # fail. Either way, there's nothing we need to do to let other greenlets get going.
        # This is just like how MySQL handles it in ``callproc_multi_result``.
        self.exit_critical_phase(conn, cursor)
        cursor.execute(stmt, params)

    ERRCODE_DEADLOCK = '40P01'

    def exception_is_deadlock(self, exc):
        # psycopg2 raises psycopg2.errors.DeadlockDetected, which is an OperationalError
        # which is a DatabaseError. This has a pgcode attribute of ERRCODE_DEADLOCK
        #
        # pg8000 raises pg8000.dbapi.ProgrammingError, which is a DatabaseError.
        if isinstance(exc, self.driver_module.DatabaseError):
            return self._get_exception_pgcode(exc) == self.ERRCODE_DEADLOCK

    def _get_exception_pgcode(self, exc):
        return exc.pgcode

database_type = 'postgresql'

implement_db_driver_options(
    __name__,
    'pg8000', 'psycopg2', 'psycopg2cffi',
)
