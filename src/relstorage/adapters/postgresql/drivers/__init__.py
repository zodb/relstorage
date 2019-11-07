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
        notices = conn.notices
        if notices:
            notices = list(notices)
            if isinstance(notices[0], dict):
                # pg8000
                notices = [d[b'M'] for d in notices]
                conn.notices.clear()
            else:
                notices = list(notices)
                del conn.notices[:]
        return notices


    def synchronize_cursor_for_rollback(self, cursor):
        """Does nothing."""

database_type = 'postgresql'

implement_db_driver_options(
    __name__,
    'pg8000', 'psycopg2', 'psycopg2cffi',
)
