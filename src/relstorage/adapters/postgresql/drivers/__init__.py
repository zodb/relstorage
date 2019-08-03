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

from ..._abstract_drivers import implement_db_driver_options
from ..._abstract_drivers import AbstractModuleDriver
from ...sql import DefaultDialect

class PostgreSQLDialect(DefaultDialect):
    """
    The defaults are setup for PostgreSQL.
    """

class AbstractPostgreSQLDriver(AbstractModuleDriver):
    dialect = PostgreSQLDialect()
    # Can we bundle statements into a single string?
    # "SELECT 1; COMMIT;"
    supports_multiple_statement_execute = True

    def connection_may_need_rollback(self, conn): # pylint:disable=unused-argument
        return True

    connection_may_need_commit = connection_may_need_rollback

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        raise NotImplementedError

    def set_lock_timeout(self, cursor, timeout):
        # PG8000 needs a literal embedded in the string; prepared
        # statements can't be SET with a variable.
        cursor.execute('SET lock_timeout = %s', (timeout,))

database_type = 'postgresql'

implement_db_driver_options(
    __name__,
    'pg8000', 'psycopg2', 'psycopg2cffi',
)
