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
pg8000 IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from collections import deque

from zope.interface import implementer

from relstorage._compat import number_types

from ...interfaces import IDBDriver
from ...sql import Compiler

from . import AbstractPostgreSQLDriver
from . import PostgreSQLDialect
from ._lobject import LobConnectionMixin


__all__ = [
    'PG8000Driver',
]


class _tuple_deque(deque):

    def append(self, row): # pylint:disable=arguments-differ
        deque.append(self, tuple(row))

class PG8000Compiler(Compiler):

    def can_prepare(self):
        # Important: pg8000 1.10 - 1.13, at least, can't handle prepared
        # statements that take parameters but it doesn't need to because it
        # prepares every statement anyway. So you must have a backup that you use
        # for that driver.
        # https://github.com/mfenniak/pg8000/issues/132
        return False

class PG8000Dialect(PostgreSQLDialect):

    def compiler_class(self):
        return PG8000Compiler


@implementer(IDBDriver)
class PG8000Driver(AbstractPostgreSQLDriver):
    __name__ = 'pg8000'
    MODULE_NAME = __name__
    PRIORITY = 3
    PRIORITY_PYPY = 2

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = True

    dialect = PG8000Dialect()
    supports_multiple_statement_execute = False

    def __init__(self):
        super(PG8000Driver, self).__init__()
        # XXX: global side-effect!
        self.driver_module.paramstyle = 'pyformat'

        # XXX: Testing. Can we remove?
        self.disconnected_exceptions += (AttributeError,)

        # Sadly, pg8000 raises ProgrammingError when it can't
        # obtain a lock, making it hard to distinguish. We'll let other
        # clauses catch such exceptions.
        self.illegal_operation_exceptions = ()
        Binary = self.Binary

        if getattr(self.driver_module, 'RSConnection', self) is self:
            class Cursor(self.driver_module.Cursor):
                def __init__(self, conn):
                    super(Cursor, self).__init__(conn)
                    # pylint:disable=access-member-before-definition
                    assert isinstance(self._cached_rows, deque)
                    # Make sure rows are tuples, not lists.
                    # BTrees don't like lists.
                    self._cached_rows = _tuple_deque()

                @property
                def connection(self):
                    # silence the warning it wants to generate:
                    # "UserWarning: DB-API extension cursor.connection used"
                    return self._c

                def copy_expert(self, sql, stream):
                    return self.execute(sql, stream=stream)

            class Connection(LobConnectionMixin,
                             self.driver_module.Connection):
                readonly = False
                RSDriverBinary = staticmethod(Binary)

                def __init__(self,
                             user, host='localhost',
                             unix_sock=None,
                             port=5432, database=None,
                             password=None, ssl=None,
                             timeout=None, application_name=None,
                             max_prepared_statements=1000,
                             tcp_keepalive=True):
                    # pylint:disable=useless-super-delegation
                    # We have to do this because the super class requires
                    # all these arguments and doesn't have defaults
                    super(Connection, self).__init__(
                        user, host, unix_sock,
                        port, database, password, ssl, timeout, application_name,
                        max_prepared_statements, tcp_keepalive)

                def cursor(self):
                    return Cursor(self)

            self.driver_module.RSConnection = Connection

        self._connect = self.driver_module.RSConnection

    def connect(self, dsn, application_name=None): # pylint:disable=arguments-differ
        # Parse the DSN into parts to pass as keywords.
        # We don't do this psycopg2 because a real DSN supports more options than
        # we do and we don't want to limit it.
        kwds = {}
        parts = dsn.split(' ')
        for part in parts:
            key, value = part.split('=')
            value = value.strip("'\"")
            key = 'database' if key == 'dbname' else key
            value = int(value) if key == 'port' else value
            kwds[key] = value
        kwds['application_name'] = application_name
        conn = self._connect(**kwds)
        return conn

    ISOLATION_LEVEL_READ_COMMITTED = 'ISOLATION LEVEL READ COMMITTED'
    ISOLATION_LEVEL_SERIALIZABLE = 'ISOLATION LEVEL SERIALIZABLE'
    ISOLATION_LEVEL_REPEATABLE_READ = 'ISOLATION LEVEL REPEATABLE READ'

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        conn = self.connect(dsn, application_name=application_name)
        cursor = self.cursor(conn)
        # For the current transaction
        transaction_stmt = 'TRANSACTION %s %s %s' % (
            isolation,
            ', READ ONLY' if read_only else '',
            ', DEFERRABLE' if deferrable else ''
        )
        cursor.execute('SET ' + transaction_stmt)
        # For future transactions on this same connection.
        # NOTE: This will probably not play will with things like pgbouncer.
        # See http://initd.org/psycopg/docs/connection.html#connection.set_session
        cursor.execute('SET SESSION CHARACTERISTICS AS ' + transaction_stmt)
        conn.commit()
        conn.readonly = read_only
        return conn

    sql_compiler_class = PG8000Compiler

    def set_lock_timeout(self, cursor, timeout):
        # PG8000 needs a literal embedded in the string; prepared
        # statements can't be SET with a variable.
        assert isinstance(timeout, number_types)
        cursor.execute('SET lock_timeout = %s' % (timeout,))

    # TODO: Implement 'connection_may_need_commit' and 'connection_may_need_rollback'
    # based on the actual state of the transaction. In psycopg2, we have the .status
    # or .info to tell us; can we find something similar here?
    def connection_may_need_rollback(self, conn):
        # Got to rollback even if we are read only to get fresh view of the database.
        return True

    def connection_may_need_commit(self, conn):
        if conn.readonly:
            return False
        return conn.in_transaction
