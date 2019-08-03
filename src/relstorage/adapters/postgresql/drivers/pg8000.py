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

__all__ = [
    'PG8000Driver',
]

# Just enough lobject functionality for everything to work. This is
# not threadsafe or useful outside of relstorage, it implements
# exactly our requirements.

class _WriteBlob(object):
    closed = False

    def __init__(self, conn, binary):
        self._binary = binary
        self._cursor = conn.cursor()
        self._offset = 0
        try:
            self._cursor.execute("SELECT lo_creat(-1)")
            row = self._cursor.fetchone()
            self.oid = row[0]
        except:
            self._cursor.close()
            raise

    def close(self):
        self._cursor.close()
        self.closed = True

    def write(self, data):
        self._cursor.execute("SELECT lo_put(%(oid)s, %(off)s, %(data)s)",
                             {'oid': self.oid, 'off': self._offset, 'data': self._binary(data)})
        self._offset += len(data)
        return len(data)

class _UploadBlob(object):
    closed = False
    fetch_size = 1024 * 1024 * 9

    def __init__(self, conn, new_file, binary):
        blob = _WriteBlob(conn, binary)
        self.oid = blob.oid
        try:
            with open(new_file, 'rb') as f:
                while 1:
                    data = f.read(self.fetch_size)
                    if not data:
                        break
                    blob.write(data)
        finally:
            blob.close()

    def close(self):
        self.closed = True

class _ReadBlob(object):
    closed = False
    fetch_size = 1024 * 1024 * 9

    def __init__(self, conn, oid):
        self._cursor = conn.cursor()
        self.oid = oid
        self.offset = 0

    def export(self, filename):
        with open(filename, 'wb') as f:
            while 1:
                data = self.read(self.fetch_size)
                if not data:
                    break
                f.write(data)
        self.close()

    def read(self, size):
        self._cursor.execute("SELECT lo_get(%(oid)s, %(off)s, %(cnt)s)",
                             {'oid': self.oid, 'off': self.offset, 'cnt': size})
        row = self._cursor.fetchone()
        data = row[0]
        self.offset += len(data)
        return data

    def close(self):
        self._cursor.close()
        self.closed = True


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

        class Connection(self.driver_module.Connection):
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

            def lobject(self, oid=0, mode='', new_oid=0, new_file=None):
                if oid == 0 and new_oid == 0 and mode == 'wb':
                    if new_file:
                        # Upload the whole file right now.
                        return _UploadBlob(self, new_file, Binary)
                    return _WriteBlob(self, Binary)
                if oid != 0 and mode == 'rb':
                    return _ReadBlob(self, oid)
                raise AssertionError("Unsupported params", dict(locals()))

        self._connect = Connection

    def connect(self, dsn): # pylint:disable=arguments-differ
        # Parse the DSN into parts to pass as keywords.
        # We don't do this psycopg2 because a real DSN supports more options than
        # we do and we don't want to limit it.
        kwds = {}
        parts = dsn.split(' ')
        for part in parts:
            key, value = part.split('=')
            value = value.strip("'\"")
            if key == 'dbname':
                key = 'database'
            kwds[key] = value
        conn = self._connect(**kwds)
        return conn

    # Extensions

    ISOLATION_LEVEL_READ_COMMITTED = 'ISOLATION LEVEL READ COMMITTED'
    ISOLATION_LEVEL_SERIALIZABLE = 'ISOLATION LEVEL SERIALIZABLE'
    ISOLATION_LEVEL_REPEATABLE_READ = 'ISOLATION LEVEL REPEATABLE READ'

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        conn = self.connect(dsn)
        cursor = conn.cursor()
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
        if application_name:
            cursor.execute("SELECT set_config('application_name', %s, False)", (application_name,))
        conn.commit()
        return conn, cursor

    sql_compiler_class = PG8000Compiler

    def set_lock_timeout(self, cursor, timeout):
        # PG8000 needs a literal embedded in the string; prepared
        # statements can't be SET with a variable.
        assert isinstance(timeout, number_types)
        cursor.execute('SET lock_timeout = %s' % (timeout,))
