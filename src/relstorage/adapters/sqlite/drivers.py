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

import sqlite3

from zope.interface import implementer

from relstorage._compat import PY3
from relstorage._compat import PY2
from relstorage._compat import PY36
from relstorage._util import log_timed

from ..drivers import implement_db_driver_options
from ..drivers import AbstractModuleDriver
from ..drivers import MemoryViewBlobDriverMixin
from ..interfaces import IDBDriver
from .._util import DatabaseHelpersMixin

from .dialect import Sqlite3Dialect

__all__ = [
    'Sqlite3Driver',
]

database_type = 'sqlite3'
logger = __import__('logging').getLogger(__name__)

@implementer(IDBDriver)
class Sqlite3Driver(MemoryViewBlobDriverMixin,
                    AbstractModuleDriver):
    dialect = Sqlite3Dialect()
    __name__ = 'sqlite3'
    MODULE_NAME = __name__
    STATIC_AVAILABLE = (
        # Tested to work on Python 2.7 and Python 3.6+;
        # seen some strange ``ProgrammingError: closed``
        # on Python 3.5
        (PY2 or PY36)
        # 3.11 is the oldest version tested on CI
        and sqlite3.sqlite_version_info[:2] >= (3, 11)
    )

    def __init__(self):
        super(Sqlite3Driver, self).__init__()
        # Sadly, if a connection is closed out from under it,
        # sqlite3 throws ProgrammingError, which is not very helpful.
        # That should really only happen in tests, though, since
        # we're directly connected to the file on disk.
        self.disconnected_exceptions += (self.driver_module.ProgrammingError,)

    if PY3:
        # in_transaction doesn't work there, so assume the worst.
        def connection_may_need_rollback(self, conn):
            try:
                return conn.in_transaction
            except sqlite3.ProgrammingError:
                # we're closed. We do need to attempt the rollback so
                # we catch the error and know to drop the connection.
                return True

        connection_may_need_commit = connection_may_need_rollback


        # Py2 returns buffer for blobs, hence the Mixin.
        # But Py3 returns plain bytes.
        def binary_column_as_state_type(self, data):
            return data

class UnableToConnect(sqlite3.OperationalError):
    filename = None
    def with_filename(self, f):
        self.filename = f
        return self

    def __str__(self):
        s = super(UnableToConnect, self).__str__()
        if self.filename:
            s += " (At: %r)" % self.filename
        return s


class Cursor(sqlite3.Cursor):
    has_analyzed_temp = False

    def execute(self, stmt, params=None):
        # While we transition away from hardcoded SQL to the query
        # objects, we still have some %s params out there. This papers
        # over that.
        if params is not None:
            stmt = stmt.replace('%s', '?')
            return sqlite3.Cursor.execute(self, stmt, params)

        return sqlite3.Cursor.execute(self, stmt)

    def executemany(self, stmt, params):
        stmt = stmt.replace('%s', '?')
        return sqlite3.Cursor.executemany(self, stmt, params)

    def __repr__(self):
        return '<Cursor at 0x%x from %r>' % (
            id(self), self.connection
        )

    def close(self):
        try:
            sqlite3.Cursor.close(self)
        except sqlite3.ProgrammingError:
            pass


class _ExplainCursor(DatabaseHelpersMixin): # pragma: no cover (A debugging aid)
    def __init__(self, cur):
        self.cur = cur

    def __getattr__(self, name):
        return getattr(self.cur, name)

    def __iter__(self):
        return iter(self.cur)

    def execute(self, sql, *args):
        if sql.strip().startswith(('INSERT', 'SELECT', 'DELETE', 'WITH')):
            exp = 'EXPLAIN QUERY PLAN ' + sql.lstrip()
            print(sql)
            self.cur.execute(exp, *args)
            print(self._rows_as_pretty_string(self.cur))
        return self.cur.execute(sql, *args)


class Connection(sqlite3.Connection):
    # pylint:disable=assigning-non-slot
    # Something about inheriting from an extension
    # class seems to get pylint confused.
    has_analyzed_temp = False
    before_commit_functions = ()
    _rs_has_closed = False
    replica = None

    def __init__(self, rs_db_filename, *args, **kwargs):
        __traceback_info__ = args, kwargs
        # PyPy3 calls self.commit() during __init__.
        self.rs_db_filename = rs_db_filename
        self.before_commit_functions = []

        try:
            super(Connection, self).__init__(*args, **kwargs)
        except sqlite3.OperationalError as e:
            raise UnableToConnect(e).with_filename(rs_db_filename)


    def __repr__(self):
        if not self.rs_db_filename:
            return super(Connection, self).__repr__()
        try:
            in_tx = self.in_transaction
        except sqlite3.ProgrammingError:
            in_tx = 'closed'

        return '<Connection at 0x%x to %r in_transaction=%s>' % (
            id(self), self.rs_db_filename,
            in_tx
        )

    if not PY3:
        # This helpful attribute is Python 3 only.
        assert not hasattr(sqlite3.Connection, 'in_transaction')
        @property
        def in_transaction(self):
            return None

    def register_before_commit_cleanup(self, func):
        self.before_commit_functions.append(func)

    if 0: # pylint:disable=using-constant-test
        def cursor(self):
            return _ExplainCursor(sqlite3.Connection.cursor(self, Cursor))
    else:
        def cursor(self):
            return sqlite3.Connection.cursor(self, Cursor)

    def commit(self, run_cleanups=True):
        if run_cleanups:
            for func in self.before_commit_functions:
                func(self)
        sqlite3.Connection.commit(self)

    def rollback(self):
        sqlite3.Connection.rollback(self)
        for func in self.before_commit_functions:
            func(self, True)

    @log_timed
    def close(self):
        # If we're the only connection open to this database,
        # and SQLITE_FCNTL_PERSIST_WAL is true (by default
        # *most* places, but apparently not in the sqlite3
        # 3.24 shipped with Apple in macOS 10.14.5), then when
        # we close the database the wal file that was built up
        # by any of the writes that have been done will be automatically
        # combined with the database file, as if with
        # "PRAGMA wal_checkpoint(RESTART)".
        #
        # This can be slow. It releases the GIL, so it could be done in another thread;
        # but most often (aside from tests) we're closing connections because we're
        # shutting down the process so spawning threads isn't really a good thing.
        if self._rs_has_closed: # pragma: no cover
            return

        self._rs_has_closed = True
        self.before_commit_functions = ()
        # Recommended best practice is to OPTIMIZE the database for
        # each closed connection. OPTIMIZE needs to run in each connection
        # so it can see what tables and indexes were used. It's usually fast,
        # but has the potential to be slow.
        try:
            self.execute("PRAGMA optimize")
        except sqlite3.OperationalError:
            logger.debug("Failed to optimize databas, probably in use", exc_info=True)
        except sqlite3.DatabaseError:
            # It's possible the file was removed.
            logger.exception("Failed to optimize database; was it removed?")

        super(Connection, self).close()


implement_db_driver_options(
    __name__,
    '.drivers'
)
