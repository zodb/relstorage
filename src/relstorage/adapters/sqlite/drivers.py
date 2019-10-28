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
import sqlite3

from zope.interface import implementer

from relstorage._compat import PY3
from relstorage._compat import PY2
from relstorage._compat import PY36
from relstorage._util import log_timed

from ..drivers import implement_db_driver_options
from ..drivers import AbstractModuleDriver
from ..drivers import GeventDriverMixin
from ..drivers import MemoryViewBlobDriverMixin
from ..interfaces import IDBDriver
from .._util import DatabaseHelpersMixin

from .dialect import Sqlite3Dialect

__all__ = [
    'Sqlite3Driver',
    'Sqlite3GeventDriver',
]

database_type = 'sqlite3'
logger = __import__('logging').getLogger(__name__)



_MB = 1024 * 1024
DEFAULT_MAX_WAL = 10 * _MB

# Benchmarking on at least one system doesn't show an improvement to
# either reading or writing by forcing a large mmap_size (maybe that's
# not enabled on macOS?). By default, just report. A setting of 0
# means do not use.
DEFAULT_MMAP_SIZE = None
# 4096 is the page size in current releases of sqlite; older versions
# used 1024. A larger page makes sense as we have biggish values.
# Going larger doesn't make much difference in benchmarks.
DEFAULT_PAGE_SIZE = 4096
# The default of -2000 is abs(N*1024) = ~2MB. At 4K page size,
# that's about 500 pages. A negative number specifies bytes, a
# positive number specifies pages. In general, since we have two
# connections for each ZODB connection, we're probably best letting
# the cache stay relatively small and letting the operating system
# page cache decide between threads and process.s
DEFAULT_CACHE_SIZE = None
# Control where temporary data is:
#
# FILE = a deleted disk file (that sqlite never flushes so
# theoretically just exists in the operating system's filesystem
# cache)
#
# MEMORY = explicitly in memory only
#
# DEFAULT = compile time default. Benchmarking for large writes
# doesn't show much difference between FILE and MEMORY *usually*:
# Sometimes changing the setting back and forth causes massive
# changes in performance, at least on a macOS desktop system.
# bother to change from the default.
DEFAULT_TEMP_STORE = None

# How long before we get 'OperationalError: database is locked'
DEFAULT_TIMEOUT = 15


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
        # in_transaction doesn't work on Py2, so assume the worst
        # by inheriting the functions.
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

    default_connect_args = {
    }

    def _connect_to_file_or_uri(self, fname,
                                factory=None,
                                timeout=DEFAULT_TIMEOUT,
                                pragmas=None,
                                quick_check=True,
                                isolation_level=None,
                                **connect_args):
        if factory is None:
            factory = Connection

        _factory = factory
        factory = lambda *args, **kwargs: _factory(fname, *args, **kwargs)

        default_connect_args = self.default_connect_args.copy()
        default_connect_args.update(connect_args)
        connect_args = default_connect_args

        connection = sqlite3.connect(
            fname,
            # See Sqlite3ConnectionManager for the meaning of this.
            # We default to None which puts us in the underlying sqlite autocommit
            # mode. This is unlike the standard library, so you must
            # either set this or manage your own transactions explicitly
            isolation_level=isolation_level,
            factory=factory,
            # We explicitly push closing off to a new thread.
            check_same_thread=False,
            timeout=timeout,
            **connect_args
        )

        if str is bytes:
            # We don't use the TEXT type, but even so
            # sqlite complains:
            #
            # ProgrammingError: You must not use 8-bit bytestrings unless
            # you use a text_factory that can interpret 8-bit bytestrings
            # (like text_factory = str). It is highly recommended that you
            # instead just switch your application to Unicode strings.
            connection.text_factory = str

        # Make sure we have at least one pragma that touches
        # the database so that we can verify that it's not corrupt.
        pragmas = pragmas or {}
        pragmas.setdefault('journal_mode', 'wal')
        cur = connection.cursor()
        __traceback_info__ = fname, cur, pragmas
        try:
            connection.execute_pragmas(**pragmas)
            # Quick integrity check before we read.
            if quick_check:
                cur.execute('PRAGMA quick_check')
                rows = cur.fetchall()
                if len(rows) != 1 or rows[0][0] != 'ok':
                    msg = '\n'.join(row[0] for row in rows)
                    raise sqlite3.DatabaseError('Quick integrity check failed %s' % msg)
        except:
            logger.exception("Failed to execute pragmas")
            cur.close()
            connection.close()
            raise

        cur.close()

        return connection

    def connect_to_file(self, fname,
                        query_only=False,
                        max_wal_size=DEFAULT_MAX_WAL,
                        mmap_size=DEFAULT_MMAP_SIZE,
                        page_size=DEFAULT_PAGE_SIZE,
                        cache_size=DEFAULT_CACHE_SIZE,
                        temp_store=DEFAULT_TEMP_STORE,
                        timeout=DEFAULT_TIMEOUT,
                        quick_check=True,
                        isolation_level=None,
                        extra_pragmas=None,
                        override_pragmas=None):
        """
        Return a DB-API Connection object.

        .. caution:: Using the connection as a context manager does **not**
           result in the connection being closed, only committed or rolled back.
        """

        # There are some things we might like to do, but we can't
        # unless we're operating on at least Python 3.4, where the URI
        # syntax is supported. It also turns out that they don't really work either:
        #
        # - Enable shared cache. (https://www.sqlite.org/sharedcache.html)
        # This doesn't work because of locking: all connections sharing the cache
        # lock at the same time.
        # - Use ?mode=ro for true query_only mode. But if the file doesn't
        # exist that fails, and we can't execute certain pragmas that way either.

        fname = os.path.abspath(fname) if fname and fname != ':memory:' else fname

        # Things we like but don't require.
        # Note that we use the primitive values for these things so that we can
        # detect when they get set for reporting purposes.
        pragmas = {
            'journal_size_limit': max_wal_size,
            'mmap_size': mmap_size,
            'page_size': page_size,
            'temp_store': temp_store,
            # WAL mode is always consistent even after a operating system
            # crash in NORMAL mode. It might lose a transaction, though.
            # The default is often FULL/2, which is higher than NORMAL/1.
            'synchronous': 1,
            # If cache_spill is allowed, at some point a transaction
            # can begin writing dirty pages to the database file, taking
            # an exclusive lock. That could be at arbitrary times, so we don't want that.
            'cache_spill': 0,

            # Disable auto-checkpoint so that commits have
            # reliable duration; after commit, if it's a good time,
            # we can run 'PRAGMA wal_checkpoint'. (In most cases, the last
            # database connection that's open will essentially do that
            # automatically.)
            # XXX: Is that really worth it? We have seen some apparent corruptions,
            # maybe due to that? It's also a balance between readers and writers.
            # so we'le leave it at the default and just report it.
            'wal_autocheckpoint': None,
            # Things to query and report.
            'soft_heap_limit': None, # 0 means no limit
            'cache_size': cache_size,
            # How big is the database?
            'page_count': None,
        }

        # User-specified extra pragmas go here.
        pragmas.update(extra_pragmas or {})

        # Things that *must* be set.
        required_pragmas = {
            # WAL mode can actually be a bit slower at commit time,
            # but buys us far better concurrency.
            # Note: In-memory databases always use 'memory' as the journal mode;
            # temporary databases always use 'delete'.
            'journal_mode': 'wal',
        }

        if query_only:
            required_pragmas['query_only'] = 1

        pragmas.update(required_pragmas)

        pragmas.update(override_pragmas or {})

        connection = self._connect_to_file_or_uri(
            fname,
            pragmas=pragmas,
            timeout=timeout,
            quick_check=quick_check,
            isolation_level=isolation_level
        )

        return connection


class Sqlite3GeventDriver(GeventDriverMixin,
                          Sqlite3Driver):

    __name__ = 'gevent ' + Sqlite3Driver.MODULE_NAME
    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = False

    default_connect_args = {
        # Keep a large number of cached statements. Certain
        # data is tracked at the cached statement level. In our
        # case that's notably the number of virtual machine instructions
        # it has executed.
        'cached_statements': 1024
    }

    # Call ``gevent.sleep()`` after this many virtual instructions
    # have been executed by the SQLite virtual machine for any one
    # statement (because instruction execution is tracked at the statement
    # level, not the database level).
    yield_to_gevent_instruction_interval = 100

    def configure_from_options(self, options):
        if options.adapter:
            conf = options.adapter.config
            self.yield_to_gevent_instruction_interval = conf.gevent_yield_interval

    def _connect_to_file_or_uri(self, *args, **kwargs): # pylint:disable=arguments-differ
        conn = super(Sqlite3GeventDriver, self)._connect_to_file_or_uri(*args, **kwargs)
        conn.set_progress_handler(self.gevent.sleep,
                                  self.yield_to_gevent_instruction_interval)
        return conn

    def enter_critical_phase_until_transaction_end(self, connection, cursor):
        connection.set_progress_handler(None, 0)

    def commit(self, conn, cursor=None):
        super(Sqlite3GeventDriver, self).commit(conn, cursor)
        conn.set_progress_handler(self.gevent.sleep,
                                  self.yield_to_gevent_instruction_interval)

    def rollback(self, conn):
        super(Sqlite3GeventDriver, self).rollback(conn)
        conn.set_progress_handler(self.gevent.sleep,
                                  self.yield_to_gevent_instruction_interval)


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

    _last_changed_settings = ()
    _last_reported_settings = ()

    @staticmethod
    def __check_and_log(reported_values, changed_values):
        reported_to_save = tuple(sorted(reported_values.items()))
        changed_to_save = tuple(sorted(changed_values.items()))

        if reported_to_save != Connection._last_reported_settings or (
                changed_to_save != Connection._last_changed_settings):
            Connection._last_changed_settings = changed_to_save
            Connection._last_reported_settings = reported_to_save
            logger.debug(
                "Connected to sqlite3 version: %s. "
                "Default connection settings: %s. "
                "Custom connection settings: %s. ",
                sqlite3.sqlite_version,
                reported_values,
                " ".join("(%s was %s; wanted %s; got %s)" % ((k,) + v)
                         for k, v in changed_values.items()))

    # PRAGMA statements don't allow ? placeholders
    # when executed. This is probably a bug in the sqlite3
    # module.
    def _execute_pragma(self, name, value):
        stmt = 'PRAGMA %s = %s' % (name, value)
        cur = self.execute(stmt)
        # On PyPy, it's important to traverse the cursor, even if
        # you don't expect any results, because it still counts as
        # a statement that's open and can cause 'OperationalError:
        # can't commit with SQL operations active'.
        return cur.fetchall()

    def execute_pragmas(self, **kwargs):
        report = {}
        changed = {} # {setting: (original, desired, updated)}
        if 'page_size' in kwargs:
            # This has to happen before changing into WAL.
            ps = kwargs.pop('page_size')
            items = [('page_size', ps)]
            items.extend(kwargs.items())
        else:
            items = kwargs.items()

        for pragma, desired in items:
            # Query, report, then change
            stmt = 'PRAGMA %s' % (pragma,)
            row, = self.execute(stmt).fetchall() or ((),)
            orig_value = row[0] if row else None
            if desired is not None and desired != orig_value:
                self._execute_pragma(pragma, desired)
                row = self.execute(stmt).fetchone()
                new_value = row[0] if row else None
                changed[pragma] = (orig_value, desired, new_value)
            else:
                report[pragma] = orig_value
        self.__check_and_log(report, changed)

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
