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

from relstorage._util import log_timed
from relstorage._compat import PY3
from relstorage._compat import WIN

from ..connmanager import AbstractConnectionManager

logger = __import__('logging').getLogger(__name__)


_MB = 1024 * 1024
DEFAULT_MAX_WAL = 10 * _MB

# Benchmarking on at least one system doesn't show an improvement to
# either reading or writing by forcing a large mmap_size. By default,
# just report. A setting of 0 means do not use.
DEFAULT_MMAP_SIZE = None
# 4096 is the page size in current releases of sqlite; older versions
# used 1024. A larger page makes sense as we have biggish values.
# Going larger doesn't make much difference in benchmarks.
DEFAULT_PAGE_SIZE = 4096
# Control where temporary data is:
#
# FILE = a deleted disk file (that sqlite never flushes so
# theoretically just exists in the operating system's filesystem
# cache)
#
# MEMORY = explicitly in memory only
#
# DEFAULT = compile time default. Benchmarking for large writes
# doesn't show much difference between FILE and MEMORY, so don't
# bother to change from the default.
DEFAULT_TEMP_STORE = None

# How long before we get 'OperationalError: database is locked'
DEFAULT_TIMEOUT = 15


class Sqlite3ConnectionManager(AbstractConnectionManager):

    def __init__(self, driver, path, options):
        self.path = path
        self.keep_history = options.keep_history
        super(Sqlite3ConnectionManager, self).__init__(options, driver)

    def open(self,
             isolation=None,
             read_only=False,
             deferrable=False,
             replica_selector=None,
             application_name=None,
             **kwargs):
        # SQLite doesn't really have isolation levels
        # in the traditional sense. It always operates in SERIALIZABLE
        # mode.
        conn = connect_to_file(self.path,
                               query_only=read_only,
                               timeout=self.options.commit_lock_timeout,
                               quick_check=False,
                               extra_pragmas={
                                   'foreign_keys': 'on',
                                   'journal_size_limit': None,
                               })
        cur = conn.cursor()
        return conn, cur

    def _do_open_for_load(self):
        conn, cur = self.open(
            isolation=self.isolation_load)

        cur.execute('BEGIN DEFERRED TRANSACTION')
        return conn, cur

    def rollback_store_quietly(self, conn, cur):
        super(Sqlite3ConnectionManager, self).rollback_store_quietly(conn, cur)
        # Important to ensure that our temp tables get cleared out
        self._call_hooks(self._on_store_opened, conn, cur, cur, restart=True)

    def restart_load(self, conn, cursor, needs_rollback=True):
        super(Sqlite3ConnectionManager, self).restart_load(conn, cursor, needs_rollback)
        # If we don't explicitly begin a transaction block, we're in
        # autocommit-mode, meaning we move forward and don't have a consistent view.
        cursor.execute('BEGIN DEFERRED TRANSACTION')


class Cursor(sqlite3.Cursor):

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

class Connection(sqlite3.Connection):
    # pylint:disable=assigning-non-slot
    # Something about inheriting from an extension
    # class seems to get pylint confused.

    __slots__ = (
        'rs_db_filename',
        '_rs_has_closed',
        'replica',
    )

    def __init__(self, *args, **kwargs):
        __traceback_info__ = args, kwargs
        super(Connection, self).__init__(*args, **kwargs)

        self.rs_db_filename = None
        self._rs_has_closed = False
        self.replica = None

    def __repr__(self):
        if not self.rs_db_filename:
            return super(Connection, self).__repr__()
        return '<Connection at %x to %r>' % (
            id(self), self.rs_db_filename
        )

    def cursor(self):
        return sqlite3.Connection.cursor(self, Cursor)

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


_last_changed_settings = ()
_last_reported_settings = ()

def __check_and_log(reported_values, changed_values):
    global _last_changed_settings
    global _last_reported_settings
    reported_to_save = tuple(sorted(reported_values.items()))
    changed_to_save = tuple(sorted(changed_values.items()))

    if reported_to_save != _last_reported_settings or changed_to_save != _last_changed_settings:
        _last_changed_settings = changed_to_save
        _last_reported_settings = reported_to_save
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
def _execute_pragma(cur, name, value):
    stmt = 'PRAGMA %s = %s' % (name, value)
    cur.execute(stmt)
    # On PyPy, it's important to traverse the cursor, even if
    # you don't expect any results, because it still counts as
    # a statement that's open and can cause 'OperationalError:
    # can't commit with SQL operations active'.
    return cur.fetchall()

def _execute_pragmas(cur, **kwargs):
    report = {}
    changed = {} # {setting: (original, desired, updated)}
    for k, v in kwargs.items():
        # Query, report, then change
        __traceback_info__ = k, v
        stmt = 'PRAGMA %s' % (k,)
        cur.execute(stmt)
        row = cur.fetchone()
        orig_value = row[0] if row else None
        if v is not None and v != orig_value:
            _execute_pragma(cur, k, v)
            cur.execute(stmt)
            row = cur.fetchone()
            new_value = row[0] if row else None
            changed[k] = (orig_value, v, new_value)
        else:
            report[k] = orig_value

    __check_and_log(report, changed)

def _connect_to_file_or_uri(fname,
                            factory=Connection,
                            timeout=DEFAULT_TIMEOUT,
                            pragmas=None,
                            quick_check=True,
                            **connect_args):

    assert issubclass(factory, Connection)

    connection = sqlite3.connect(
        fname,
        # If we do nothing, this means we're in autocommit
        # mode. Creating our own transactions with BEGIN
        # disables that until the COMMIT.
        isolation_level=None,
        factory=factory,
        # We explicitly push closing off to a new thread.
        check_same_thread=False,
        timeout=timeout,
        **connect_args
    )

    connection.rs_db_filename = fname

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
        _execute_pragmas(cur, **pragmas)
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


USE_SHARED_CACHE = False

def connect_to_file(fname,
                    query_only=False,
                    max_wal_size=DEFAULT_MAX_WAL,
                    mmap_size=DEFAULT_MMAP_SIZE,
                    page_size=DEFAULT_PAGE_SIZE,
                    temp_store=DEFAULT_TEMP_STORE,
                    timeout=DEFAULT_TIMEOUT,
                    quick_check=True,
                    extra_pragmas=None):
    """
    Return a DB-API Connection object.

    .. caution:: Using the connection as a context manager does **not**
       result in the connection being closed, only committed or rolled back.
    """

    # There are some things we'd like to do, but we can't
    # unless we're operating on at least Python 3.4, where the uri
    # syntax is supported.
    #
    # - Enable shared cache. (https://www.sqlite.org/sharedcache.html)
    # This doesn't work because of locking: all connections sharing the cache
    # lock at the same time.

    fname = os.path.abspath(fname) if fname and fname != ':memory:' else fname

    connect_args = {}
    if PY3 and USE_SHARED_CACHE:
        connect_args = {'uri': True}
        # Follow the steps to convert to a URI
        # https://www.sqlite.org/uri.html#the_uri_path
        fname = fname.replace('?', '%3f')
        fname = fname.replace('#', '%23')
        if WIN:
            fname = fname.replace('\\', '/')
        while '//' in fname:
            fname = fname.replace('//', '/')
        if WIN and os.path.splitdrive(fname)[0]:
            fname = '/' + fname
        fname = 'file:' + fname + '?cache=shared'
        # It might seem nice to use '?mode=ro' if query_only, but
        # we can't do that if the file doesn't exist, and we can't execute
        # certain pragmas either.

    pragmas = {
        # WAL mode can actually be a bit slower at commit time,
        # but buys us far better concurrency.
        # Note: In-memory databases always use 'memory' as the journal mode;
        # temporary databases always use 'delete'.
        'journal_mode': 'wal',
        'journal_size_limit': max_wal_size,
        'mmap_size': mmap_size,
        'page_size': page_size,
        'temp_store': temp_store,
        # WAL mode is always consistent even after a operating system
        # crash in NORMAL mode. It might lose a transaction, though.
        # The default is often FULL, which is higher than NORMAL.
        'synchronous': 'NORMAL',
        # Disable auto-checkpoint so that commits have
        # reliable duration; after commit, if it's a good time,
        # we can run 'PRAGMA wal_checkpoint'. (In most cases, the last
        # database connection that's open will essentially do that
        # automatically.)
        # XXX: Is that really worth it? We have seen some apparent corruptions,
        # so we'le leave it at the default and just report it.
        # 'wal_autocheckpoint': 0,
        'wal_autocheckpoint': None,
        # Things to query and report.
        'soft_heap_limit': None, # 0 means no limit
        # The default of -2000 is 2000 pages. At 4K page size,
        # that's 8MB.
        'cache_size': None,
        # How big is the database?
        'page_count': None,
    }

    if query_only:
        pragmas['query_only'] = 'on'

    pragmas.update(extra_pragmas or {})

    connection = _connect_to_file_or_uri(
        fname,
        pragmas=pragmas,
        timeout=timeout,
        quick_check=quick_check,
        **connect_args
    )

    return connection
