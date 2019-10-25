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

from ..connmanager import AbstractConnectionManager
from .drivers import Connection

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
    consistent snapshot of the database, but for load, we can let the
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
    # more apparent should that happen.
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

        conn = connect_to_file(self.path,
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

    def _do_open_for_store(self, **open_args):
        # We also make sure some stats are populated about the
        # size of the database. We make sure it assumes that the main
        # state tables are large in comparison to the temp tables. If
        # we don't, then we tend to get a query plan for detecting
        # conflicts that is very bad: SCAN object_state SEARCH
        # temp_store BY PRIMARY KEY instead of the good one, SCAN
        # temp_store SEARCH object_state BY PRIMARY KEY
        conn, cur = AbstractConnectionManager._do_open_for_store(self, **open_args)
        # We're in an autocommit connection right now
        assert not conn.in_transaction
        if not self.are_stats_ok(cur):
            self.correct_stats(cur)
            conn.commit()
        return conn, cur

    desired_large_table_stat = 2000000

    def are_stats_ok(self, cursor):
        try:
            cursor.execute("""
            SELECT stat FROM sqlite_stat1
            WHERE tbl in ('object_state', 'current_object')
            """)
        except sqlite3.OperationalError:
            return False
        rows = cursor.fetchall()
        if not rows:
            return False
        for row in rows:
            # a space separated list, first item is integer giving # rows
            # But the Python library may have already taken it as an integer.
            try:
                count = int(row[0])
            except ValueError:
                count = int(row[0].split()[0])
            if count < self.desired_large_table_stat:
                return False
        return True

    def correct_stats(self, cursor):
        """
        Takes a lock and commits the transaction.
        """
        script = """
        DELETE FROM sqlite_stat1 WHERE tbl in ('object_state', 'current_object')
        AND idx IS NULL;
        """
        try:
            cursor.execute(script)
        except sqlite3.OperationalError:
            pass

        script = """
        ANALYZE;
        INSERT INTO sqlite_stat1
            SELECT 'object_state', NULL, {count}
            WHERE NOT EXISTS (
                SELECT 1 FROM sqlite_stat1 WHERE tbl = 'object_state'
                AND idx IS NULL
        );
        INSERT INTO sqlite_stat1
            SELECT 'current_object', NULL, {count}
            WHERE NOT EXISTS (
                SELECT 1 FROM sqlite_stat1 WHERE tbl = 'current_object'
                AND idx IS NULL
        );
        ANALYZE sqlite_master;
        """.format(count=self.desired_large_table_stat)
        cursor.executescript(script)
        cursor.connection.commit()

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
        row, = cur.execute(stmt).fetchall() or ((),)
        orig_value = row[0] if row else None
        if desired is not None and desired != orig_value:
            _execute_pragma(cur, pragma, desired)
            cur.execute(stmt)
            row = cur.fetchone()
            new_value = row[0] if row else None
            changed[pragma] = (orig_value, desired, new_value)
        else:
            report[pragma] = orig_value
    __check_and_log(report, changed)

def _connect_to_file_or_uri(fname,
                            factory=Connection,
                            timeout=DEFAULT_TIMEOUT,
                            pragmas=None,
                            quick_check=True,
                            isolation_level=None,
                            **connect_args):

    _factory = factory
    factory = lambda *args, **kwargs: _factory(fname, *args, **kwargs)

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


def connect_to_file(fname,
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

    connection = _connect_to_file_or_uri(
        fname,
        pragmas=pragmas,
        timeout=timeout,
        quick_check=quick_check,
        isolation_level=isolation_level
    )

    return connection
