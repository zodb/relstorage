##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
Helpers for various disk-based persistent storage format.

Doesn't actually do any persistence itself.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import errno
import logging
import os
import os.path
import sqlite3

from relstorage._compat import PY3

if PY3:
    # On Py3, use the built-in pickle, so that we can get
    # protocol 4 when available. It is *much* faster at writing out
    # individual large objects such as the cache dict (about 3-4x faster)
    from pickle import Unpickler
    from pickle import Pickler
else:
    # On Py2, zodbpickle gives us protocol 3, but we don't
    # use its special binary type
    from relstorage._compat import Unpickler
    from relstorage._compat import Pickler

# Export
Unpickler = Unpickler
Pickler = Pickler

SQ3_SUPPORTS_WINDOW = sqlite3.sqlite_version_info >= (3, 25) # 2018-09-15
SQ3_SUPPORTS_UPSERT = sqlite3.sqlite_version_info >= (3, 24) # 2018-06-04
SQ3_SUPPORTS_PAREN_UPDATE = sqlite3.sqlite_version_info >= (3, 15) # 2016-10-14
SQ3_SUPPORTS_CTE = sqlite3.sqlite_version_info >= (3, 8, 3) # 2014-02-03
SQ3_MIN_VERSION = (3, 8, 3)
SQ3_IS_MIN_VERSION = sqlite3.sqlite_version_info >= SQ3_MIN_VERSION

logger = log = logging.getLogger(__name__)

def _normalize_path(options):
    path = os.path.expanduser(os.path.expandvars(options.cache_local_dir))
    path = os.path.abspath(path)
    return path

def trace_file(options, prefix):
    # Return an open file for tracing to, if that is set up.
    # Otherwise, return nothing.

    # We choose a trace file based on ZEO_CACHE_TRACE. If it is
    # set to 'single', then we use a single file (not suitable for multiple
    # process, but records client opens/closes). If it is set to any other value,
    # we include a pid. If it is not set, we do nothing.
    trace = os.environ.get("ZEO_CACHE_TRACE")
    if not trace or not options.cache_local_dir:
        return None

    if trace == 'single':
        pid = 0
    else: # pragma: no cover
        pid = os.getpid()

    name = 'relstorage-trace-' + prefix + '.' + str(pid) + '.trace'

    parent_dir = _normalize_path(options)
    try:
        os.makedirs(parent_dir)
    except os.error:
        pass
    fname = os.path.join(parent_dir, name)
    try:
        tf = open(fname, 'ab')
    except IOError as e: # pragma: no cover
        log.warning("Cannot write tracefile %r (%s)", fname, e)
        tf = None
    else:
        log.info("opened tracefile %r", fname)
    return tf

class Connection(sqlite3.Connection):
    # pylint:disable=assigning-non-slot
    # Something about inheriting from an extension
    # class seems to get pylint confused.

    __slots__ = (
        'rs_db_filename',
        'rs_close_async',
        '_rs_has_closed',
    )

    def __init__(self, *args, **kwargs):
        __traceback_info__ = args, kwargs
        super(Connection, self).__init__(*args, **kwargs)

        self.rs_db_filename = None
        self.rs_close_async = DEFAULT_CLOSE_ASYNC
        self._rs_has_closed = False

    def __repr__(self):
        if not self.rs_db_filename:
            return super(Connection, self).__repr__()
        return '<Connection at %x to %r>' % (
            id(self), self.rs_db_filename
        )

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
        # This can be slow, and it releases the GIL, so do that in another thread
        if self._rs_has_closed: # pragma: no cover
            return
        self._rs_has_closed = True
        from relstorage._util import spawn as _spawn
        spawn = _spawn if self.rs_close_async else lambda f: f()
        def optimize_and_close():
            # Recommended best practice is to OPTIMIZE the database for
            # each closed connection. OPTIMIZE needs to run in each connection
            # so it can see what tables and indexes were used. It's usually fast,
            # but has the potential to be slow, so let it happen in the background.
            try:
                self.execute("PRAGMA optimize")
            except sqlite3.DatabaseError:
                # It's possible the file was removed.
                logger.exception("Failed to optimize database; was it removed?")

            super(Connection, self).close()
        spawn(optimize_and_close)


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
    cur.fetchall()

def _execute_pragmas(cur, **kwargs):
    logger.info("Connected to sqlite3 version %s", sqlite3.sqlite_version)
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
            logger.debug(
                "Original %s = %s. Desired %s = %s. Updated %s = %s",
                k, orig_value,
                k, v,
                k, new_value)
        else:
            logger.debug("Using %s = %s", k, orig_value)

_MB = 1024 * 1024
DEFAULT_MAX_WAL = 10 * _MB
DEFAULT_CLOSE_ASYNC = False
# Benchmarking on at least one system doesn't show an improvement to
# either reading or writing by forcing a large mmap_size.
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

def _connect_to_file(fname, factory=Connection,
                     close_async=DEFAULT_CLOSE_ASYNC,
                     timeout=DEFAULT_TIMEOUT,
                     pragmas=None):

    connection = sqlite3.connect(
        fname,
        # If we do nothing, this means we're in autocommit
        # mode. Creating our own transactions with BEGIN
        # disables that until the COMMIT.
        isolation_level=None,
        factory=factory,
        # We explicitly push closing off to a new thread.
        check_same_thread=False,
        timeout=timeout)
    if isinstance(connection, Connection):
        connection.rs_db_filename = fname
        connection.rs_close_async = close_async

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
    pragmas.setdefault('journal_mode', 'wal')
    cur = connection.cursor()
    __traceback_info__ = fname, cur, pragmas
    try:
        _execute_pragmas(cur, **pragmas)
    except:
        logger.exception("Failed to execute pragmas")
        cur.close()
        if hasattr(connection, 'rs_close_async'):
            connection.rs_close_async = False
            connection.close()
        raise

    cur.close()

    return connection

def sqlite_files(options, prefix):
    """
    Calculate the sqlite filename and return it, plus a function that will
    destroy the sqlite file.
    """
    parent_dir = getattr(options, 'cache_local_dir', options)
    # Allow for memory and temporary databases (empty string):
    if parent_dir != ':memory:' and parent_dir:
        parent_dir = _normalize_path(options)
        try:
            # make it if needed. try to avoid a time-of-use/check
            # race (not that it matters here)
            os.makedirs(parent_dir)
        except os.error:
            pass

        fname = os.path.join(parent_dir, 'relstorage-cache-' + prefix + '.sqlite3')
        wal_fname = fname + '-wal'
        shm_fname = fname + '-shm'
        def real_destroy():
            logger.info("Replacing any existing cache at %s", fname)
            __quiet_remove(fname)
            __quiet_remove(wal_fname)
            __quiet_remove(shm_fname)
        destroy = real_destroy
    else:
        fname = parent_dir
        wal_fname = None
        def noop_destroy():
            "Nothing to do."
        destroy = noop_destroy
    return fname, destroy


class Sqlite3TooOldError(ValueError):
    """Raised if the sqlite3 module is too old."""

    def __init__(self):
        super(Sqlite3TooOldError, self).__init__(
            "Unable to use sqlite; minimum version is %s but this version is %s" % (
                SQ3_MIN_VERSION, sqlite3.sqlite_version_info
            ))

CORRUPT_DB_EXCEPTIONS = (sqlite3.DatabaseError,)
FAILURE_TO_OPEN_DB_EXCEPTIONS = (sqlite3.OperationalError, Sqlite3TooOldError)


def sqlite_connect(options, prefix,
                   overwrite=False,
                   max_wal_size=DEFAULT_MAX_WAL,
                   close_async=DEFAULT_CLOSE_ASYNC,
                   mmap_size=DEFAULT_MMAP_SIZE,
                   page_size=DEFAULT_PAGE_SIZE,
                   temp_store=DEFAULT_TEMP_STORE,
                   timeout=DEFAULT_TIMEOUT):
    """
    Return a DB-API Connection object.

    .. caution:: Using the connection as a context manager does **not**
       result in the connection being closed, only committed or rolled back.
    """

    if not SQ3_IS_MIN_VERSION: # pragma: no cover
        raise Sqlite3TooOldError()

    fname, destroy = sqlite_files(options, prefix)

    corrupt_db_ex = CORRUPT_DB_EXCEPTIONS
    if overwrite:
        destroy()
        corrupt_db_ex = ()

    pragmas = {
        # WAL mode can actually be a bit slower at commit time,
        # but buys us better concurrency.
        # Note: In-memory databases always use 'memory' as the journal mode;
        # temporary databases always use 'delete'.
        'journal_mode': 'wal',
        'mmap_size': mmap_size,
        'page_size': page_size,
        'temp_store': temp_store,
        # Eliminate as much checkpoint disk IO as we can. We're just
        # a cache, not a primary source of truth.
        'synchronous': 'OFF',
        # Disable auto-checkpoint so that commits have
        # reliable duration; after commit, if it's a good time,
        # we can run 'PRAGMA wal_checkpoint'. (In most cases, the last
        # database connection that's open will essentially do that
        # automatically.)
        # XXX: Is that really worth it? We'll just begin by increasing
        # it
        # 'wal_autocheckpoint': 0,
        'wal_autocheckpoint': max_wal_size // page_size,
        'threads': 2,
        # Things to query and report.
        'soft_heap_limit': None,
        # The default of -2000 is 2000 pages. At 4K page size,
        # that's 8MB.
        'cache_size': None,
    }

    try:
        connection = _connect_to_file(
            fname,
            close_async=close_async,
            pragmas=pragmas,
            timeout=timeout,
        )
    except corrupt_db_ex as e:
        __traceback_info__ = e, fname, destroy
        logger.exception("Corrupt cache database at %s; replacing", fname)
        destroy()
        connection = _connect_to_file(fname, close_async=close_async,
                                      pragmas=pragmas)

    return connection


def __quiet_remove(path):
    try:
        os.unlink(path)
    except os.error as e:
        # TODO: Use FileNotFoundError on Python 3
        log_meth = log.exception
        if e.errno == errno.ENOENT:
            log_meth = log.debug
        log_meth("Failed to remove %r", path)
        return False
    else:
        return True
