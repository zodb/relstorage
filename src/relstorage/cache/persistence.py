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


from relstorage.adapters.sqlite.drivers import Sqlite3Driver
from relstorage.adapters.sqlite.dialect import SQ3_SUPPORTS_CTE

# Because we use a CTE in our default queries. In principle,
# we could probably re-write queries and run on 3.7.11; 3.7.0 was the
# oldest to support WAL mode, and 3.7.11 added multiple VALUE syntax.
# Prior to 3.7.6, you can't use LENGTH() on a BLOB column.
SQ3_IS_MIN_VERSION = SQ3_SUPPORTS_CTE

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

        fname = os.path.join(parent_dir, 'relstorage-cache2-' + prefix + '.sqlite3')
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

    def __init__(self, *_args):
        super(Sqlite3TooOldError, self).__init__(
            "Unable to use sqlite; minimum version is %s but this version is %s" % (
                "3.8.3", sqlite3.sqlite_version_info
            ))


class _Driver(Sqlite3Driver):
    STATIC_AVAILABLE = SQ3_SUPPORTS_CTE
    DriverNotAvailableError = Sqlite3TooOldError

CORRUPT_DB_EXCEPTIONS = (sqlite3.DatabaseError,)
FAILURE_TO_OPEN_DB_EXCEPTIONS = (sqlite3.OperationalError, Sqlite3TooOldError)


def sqlite_connect(options, prefix,
                   overwrite=False,
                   **connect_kwargs):
    """
    Return a DB-API Connection object.

    If the database is corrupted, the file will be removed and recreated.

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

    connect_args = (fname,)

    try:
        connection = _Driver().connect_to_file(
            *connect_args,
            **connect_kwargs
        )
    except corrupt_db_ex as e:
        __traceback_info__ = e, fname, destroy
        logger.exception("Corrupt cache database at %s; replacing", fname)
        destroy()
        connection = _Driver().connect_to_file(
            *connect_args,
            **connect_kwargs
        )

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
