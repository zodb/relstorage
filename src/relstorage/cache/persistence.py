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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
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
        from relstorage._compat import spawn
        def c():
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
        spawn(c)


def _connect_to_file(fname, factory=Connection):

    connection = sqlite3.connect(
        fname,
        # We'll manage transactions, thank you.
        isolation_level=None,
        factory=factory,
        check_same_thread=False,
        timeout=10)

    if str is bytes:
        # We don't use the TEXT type, but even so
        # sqlite complains:
        #
        # ProgrammingError: You must not use 8-bit bytestrings unless
        # you use a text_factory that can interpret 8-bit bytestrings
        # (like text_factory = str). It is highly recommended that you
        # instead just switch your application to Unicode strings.
        connection.text_factory = str

    return connection

def sqlite_connect(options, prefix, overwrite=False, max_wal=10*1024*1024):
    """
    Return a DB-API Connection object.

    .. caution:: Using the connection as a context manager does **not**
       result in the connection being closed, only committed or rolled back.
    """
    parent_dir = _normalize_path(options)
    try:
        # make it if needed. try to avoid a time-of-use/check
        # race (not that it matters here)
        os.makedirs(parent_dir)
    except os.error:
        pass

    fname = os.path.join(parent_dir, 'relstorage-cache-' + prefix + '.sqlite3')
    wal_fname = fname + '-wal'
    if overwrite:
        logger.info("Replacing any existing cache at %s", fname)
        __quiet_remove(fname)
        __quiet_remove(wal_fname)

    connection = _connect_to_file(fname)
    # WAL mode can actually be a bit slower at commit time,
    # but buys us better concurrency.
    try:
        cur = connection.execute('PRAGMA journal_mode = WAL')
    except sqlite3.DatabaseError:
        if overwrite:
            raise
        logger.info("Corrupt cache database at %s; replacing", fname)
        connection.close()
        return sqlite_connect(options, prefix, True)

    mode = cur.fetchall()[0][0]
    if mode != 'wal':
        raise ValueError("Couldn't set WAL mode")

    cur.execute("PRAGMA synchronous = OFF")
    # Disable auto-checkpoint so that commits have
    # reliable duration; after commit, if it's a good time,
    # we can run 'PRAGMA wal_checkpoint'. (In most cases, the last
    # database connection that's open will essentially do that
    # automatically.)
    cur.execute("PRAGMA wal_autocheckpoint = 0")
    # In fact, now might be a good time to checkpoint. If we're opening the
    # DB, we could be around for awhile. Doing the checkpoint will
    # release the GIL, so it's good to do in another thread.
    # (If we're monkey-patched by gevent, do so in a gevent worker)
    if (not overwrite
            and os.path.exists(wal_fname)
            and os.stat(wal_fname).st_size >= max_wal):
        # TODO: Explicit test for this condition.
        def checkpoint():
            conn = _connect_to_file(fname, factory=sqlite3.Connection)
            begin = time.time()
            cur = conn.execute("PRAGMA wal_checkpoint(RESTART)")
            ok, modified_pages, number_moved_to_db = cur.fetchone()
            cur.execute("PRAGMA optimize")
            end = time.time()
            logger.info(
                "Checkpointed WAL database %s in %s. "
                "Busy? %s Modified pages: %d, moved pages: %d",
                fname, end - begin, ok == 0, modified_pages, number_moved_to_db
            )
            conn.close()

        from relstorage._compat import spawn
        spawn(checkpoint)


    cur.close()
    return connection, fname

def __quiet_remove(path):
    try:
        os.unlink(path)
    except os.error: # pragma: no cover
        log.debug("Failed to remove %r", path)
        return False
    else:
        return True
