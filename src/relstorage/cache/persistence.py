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
import os
import os.path

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


def sqlite_connect(options, prefix, overwrite=False):
    # Dump the file.
    parent_dir = _normalize_path(options)
    try:
        # make it if needed. try to avoid a time-of-use/check
        # race (not that it matters here)
        os.makedirs(parent_dir)
    except os.error:
        pass

    fname = os.path.join(parent_dir, 'relstorage-cache-' + prefix + '.sqlite3')
    if overwrite:
        __quiet_remove(fname)

    import sqlite3
    connection = sqlite3.connect(
        fname,
        # We'll manage transactions, thank you.
        isolation_level=None,
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

    # WAL mode can actually be a bit slower at commit time,
    # but buys us better concurrency.
    cur = connection.execute('PRAGMA journal_mode = WAL')
    mode = cur.fetchall()[0][0]
    if mode != 'wal':
        raise ValueError("Couldn't set WAL mode")

    connection.execute("PRAGMA synchronous = OFF")
    return connection, fname

def __quiet_remove(path):
    try:
        os.unlink(path)
    except os.error: # pragma: no cover
        log.debug("Failed to remove %r", path)
        return False
    else:
        return True
