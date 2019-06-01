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

import glob
import gzip
import io
import logging
import os
import os.path
import tempfile
import time

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


def _open(_options, filename, *args):
    return io.open(filename, *args, buffering=16384)


def _gzip_ext(options):
    if options.cache_local_dir_compress:
        return ".rscache.gz"
    return ".rscache"


def _gzip_file(options, filename, fileobj, **kwargs):
    if not options.cache_local_dir_compress:
        return fileobj
    # These files would *appear* to be extremely compressable. One
    # zodbshootout example with random data compressed a 3.4MB
    # file to 393K and a 19M file went to 3M.

    # As far as speed goes: for writing a 512MB file containing
    # 650,987 values with only 1950 distinct values (so
    # potentially highly compressible, although none of the
    # identical items were next to each other in the dict on
    # purpose) of random data under Python 2.7:

    # no GzipFile is                   8s
    # GzipFile with compresslevel=0 is 11s
    # GzipFile with compresslevel=5 is 28s (NOTE: Time was the same for Python 3)

    # But the on disk size at compresslevel=5 was 526,510,662
    # compared to the in-memory size of 524,287,388 (remembering
    # there is more overhead on disk). So its hardly worth it.

    # Under Python 2.7, buffering is *critical* for performance.
    # Python 3 doesn't have this problem as much for reads, but it's nice to still do.

    # For writing, the fileobj itself must be buffered; this is
    # taken care of by passing objects obtained from io.open; without
    # that low-level BufferdWriter, what is 10s to write 512MB in 600K objects
    # becomes 40s.

    gz_cache_file = gzip.GzipFile(filename, fileobj=fileobj, **kwargs)
    if kwargs.get('mode') == 'rb':
        # For reading, 2.7 without buffering 100,000 objects from a
        # 2MB file takes 4 seconds; with it, it takes around 1.3.
        return io.BufferedReader(gz_cache_file)

    return gz_cache_file


def _list_cache_files(options, prefix):
    "Returns a list of absolute paths"
    path = _normalize_path(options)
    possible_caches = glob.glob(os.path.join(path, 'relstorage-cache-'
                                             + prefix
                                             + '.*'
                                             + _gzip_ext(options)))
    return [os.path.abspath(x) for x in possible_caches]


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


def _stat_cache_files(options, prefix):
    """
    Return a list of cache file names,
    sorted so that the newest and largest files are first.
    """
    stats = []
    for possible_cache_path in _list_cache_files(options, prefix):
        try:
            stats.append((os.stat(possible_cache_path), possible_cache_path))
        except os.error: # pragma: no cover
            # file must be gone, probably we're cleaning things out
            pass

    # Newest and biggest first; tie breaker of the filename.
    #
    # Note that the mtime comes from what the cache tells us about the
    # data, and if the database is not changing (especially likely in
    # synthetic benchmarks), multiple cache files could have that same
    # mtime, so the second candidate is the newness of the file itself
    # (the newer the file, the more likely it is to have a bigger collection of
    # data). We might need to move some knowledge about cache contents (number of keys,
    # etc) down to this level to fully exploit that.
    stats.sort(key=lambda s: (s[0].st_mtime, s[0].st_ctime, s[0].st_size, s[1]), reverse=True)
    return [s[1] for s in stats]


def count_cache_files(options, prefix):
    return len(_list_cache_files(options, prefix))

def _connect(options, prefix, force=False):
    # Dump the file.
    parent_dir = _normalize_path(options)
    try:
        # make it if needed. try to avoid a time-of-use/check
        # race (not that it matters here)
        os.makedirs(parent_dir)
    except os.error:
        pass

    fname = os.path.join(parent_dir, 'relstorage-cache-' + prefix + '.sqlite3')
    if force:
        __quiet_remove(fname)

    import sqlite3
    connection = sqlite3.connect(
        fname,
        isolation_level=None)

    # WAL mode can actually be a bit slower at commit time,
    # but buys us better concurrency.
    cur = connection.execute('PRAGMA journal_mode = WAL')
    mode = cur.fetchall()[0][0]
    if mode != 'wal':
        raise ValueError("Couldn't set WAL mode")

    connection.execute("PRAGMA synchronous = OFF")
    return connection

def load_local_cache(options, prefix, local_client_bucket):
    connection = _connect(options, prefix)
    local_client_bucket.read_from_sqlite(connection)
    return 1

    # Given an options that points to a local cache dir,
    # choose a file from that directory and load it.
    stats = _stat_cache_files(options, prefix)
    if not stats:
        log.debug("No cache files found")

    max_load = options.cache_local_dir_read_count or len(stats)
    loaded_count = 0
    log.debug("Possible cache files: %s", stats)
    for cache_path in stats:
        if loaded_count >= max_load:
            break

        try:
            with _open(options, cache_path, 'rb') as raw_cache_file:
                with _gzip_file(options, cache_path, fileobj=raw_cache_file, mode='rb') as gzf:
                    _, stored = local_client_bucket.read_from_stream(gzf)
                    loaded_count += 1
                    if not stored or local_client_bucket.size >= local_client_bucket.limit:
                        break # pragma: no cover
        except (NameError, AttributeError, TypeError, ValueError): # pragma: no cover
            # Programming errors, need to be caught in testing
            raise
        except Exception: # pylint:disable=broad-except
            log.exception("Invalid cache file %r", cache_path)
            __quiet_remove(cache_path)
    return loaded_count

def __write_temp_cache_file(options, prefix, parent_dir, persistent_cache):
    prefix = 'relstorage-cache-' + prefix + '.'
    suffix = _gzip_ext(options) + '.T'
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=parent_dir)
    os.close(fd)
    # Re-open by the path to get a readable filename.
    try:
        log.debug("Writing cache file %s", path)
        with _open(options, path, 'wb') as f:
            with _gzip_file(options, filename=path, fileobj=f, mode='wb', compresslevel=5) as fz:
                persistent_cache.write_to_stream(fz)
    except:
        __quiet_remove(path)
        raise
    else:
        return path

def __set_mod_time(new_path, persistent_cache):
    try:
        f = persistent_cache.get_cache_modification_time_for_stream
    except AttributeError:
        mod_time = None
    else:
        mod_time = f()

    if mod_time and mod_time > 0:
        # Older PyPy on Linux raises an OSError/Errno22 if the mod_time is less than 0
        # and is a float
        # (https://bitbucket.org/pypy/pypy/issues/2408/cpython-difference-osutime-path-11-11)
        logger.debug("Setting date of %r to cache time %s (current time %s)",
                     new_path, mod_time, time.time())
        try:
            os.utime(new_path, (mod_time, mod_time))
        except IOError:
            # Under some concurrent scenarios, we've seen this
            # raise FileNotFound.
            __quiet_remove(new_path)

def __quiet_remove(path):
    try:
        os.unlink(path)
    except os.error: # pragma: no cover
        log.debug("Failed to remove %r", path)
        return False
    else:
        return True

def save_local_cache(options, prefix, persistent_cache, force=False):
    connection = _connect(options, prefix, force=force)

    persistent_cache.write_to_sqlite(connection)
    connection.execute('PRAGMA optimize')
    return

    try:
        path = __write_temp_cache_file(options, prefix, parent_dir, persistent_cache)
    except (NameError, AttributeError, TypeError, ValueError): # pragma: no cover
        # programming errors that should be caught in testing
        raise
    except Exception: # pylint:disable=broad-except
        log.exception("Failed to save cache file %s", persistent_cache)
        return

    # Ok, now pick a place to put it, dropping the oldest file,
    # if necessary. Do this with a lock, because apparently there have
    # been some race conditions

    # Now assign our permanent name by stripping the tmp suffix and renaming
    assert path.endswith(".T")
    new_path = path[:-2]
    __set_mod_time(path, persistent_cache)

    try:
        os.rename(path, new_path)
    except os.error: # pragma: no cover
        log.exception("Failed to rename %r to %r; not doing maintenance", path, new_path)
        __quiet_remove(path)
        raise

    grace_period = time.time() - 30

    del path
    # No lock
    __set_mod_time(new_path, persistent_cache)

    # Now remove any extra (old, small) files if we have too many
    # If there are multiple storages shutting down, they will race
    # each other to do this.
    stats = _stat_cache_files(options, prefix)
    while len(stats) > options.cache_local_dir_count and len(stats) > 1:
        oldest_file = stats[-1]
        try:
            if os.stat(oldest_file).st_ctime > grace_period:
                # If the oldest file is only a few minutes old,
                # we're cycling too fast. Let it be.
                break
        except IOError:
            break

        # It's possible but unlikely for two processes to write to disk within the limit
        # of filesystem modification time tracking. If one of those processes
        # was us, then we still have to pick a loser.
        log.debug("Removing expired cache file %s", oldest_file)
        if not __quiet_remove(oldest_file):
            # One process will succeed, all the others will fail
            log.info("Failed to prune file %r; stopping", oldest_file)
            break

        stats = _stat_cache_files(options, prefix)

    return new_path
