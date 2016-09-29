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
from __future__ import absolute_import, print_function, division

import logging

import glob
import gzip
import io
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
    path = _normalize_path(options)
    possible_caches = glob.glob(os.path.join(path, 'relstorage-cache-'
                                             + prefix
                                             + '.*'
                                             + _gzip_ext(options)))
    return possible_caches


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
    fds = []
    stats = []
    try:
        for possible_cache_path in _list_cache_files(options, prefix):
            cache_file = _open(options, possible_cache_path, 'rb')
            fds.append(cache_file)
            buf_cache_file = _gzip_file(options, possible_cache_path, fileobj=cache_file, mode='rb')
            stats.append((os.fstat(cache_file.fileno()), buf_cache_file, possible_cache_path, cache_file))
    except: # pragma: no cover
        for _f in fds:
            _f.close()
        raise

    # Newest and biggest first
    stats.sort(key=lambda s: (s[0].st_mtime, s[0].st_size), reverse=True)

    return stats


def count_cache_files(options, prefix):
    return len(_list_cache_files(options, prefix))


def load_local_cache(options, prefix, local_client_bucket):
    # Given an options that points to a local cache dir,
    # choose a file from that directory and load it.
    stats = _stat_cache_files(options, prefix)
    if not stats:
        log.debug("No cache files found")
    max_load = options.cache_local_dir_read_count or len(stats)
    loaded_count = 0
    try:
        for _, fd, cache_path, _ in stats:
            if loaded_count >= max_load:
                break

            try:
                _, stored = local_client_bucket.read_from_stream(fd)
                loaded_count += 1
                if not stored or local_client_bucket.size >= local_client_bucket.limit:
                    break # pragma: no cover
            except (NameError, AttributeError): # pragma: no cover
                # Programming errors, need to be caught in testing
                raise
            except: # pylint:disable=bare-except
                log.exception("Invalid cache file %s", cache_path)
                fd.close()
                os.remove(cache_path)
    finally:
        for e in stats:
            e[1].close()
            e[3].close()
    return loaded_count


def save_local_cache(options, prefix, persistent_cache, _pid=None):
    # Dump the file.
    tempdir = _normalize_path(options)
    try:
        # make it if needed. try to avoid a time-of-use/check
        # race (not that it matters here)
        os.makedirs(tempdir)
    except os.error:
        pass

    fd, path = tempfile.mkstemp('._rscache_', dir=tempdir)
    with _open(options, fd, 'wb') as f:
        with _gzip_file(options, filename=path, fileobj=f, mode='wb', compresslevel=5) as fz:
            try:
                persistent_cache.write_to_stream(fz)
            except (NameError, AttributeError): # pragma: no cover
                # Programming errors, need to be caught in testing
                raise
            except:
                log.exception("Failed to save cache file %s", path)
                fz.close()
                f.close()
                os.remove(path)
                return

    # Ok, now pick a place to put it, dropping the oldest file,
    # if necessary.

    files = _list_cache_files(options, prefix)
    if len(files) < options.cache_local_dir_count:
        pid = _pid or os.getpid() # allowing passing for testing
        # Odds of same pid existing already are too low to worry about
        new_name = 'relstorage-cache-' + prefix + '.' + str(pid) + _gzip_ext(options)
        new_path = os.path.join(tempdir, new_name)
        os.rename(path, new_path)
    else:
        stats = _stat_cache_files(options, prefix)
        # oldest and smallest first
        stats.reverse()
        try:
            stats[0][1].close()
            new_path = stats[0][2]
            os.rename(path, new_path)
        finally:
            for e in stats:
                e[1].close()
                e[3].close()

    try:
        f = persistent_cache.get_cache_modification_time_for_stream
    except AttributeError:
        mod_time = None
    else:
        mod_time = f()

    if mod_time and mod_time > 0:
        # PyPy on Linux raises an OSError/Errno22 if the mod_time is less than 0
        # and is a float
        logger.debug("Setting date of %r to cache time %s (current time %s)",
                     new_path, mod_time, time.time())
        os.utime(new_path, (mod_time, mod_time))

    return new_path
