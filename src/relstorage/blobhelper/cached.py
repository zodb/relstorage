##############################################################################
#
# Copyright (c) 2009,2019 Zope Foundation and Contributors.
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
from __future__ import print_function
from __future__ import division

import os
import re
import time

from binascii import hexlify

from BTrees import OOBTree # pylint:disable=no-name-in-module
import zc.lockfile

import ZODB.blob
from zope.interface import implementer

from relstorage._util import byte_display
from relstorage._util import spawn as native_thread_spawn
from relstorage._util import thread_spawn
from relstorage._util import timer
from relstorage._util import bytes8_to_int64
from relstorage.interfaces import POSKeyError

from .interfaces import ICachedBlobHelper
from .abstract import AbstractBlobHelper
from .util import lock_blob


logger = __import__('logging').getLogger(__name__)


class _AbstractCacheSizeMonitor(object):
    __slots__ = ()

    def __init__(self, options):
        """Does nothing."""

    def close(self):
        """
        Finish any processing and free any resources used by this monitor.
        """
        raise NotImplementedError

    def loaded(self, byte_count):
        """
        Let the monitor know that *byte_count* bytes of data have been
        added to the cache.
        """
        raise NotImplementedError

class _UnlimitedCacheSizeMonitor(_AbstractCacheSizeMonitor):
    """
    Use this when no limit has been configured.
    """
    __slots__ = ()

    def close(self):
        """Does nothing."""

    def loaded(self, byte_count):
        """Does nothing."""


class _LimitedCacheSizeMonitor(_AbstractCacheSizeMonitor):
    """
    Control the size of the blob cache. This object is shared
    between BlobHelpers, so it needs to be thread safe.
    """

    __slots__ = (
        'blob_dir',
        'blob_cache_max_size',
        'blob_cache_target_cleanup_size',
        'bytes_loaded_since_last_check',
        'bytes_loaded_check_threshold',
        '_lock',
        '_checker_thread',
        '_reduced_event',
        '_exceeded_counter',
    )

    def __init__(self, options):
        import threading

        assert options.blob_cache_size_check < 100

        super(_LimitedCacheSizeMonitor, self).__init__(options)

        self.blob_dir = options.blob_dir
        self.blob_cache_max_size = options.blob_cache_size
        self.bytes_loaded_check_threshold = (
            self.blob_cache_max_size * options.blob_cache_size_check / 100.0
        )

        self.blob_cache_target_cleanup_size = max(
            self.blob_cache_max_size - self.bytes_loaded_check_threshold,
            0
        )
        self.bytes_loaded_since_last_check = 0
        # thread-safe according to whatever threading system is in use.
        # We *want* mankey patches here.
        self._lock = threading.Lock()
        self._reduced_event = threading.Event()
        self._checker_thread = None
        self._exceeded_counter = 0
        self._check()

    def close(self):
        try:
            if self._checker_thread is not None:
                self.wait_for_checker()
        except Exception: # pragma: no cover
            logger.exception("When waiting on the checker thread %r",
                             self._checker_thread)
            raise
        finally:
            self._checker_thread = None

    def loaded(self, byte_count):
        with self._lock:
            self.bytes_loaded_since_last_check += byte_count
            if self.bytes_loaded_since_last_check >= self.bytes_loaded_check_threshold:
                logger.debug(
                    "Loaded %s bytes (>= %s) into %s, may need to check.",
                    byte_display(self.bytes_loaded_since_last_check),
                    byte_display(self.bytes_loaded_check_threshold),
                    self.blob_dir
                )
                self._check()

    def wait_for_checker(self):
        """For tests only."""
        # Efficiently wait only if a checker thread is running.
        # It's native, a threading.Event() might not be and we'd need an
        # asyncwatcher to communicate among threads. The tests that use us patch
        # to be sure we use matching events.
        with self._lock:
            if self._checker_thread is None:
                return
        self._reduced_event.wait()

    def _check(self):
        """
        Run blob cache cleanup in another thread if needed.

        Must be called with our lock held (or a guarantee that we're
        single threaded.)
        """
        on_init = self.bytes_loaded_since_last_check == 0
        self.bytes_loaded_since_last_check = 0
        self._reduced_event.clear()
        self._exceeded_counter += 1
        checker_thread = self._checker_thread
        if checker_thread is not None and not checker_thread.ready():
            # One running still.
            logger.debug("Checker %s still running, not spawning for %s",
                         self._checker_thread,
                         self.blob_dir)
            return

        # Only spawn a new one if there's not one running.
        # This gets set back to None as part of the cleanup callback.
        logger.info(
            "Spawning cache checker for %s (%s)",
            self.blob_dir,
            "creating storage" if on_init else "exceeded threshold"
        )
        self._exceeded_counter = 0
        self._checker_thread = self._spawn()

    def _spawn(self):
        checker = _BlobCacheSizeChecker(
            self.blob_dir, self.blob_cache_target_cleanup_size, self._when_done
        )
        return native_thread_spawn(checker)

    def _when_done(self, checker, holding_clean_lock):
        """
        Callback to be run from the cleanup thread.

        Cleans up the state of *self*.
        """
        with self._lock:
            # checker is the BlobCacheSizeChecker, but self._checker
            # is the spawned thread.
            # This is the last thing the BlobCacheSizeChecker does, so by
            # definition it cannot be ready yet.
            self._checker_thread = None
            if not holding_clean_lock:
                self._reduced_event.set()
                return

            # In principle, if the checker finished sizing the directory and got
            # a cache size under its target and wanted to return to us,
            # but then some other threads immediately loaded a bunch of blobs,
            # we could go over that size. We prevent this by checking
            # the size again here, while we're holding our lock, and if we're
            # too big, we'll go again. This happens during the test cases.
            # checker can be None if we did this externally.
            dir_size = -1
            duration = 0.0
            if checker is not None:
                dir_size = checker.blob_dir_size
                duration = checker.duration

            logger.info(
                "Finished checking %s (in %.3fs) with size of %s (max: %s; target %s)",
                self.blob_dir, duration,
                byte_display(dir_size),
                byte_display(self.blob_cache_max_size),
                byte_display(self.blob_cache_target_cleanup_size)
            )
            if self._exceeded_counter or dir_size > self.blob_cache_target_cleanup_size:
                logger.debug(
                    "Requesting new check for %s with size of %s (max: %s; target %s)",
                    self.blob_dir,
                    dir_size,
                    self.blob_cache_max_size,
                    byte_display(self.blob_cache_target_cleanup_size)
                )
                self._check()
            else:
                self._reduced_event.set()


class _ExternalLimitedCacheSizeMonitor(_LimitedCacheSizeMonitor):
    """
    Uses a subprocess instead of a thread.
    """
    __slots__ = ()

    @staticmethod
    def __run_checker(blob_dir, blob_cache_target_cleanup_size, when_done):
        import subprocess
        import sys
        try:
            popen = subprocess.Popen([
                sys.executable,
                "-m",
                __name__,
                blob_dir,
                str(int(blob_cache_target_cleanup_size))
            ])

            popen.wait()
        finally:
            # Pretend that we held the check lock, even if we're not
            # sure that we did. If we need to go again because we've already
            # stored that much more data, that lets us schedule the process.
            # TODO: Make the external process write something we can read,
            # or use the exit code to determine if it had the lock.
            when_done(None, True)
            when_done = None

    def _spawn(self):
        # We need to call our _when_done handler so that tests can
        # determine when to look at the directory. This does not need
        # to be a native thread under gevent, a greenlet thread is
        # perfect (native threads in the threadpool can be limited),
        # but we do need to wait on the popen object.
        return thread_spawn(self.__run_checker,
                            args=(self.blob_dir,
                                  self.blob_cache_target_cleanup_size,
                                  self._when_done))


@implementer(ICachedBlobHelper)
class CacheBlobHelper(AbstractBlobHelper):

    NEEDS_DB_LOCK_TO_VOTE = False
    NEEDS_DB_LOCK_TO_FINISH = False

    def __init__(self, options, adapter, fshelper=None, cache_checker=None):
        assert not options.shared_blob_dir

        if fshelper is None:
            # The blob directory is a cache of the blobs
            if _BlobCacheLayout.LAYOUT_NAME not in ZODB.blob.LAYOUTS:
                ZODB.blob.LAYOUTS[_BlobCacheLayout.LAYOUT_NAME] = _BlobCacheLayout()
            fshelper = ZODB.blob.FilesystemHelper(
                options.blob_dir, layout_name=_BlobCacheLayout.LAYOUT_NAME)
            fshelper.create()

        super(CacheBlobHelper, self).__init__(options, adapter, fshelper)

        # All blob helpers for all instances of this storage share the
        # same cache_checker object.
        if cache_checker is None:
            factory = _UnlimitedCacheSizeMonitor
            if options.blob_cache_size:
                factory = _LimitedCacheSizeMonitor
                if options.blob_cache_size_check_external:
                    factory = _ExternalLimitedCacheSizeMonitor
            cache_checker = factory(options)

        self.cache_checker = cache_checker
        self.new_instance_kwargs['cache_checker'] = self.cache_checker

    def close(self):
        super(CacheBlobHelper, self).close()
        try:
            self.cache_checker.close()
        except Exception: # pragma: no cover pylint:disable=broad-except
            # On CI tests, we've rarely seen this raise
            # gevent.exceptions.LoopExit from
            # ``self._reduced_event.wait()``. That probably means
            # another exception already occurred and we're in teardown.
            # We don't want to raise this, there's no corrective action that
            # can be taken.
            logger.exception("When shutting down the cache_checker %r",
                             self.cache_checker)

    def _loadBlobInternal(self, cursor, oid, serial, blob_lock=None):
        blob_filename = self._cachedLoadBlobInternal(oid, serial)
        if not blob_filename:
            # OK, it's not on disk in our cache. We need to lock and
            # download. In order to lock, we need to create the directory
            # first.
            blob_filename = self._get_lockable_blob_filename(oid, serial)
            my_lock = lock_blob(blob_filename) if blob_lock is None else blob_lock
            try:
                blob_filename = self._loadBlobLocked(cursor, oid, serial, blob_filename)
            finally:
                if blob_lock is None:
                    # If we take out the lock, we close the lock.
                    # Otherwise, it's the caller's responsibility.
                    my_lock.close()
        return blob_filename

    def _loadBlobLocked(self, cursor, oid, serial, blob_filename):
        """
        Returns a filename that exists on disk, or raises a POSKeyError.
        """
        # OK, it's not here and we (or someone) needs to get it. We
        # want to avoid getting it multiple times. We want to avoid
        # getting it multiple times even accross separate client
        # processes on the same machine. We'll use file locking.
        # (accomplished by our caller.)

        # We got the lock, so it's our job to download it. First,
        # we'll double check that someone didn't download it while
        # we were getting the lock:
        if os.path.exists(blob_filename):
            return self._accessed(blob_filename)

        self.download_blob(cursor, oid, serial, blob_filename)

        if os.path.exists(blob_filename):
            return self._accessed(blob_filename)
        __traceback_info__ = cursor
        raise POSKeyError(oid, serial=serial, fn=blob_filename)

    def upload_blob(self, cursor, oid, serial, filename):
        """
        Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        if serial is not None:
            tid_int = bytes8_to_int64(serial)
        else:
            tid_int = None
        self.adapter.mover.upload_blob(cursor, bytes8_to_int64(oid), tid_int, filename)

    def download_blob(self, cursor, oid, serial, filename):
        """Download a blob into a file"""
        tmp_fn = filename + ".tmp"
        bytecount = self.adapter.mover.download_blob(
            cursor, bytes8_to_int64(oid), bytes8_to_int64(serial), tmp_fn)
        if os.path.exists(tmp_fn):
            os.rename(tmp_fn, filename)
        self.cache_checker.loaded(bytecount)

    def storeBlob(self, cursor, store_func,
                  oid, serial, data, blobfilename, version, txn):
        assert not version
        temp_path = self._doStoreBlob(
            store_func,
            oid, serial, data, blobfilename,
            txn
        )
        self.upload_blob(cursor, oid, None, temp_path)

    def restoreBlob(self, cursor, oid, serial, blobfilename):
        # This bypasses ``txn_has_blobs``. That shouldn't cause any problems
        # because ``serial`` should not be none here, so we are avoiding the
        # temp tables. The use of ``txn_has_blobs`` in tpc_vote() is to check if we need
        # to move temporary blobs into place.
        self.upload_blob(cursor, oid, serial, blobfilename)

    def copy_undone(self, copied, tid):
        """
        Not needed in a cache.
        """

    def after_pack(self, oid_int, tid_int):
        """
        Not needed in a cache.

        Although, it might be helpful as a size control?
        """

    def _remove_old_revisions_of_stored_blobs(self, tid, total_size_stored):
        """
        Prune old revisions of blobs that are not in use.

        This is only done if we're not keeping history. This assumes
        the _BlobCacheLayout. (TODO: Generalize so we can do this for
        shared blob dirs? That's slightly more dangerous since it's
        our only copy of the data.)

        Because this could reduce the disk footprint of the cache, it
        takes the total amount of data stored, and returns a modified
        value (less if we removed old revisions).
        """
        if not total_size_stored or self.options.keep_history:
            return total_size_stored

        # If we've added/edited some blobs in this transaction,
        # and we're not keeping history, see if we can clean up
        # some older blobs for the same OIDs. This will help keep
        # our cache size contained.
        blob_suffix = ZODB.blob.BLOB_SUFFIX
        get_dir_for_oid = self.fshelper.getPathForOID # Including the base dir
        get_local_path_for_oid_tid = self.fshelper.layout.getBlobFilePath
        total_size = total_size_stored

        for stored_blob_oid in self._txn_blobs:
            # Chop off the first part of the OID; that's implicit in the full path
            # that we pass to listdir()
            stored_blob_file_path = os.path.split(
                get_local_path_for_oid_tid(stored_blob_oid, tid))[1]
            stored_oid_part, stored_tid_part, _ = stored_blob_file_path.split('.')

            dir_for_oid = get_dir_for_oid(stored_blob_oid)
            all_blob_files = [fname
                              for fname in os.listdir(dir_for_oid)
                              if fname.endswith(blob_suffix)]
            if len(all_blob_files) < 2:
                # Nothing to do if there's only one file.
                continue

            for filename in all_blob_files:
                # TODO: We could save some calls to listdir() if we collected
                # all the OIDs in self._txn_blobs that will wind up sharing
                # a single directory. That would be a lot of blobs in a transaction,
                # though.

                # The "right" way to get an oid and tid from a path is to use
                # fshelper.splitBlobFilename(), but it assumes that the first part of the
                # path component contains the entire OID, whereas here we only have
                # the remainder.
                disk_oid_part, disk_tid_part, _ = filename.split('.')
                if disk_oid_part != stored_oid_part:
                    continue

                filepath = os.path.join(self.blob_dir, dir_for_oid, filename)
                if disk_tid_part < stored_tid_part:
                    # The TID is stored in hex form. The hex form sorts identically to the
                    # byte or integer form, with increasing values meaning newer tids.
                    logger.debug(
                        "Found older cached version of the blob %s at %s; attempting removal.",
                        stored_blob_oid, filepath
                    )
                    # This takes the lock and removes it.
                    total_size -= _BlobCacheSizeChecker.remove_blob_at_path(filepath)

        # It's possible we actually removed more than we stored, if lots of them
        # were still open before.
        return total_size if total_size >= 0 else 0

    def finish(self, tid):
        try:
            total_size = self._move_blobs_into_place(tid)
            # If this slows commit down too much, we could push it to a thread
            # in a few different ways (a queue.Queue consumer, or just spawn())
            total_size = self._remove_old_revisions_of_stored_blobs(tid, total_size)
            self.cache_checker.loaded(total_size)
        except Exception: # pylint:disable=broad-except
            # We're a cache, we can ignore issues moving things into
            # place or cleaning up old revisions. The data is still
            # safe and will be downloaded if needed. Raising exceptions from
            # finish() is bad, so don't do that.
            logger.exception("Failed to properly put blob cache files into place.")
        finally:
            super(CacheBlobHelper, self).finish(tid)


class _BlobCacheLayout(object):
    """
    Uses a two-level directory layout::

        <blob-dir>/<oid1>/<oid2>.<tid>.blob

    For example::

        <blob-dir>/23/0.03d167f919308700.blob

    The ``<oid1>`` (directory name) and ``<oid2>`` (first part of the
    filename) are derived from the OID of the blob when treated as a
    64-bit integer; ``<oid1>`` will only ever contain one or more
    ASCII digits.
    """

    # This layout is a clone of the ZEO.ClientStorage.BlobCacheLayout
    # class. We haven't changed anything about how it is structured,
    # but we *might* in the future; we'd like to change the name, but
    # that would invalidate all existing caches (the layout name is
    # stored in a file on disk and checked when the FilesystemHelper is
    # created).
    #
    # TODO: In particular, even though a history-free storage only has
    # one revision of a blob in the database, we don't consider that
    # when we're caching a blob, or when we're cleaning blobs up. We
    # should be able to do better.
    LAYOUT_NAME = 'zeocache'

    size = 997

    def oid_to_path(self, oid):
        rem = bytes8_to_int64(oid) % self.size
        return str(rem)

    def getBlobFilePath(self, oid, tid):
        base, rem = divmod(bytes8_to_int64(oid), self.size)
        return os.path.join(
            str(rem),
            "%s.%s%s" % (
                base,
                hexlify(tid).decode('ascii'),
                ZODB.blob.BLOB_SUFFIX
            )
        )

class _BlobCacheSizeChecker(timer):

    __slots__ = (
        'blob_dir',
        # The last measured size of the blob directory.
        'blob_dir_size',
        'target_size',
        '_finished_callback',
        # To name a thread
        '__name__',
        'duration',
    )

    def __init__(self, blob_dir, target_size, when_done=lambda _me, _holding_lock: None):
        with open(os.path.join(blob_dir, ZODB.blob.LAYOUT_MARKER)) as layout_file:
            layout = layout_file.read().strip()

        if layout != _BlobCacheLayout.LAYOUT_NAME:
            # Refuse to do anything for the wrong layout. This is especially
            # important since we can be called with `python -m ...` now.
            logger.critical("Invalid blob directory layout %s in %s", layout, blob_dir)
            raise ValueError("Invalid blob directory layout", layout, blob_dir)


        self.blob_dir = blob_dir
        self.target_size = target_size
        self.blob_dir_size = None
        self._finished_callback = when_done
        self.duration = 0.0

        self.__name__ = 'Blob Cache Checker: %s' % (blob_dir,)

    def __acquire_check_lock(self):
        # Returns a lock, or None if we couldn't acquire it.
        blob_dir = self.blob_dir
        lock_path = os.path.join(blob_dir, 'check_size.lock')

        try:
            return zc.lockfile.LockFile(lock_path)
        except zc.lockfile.LockError:
            try:
                time.sleep(1)
                return zc.lockfile.LockFile(lock_path)
            except zc.lockfile.LockError:
                # Someone is already cleaning up, so don't bother
                logger.debug("Another thread is checking the blob cache size.")
                return

    def __size_blob_dir(self, is_cache_dir_name=re.compile(r'\d+$').match):
        # Calculate the sizes of the blobs stored in the blob_dir.
        # Return the total size, and a BTree {atime: [full path to blob file]}

        # TODO: nti.zodb.containers has support for mapping
        # time.time() values into integers for use with the (smaller,
        # faster) IOBTree. Use that if we can prove that we can pop
        # the min atime successfully (that is, while the
        # nti.zodb.containers transformation is lossless and
        # reversible, we need to prove that it also maintains order;
        # I'm not sure it does).
        #
        # Other optimizations: Don't use a list until we get more than one
        # file with a matching atime. And/or use tuples and not lists:
        # tuples aren't tracked by the GC like lists are (after they survive one
        # collection, anyway).

        blob_dir = self.blob_dir
        blob_suffix = ZODB.blob.BLOB_SUFFIX
        files_by_atime = OOBTree.BTree()
        size = 0

        # Use os.walk() instead of os.listdir(); on 3.5+ this is much faster
        # thanks to the use of os.scandir(). When we're on Python 3.5+ *only*
        # we could use os.scandir ourself and maybe save some stat calls?
        for dirpath, dirnames, filenames in os.walk(blob_dir):
            # Walk top-down, only recursing into directories matching the
            # OID components (of which there should be one level)
            dirnames[:] = [d for d in dirnames if is_cache_dir_name(d)]
            # Examine blob files.
            blobfile_paths = [os.path.join(dirpath, f)
                              for f in filenames
                              if f.endswith(blob_suffix)]

            for file_path in blobfile_paths:
                stat = os.stat(file_path)
                size += stat.st_size
                t = stat.st_atime
                if t not in files_by_atime:
                    files_by_atime[t] = []

                # The ZEO version returns a weird version of the path,
                #
                #     os.path.join(dirname, file_name)
                #
                # which it must later re-combine to get an actual path:
                #
                #     os.path.join(blob_dir, file_name)
                #
                # It's not clear why it doesn't return the full path
                # that it already has. Temporary memory savings,
                # perhaps? If so, is that even a concern anymore?
                files_by_atime[t].append(file_path)

        logger.debug("Blob cache size for %s: %s", self.blob_dir, byte_display(size))
        return size, files_by_atime

    @staticmethod
    def remove_blob_at_path(file_path, lock_retries=0):
        """
        Return the size of the blob that was removed, or 0
        if the blob couldn't be removed because it was locked
        or otherwise open (e.g., on Windows).
        """
        try:
            lock = lock_blob(file_path, lock_retries)
        except zc.lockfile.LockError:
            logger.debug("Skipping locked blob %s", file_path)
            return 0  # In use, skip

        try:
            fsize = os.stat(file_path).st_size
            try:
                ZODB.blob.remove_committed(file_path)
            except OSError:
                return 0 # probably open on windows
            else:
                return fsize
        finally:
            lock.close()

    def __shrink_blob_dir(self, current_size, files_by_atime):
        size = current_size
        target_size = self.target_size
        remove = self.remove_blob_at_path

        while size > target_size and files_by_atime:
            for file_path in files_by_atime.pop(files_by_atime.minKey()):
                size -= remove(file_path)
                if size <= target_size:
                    break

        logger.debug("Reduced blob cache size for %s: %s", self.blob_dir, byte_display(size))

    def __call__(self):
        with self:
            logger.info("Checking blob cache size for %s. (target: %s)",
                        self.blob_dir,
                        byte_display(self.target_size))

            check_lock = self.__acquire_check_lock()
            try:
                if check_lock is None:
                    logger.info("Failed to get filesystem clean lock (%s); quitting.",
                                self.blob_dir)
                    return

                self.__run_with_lock()
            finally:
                if check_lock is not None:
                    check_lock.close()
                self._finished_callback(self, check_lock is not None)

    def __run_with_lock(self):
        while 1:
            size, files_by_atime = self.__size_blob_dir()
            self.blob_dir_size = size

            if size <= self.target_size:
                logger.info(
                    'Traversed %s to compute size %s (<= %s); quitting.',
                    self.blob_dir,
                    byte_display(self.blob_dir_size),
                    byte_display(self.target_size)
                )
                break

            self.__shrink_blob_dir(size, files_by_atime)



def main():
    """
    This module can be executed as a main module. It takes two arguments:

    - a cache blob directory; and
    - a target size.

    It will proceed to prune old files from that directory until it reaches the
    target size.

    This is mainly meant to be used with the ``blob_cache_size_check_external``
    option.

    Exits with 0 on success, or a positive value on failure.
    """
    import logging
    import sys
    # Configure basic logging to go to stdout/err so that it can be seen from
    # our parent process when we're launched from a storage.
    logging.basicConfig()

    args = sys.argv[1:]

    cache_dir = args[0]
    target_size = int(args[1])

    checker = _BlobCacheSizeChecker(cache_dir, target_size)
    checker()


if __name__ == '__main__':
    main()
