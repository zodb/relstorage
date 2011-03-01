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
"""Blob management utilities needed by RelStorage.

Most of this code is lifted from ZODB/ZEO.
"""

from ZODB import POSException
from ZODB import utils
from ZODB.utils import p64
from ZODB.utils import u64
import BTrees.OOBTree
import logging
import os
import re
import thread
import threading
import time
import zc.lockfile


try:
    import ZODB.blob
    from ZODB.blob import is_blob_record
    # Using ZODB 3.9 or above
except ImportError:
    try:
        from ZODB.blob import Blob
    except ImportError:
        # Using ZODB < 3.8
        def is_blob_record(record):
            False
    else:
        # Using ZODB 3.8
        import cPickle
        import cStringIO

        def find_global_Blob(module, class_):
            if module == 'ZODB.blob' and class_ == 'Blob':
                return Blob

        def is_blob_record(record):
            """Check whether a database record is a blob record.

            This is primarily intended to be used when copying data from one
            storage to another.

            """
            if record and ('ZODB.blob' in record):
                unpickler = cPickle.Unpickler(cStringIO.StringIO(record))
                unpickler.find_global = find_global_Blob

                try:
                    return unpickler.load() is Blob
                except (MemoryError, KeyboardInterrupt, SystemExit):
                    raise
                except Exception:
                    pass

            return False


class BlobHelper(object):
    """Blob support for RelStorage.

    There is one BlobHelper per storage instance.  Each BlobHelper
    instance has access to the associated adapter as well as shared
    instances of fshelper (a ZODB.blob.FilesystemHelper) and
    cache_checker (a BlobCacheChecker).
    """

    # _txn_blobs: {oid->filename}; contains blob data for the
    # currently uncommitted transaction.
    _txn_blobs = None

    def __init__(self, options, adapter, fshelper=None, cache_checker=None):
        self.options = options
        self.adapter = adapter
        self.blob_dir = options.blob_dir
        self.shared_blob_dir = options.shared_blob_dir

        if fshelper is None:
            if self.shared_blob_dir:
                # Share files over NFS or similar
                fshelper = ZODB.blob.FilesystemHelper(self.blob_dir)
            else:
                # The blob directory is a cache of the blobs
                if 'zeocache' not in ZODB.blob.LAYOUTS:
                    ZODB.blob.LAYOUTS['zeocache'] = BlobCacheLayout()
                fshelper = ZODB.blob.FilesystemHelper(
                    self.blob_dir, layout_name='zeocache')
            fshelper.create()
            fshelper.checkSecure()
        self.fshelper = fshelper

        if cache_checker is None:
            cache_checker = BlobCacheChecker(options)
        self.cache_checker = cache_checker

    def new_instance(self, adapter):
        return BlobHelper(options=self.options, adapter=adapter,
            fshelper=self.fshelper, cache_checker=self.cache_checker)

    def clear_temp(self):
        self._txn_blobs = None

    @property
    def txn_has_blobs(self):
        return bool(self._txn_blobs)

    def close(self):
        self.cache_checker.close()

    def download_blob(self, cursor, oid, serial, filename):
        """Download a blob into a file"""
        tmp_fn = filename + ".tmp"
        bytes = self.adapter.mover.download_blob(
            cursor, u64(oid), u64(serial), tmp_fn)
        if os.path.exists(tmp_fn):
            os.rename(tmp_fn, filename)
        self.cache_checker.loaded(bytes)

    def upload_blob(self, cursor, oid, serial, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        if serial is not None:
            tid_int = u64(serial)
        else:
            tid_int = None
        self.adapter.mover.upload_blob(cursor, u64(oid), tid_int, filename)

    def loadBlob(self, cursor, oid, serial):
        # Load a blob.  If it isn't present and we have a shared blob
        # directory, then assume that it doesn't exist on the server
        # and return None.

        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        if self.shared_blob_dir:
            if os.path.exists(blob_filename):
                return blob_filename
            else:
                # We're using a server shared cache.  If the file isn't
                # here, it's not anywhere.
                raise POSException.POSKeyError("No blob file", oid, serial)

        if os.path.exists(blob_filename):
            return _accessed(blob_filename)

        # First, we'll create the directory for this oid, if it doesn't exist.
        self.fshelper.getPathForOID(oid, create=True)

        # OK, it's not here and we (or someone) needs to get it.  We
        # want to avoid getting it multiple times.  We want to avoid
        # getting it multiple times even accross separate client
        # processes on the same machine. We'll use file locking.

        lock = _lock_blob(blob_filename)
        try:
            # We got the lock, so it's our job to download it.  First,
            # we'll double check that someone didn't download it while we
            # were getting the lock:

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            self.download_blob(cursor, oid, serial, blob_filename)

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            raise POSException.POSKeyError("No blob file", oid, serial)

        finally:
            lock.close()

    def openCommittedBlobFile(self, cursor, oid, serial, blob=None):
        blob_filename = self.loadBlob(cursor, oid, serial)
        try:
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        except (IOError):
            # The file got removed while we were opening.
            # Fall through and try again with the protection of the lock.
            pass

        lock = _lock_blob(blob_filename)
        try:
            blob_filename = self.fshelper.getBlobFilename(oid, serial)
            if not os.path.exists(blob_filename):
                if self.shared_blob_dir:
                    # We're using a server shared cache.  If the file isn't
                    # here, it's not anywhere.
                    raise POSException.POSKeyError("No blob file", oid, serial)
                self.download_blob(cursor, oid, serial, blob_filename)
                if not os.path.exists(blob_filename):
                    raise POSException.POSKeyError("No blob file", oid, serial)

            _accessed(blob_filename)
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        finally:
            lock.close()

    def temporaryDirectory(self):
        return self.fshelper.temp_dir

    def storeBlob(self, cursor, store_func,
            oid, serial, data, blobfilename, version, txn):
        """Storage API: store a blob object."""
        assert not version

        # Grab the file right away. That way, if we don't have enough
        # room for a copy, we'll know now rather than in tpc_finish.
        # Also, this relieves the client of having to manage the file
        # (or the directory contianing it).
        self.fshelper.getPathForOID(oid, create=True)
        fd, target = self.fshelper.blob_mkstemp(oid, serial)
        os.close(fd)

        # It's a bit odd (and impossible on windows) to rename over
        # an existing file.  We'll use the temporary file name as a base.
        target += '-'
        ZODB.blob.rename_or_copy_blob(blobfilename, target)
        os.remove(target[:-1])
        self._add_blob_to_transaction(oid, target)

        store_func(oid, serial, data, '', txn)

        if not self.shared_blob_dir:
            self.upload_blob(cursor, oid, None, target)

    def _add_blob_to_transaction(self, oid, filename):
        if self._txn_blobs is None:
            self._txn_blobs = {}
        else:
            old_filename = self._txn_blobs.get(oid)
            if old_filename is not None and old_filename != filename:
                ZODB.blob.remove_committed(old_filename)
        self._txn_blobs[oid] = filename

    def restoreBlob(self, cursor, oid, serial, blobfilename):
        if self.shared_blob_dir:
            self.fshelper.getPathForOID(oid, create=True)
            targetname = self.fshelper.getBlobFilename(oid, serial)
            ZODB.blob.rename_or_copy_blob(blobfilename, targetname)
        else:
            self.upload_blob(cursor, oid, serial, blobfilename)

    def copy_undone(self, copied, tid):
        """After an undo operation, copy the matching blobs forward.

        The copied parameter is a list of (integer oid, integer tid).
        """
        if not self.shared_blob_dir:
            # Not necessary
            return

        for oid_int, old_tid_int in copied:
            oid = p64(oid_int)
            old_tid = p64(old_tid_int)
            orig_fn = self.fshelper.getBlobFilename(oid, old_tid)
            if not os.path.exists(orig_fn):
                # not a blob
                continue

            new_fn = self.fshelper.getBlobFilename(oid, tid)
            orig = open(orig_fn, 'r')
            new = open(new_fn, 'wb')
            ZODB.utils.cp(orig, new)
            orig.close()
            new.close()

            self._add_blob_to_transaction(oid, new_fn)

    def after_pack(self, oid_int, tid_int):
        """Called after an object state has been removed by packing.

        Removes the corresponding blob file.
        """
        if not self.shared_blob_dir:
            # Not necessary
            return

        oid = p64(oid_int)
        tid = p64(tid_int)
        fn = self.fshelper.getBlobFilename(oid, tid)
        if self.adapter.keep_history:
            # remove only the revision just packed
            if os.path.exists(fn):
                ZODB.blob.remove_committed(fn)
                dirname = os.path.dirname(fn)
                if not _has_files(dirname):
                    ZODB.blob.remove_committed_dir(dirname)
        else:
            # remove all revisions
            dirname = os.path.dirname(fn)
            if os.path.exists(dirname):
                for name in os.listdir(dirname):
                    ZODB.blob.remove_committed(os.path.join(dirname, name))
                ZODB.blob.remove_committed_dir(dirname)

    def vote(self, tid):
        if self._txn_blobs:
            # We now have a transaction ID, so rename all the blobs
            # accordingly.
            for oid, sourcename in self._txn_blobs.items():
                bytes = os.stat(sourcename).st_size
                self.cache_checker.loaded(bytes, check=False)
                targetname = self.fshelper.getBlobFilename(oid, tid)
                if sourcename != targetname:
                    lock = _lock_blob(targetname)
                    try:
                        ZODB.blob.rename_or_copy_blob(sourcename, targetname)
                    finally:
                        lock.close()
                    self._txn_blobs[oid] = targetname
            self.cache_checker.check(True)

    def abort(self):
        if self._txn_blobs:
            for oid, filename in self._txn_blobs.iteritems():
                if os.path.exists(filename):
                    ZODB.blob.remove_committed(filename)
                    if self.shared_blob_dir:
                        dirname = os.path.dirname(filename)
                        if not _has_files(dirname):
                            ZODB.blob.remove_committed_dir(dirname)


class BlobCacheChecker(object):
    """Control the size of the blob cache.  Shared between BlobHelpers."""

    def __init__(self, options):
        self.blob_dir = options.blob_dir
        self.shared_blob_dir = options.shared_blob_dir
        self._blob_cache_size = options.blob_cache_size
        self._blob_data_bytes_loaded = 0
        if self._blob_cache_size is not None:
            assert options.blob_cache_size_check < 100
            self._blob_cache_size_check = (
                self._blob_cache_size * options.blob_cache_size_check / 100)
            self.check()

    def close(self):
        if self._check_blob_size_thread is not None:
            self._check_blob_size_thread.join()

    def loaded(self, bytes, check=True):
        self._blob_data_bytes_loaded += bytes
        if check:
            self.check(True)

    _check_blob_size_thread = None
    def check(self, check_loaded=False):
        """If appropriate, run blob cache cleanup in another thread."""
        if self._blob_cache_size is None:
            return
        if self.shared_blob_dir or not self.blob_dir:
            return

        if (check_loaded and
                self._blob_data_bytes_loaded < self._blob_cache_size_check):
            return

        self._blob_data_bytes_loaded = 0

        target = max(self._blob_cache_size - self._blob_cache_size_check, 0)

        check_blob_size_thread = threading.Thread(
            target=_check_blob_cache_size,
            args=(self.blob_dir, target),
            )
        check_blob_size_thread.setDaemon(True)
        check_blob_size_thread.start()
        self._check_blob_size_thread = check_blob_size_thread


# Note: the following code is copied directly from ZEO.ClientStorage.
# It is copied for two reasons:
#
# 1. Most of the symbols are not public (the function names start
#    with an underscore), indicating their signature could change
#    at any time.
#
# 2. No such code exists in ZODB 3.8, when blob support was first added
#    to ZODB, but RelStorage needs to continue to support ZODB 3.8
#    and 3.7 for a few years.


class BlobCacheLayout(object):

    size = 997

    def oid_to_path(self, oid):
        return str(utils.u64(oid) % self.size)

    def getBlobFilePath(self, oid, tid):
        base, rem = divmod(utils.u64(oid), self.size)
        return os.path.join(
            str(rem),
            "%s.%s%s" % (base, tid.encode('hex'), ZODB.blob.BLOB_SUFFIX)
        )


def _accessed(filename):
    try:
        os.utime(filename, (time.time(), os.stat(filename).st_mtime))
    except OSError:
        pass # We tried. :)
    return filename

cache_file_name = re.compile(r'\d+$').match
def _check_blob_cache_size(blob_dir, target):

    logger = logging.getLogger(__name__+'.check_blob_cache')

    layout = open(os.path.join(blob_dir, ZODB.blob.LAYOUT_MARKER)
                  ).read().strip()
    if not layout == 'zeocache':
        logger.critical("Invalid blob directory layout %s", layout)
        raise ValueError("Invalid blob directory layout", layout)

    attempt_path = os.path.join(blob_dir, 'check_size.attempt')

    try:
        check_lock = zc.lockfile.LockFile(
            os.path.join(blob_dir, 'check_size.lock'))
    except zc.lockfile.LockError:
        try:
            time.sleep(1)
            check_lock = zc.lockfile.LockFile(
                os.path.join(blob_dir, 'check_size.lock'))
        except zc.lockfile.LockError:
            # Someone is already cleaning up, so don't bother
            logger.debug("%s Another thread is checking the blob cache size.",
                         thread.get_ident())
            open(attempt_path, 'w').close() # Mark that we tried
            return

    logger.debug("%s Checking blob cache size. (target: %s)",
                 thread.get_ident(), target)

    try:
        while 1:
            size = 0
            blob_suffix = ZODB.blob.BLOB_SUFFIX
            files_by_atime = BTrees.OOBTree.BTree()

            for dirname in os.listdir(blob_dir):
                if not cache_file_name(dirname):
                    continue
                base = os.path.join(blob_dir, dirname)
                if not os.path.isdir(base):
                    continue
                for file_name in os.listdir(base):
                    if not file_name.endswith(blob_suffix):
                        continue
                    file_path = os.path.join(base, file_name)
                    if not os.path.isfile(file_path):
                        continue
                    stat = os.stat(file_path)
                    size += stat.st_size
                    t = stat.st_atime
                    if t not in files_by_atime:
                        files_by_atime[t] = []
                    files_by_atime[t].append(os.path.join(dirname, file_name))

            logger.debug("%s   blob cache size: %s", thread.get_ident(), size)

            if size <= target:
                if os.path.isfile(attempt_path):
                    try:
                        os.remove(attempt_path)
                    except OSError:
                        pass # Sigh, windows
                    continue
                logger.debug("%s   -->", thread.get_ident())
                break

            while size > target and files_by_atime:
                for file_name in files_by_atime.pop(files_by_atime.minKey()):
                    file_name = os.path.join(blob_dir, file_name)
                    lockfilename = os.path.join(os.path.dirname(file_name),
                                                '.lock')
                    try:
                        lock = zc.lockfile.LockFile(lockfilename)
                    except zc.lockfile.LockError:
                        logger.debug("%s Skipping locked %s",
                                     thread.get_ident(),
                                     os.path.basename(file_name))
                        continue  # In use, skip

                    try:
                        fsize = os.stat(file_name).st_size
                        try:
                            ZODB.blob.remove_committed(file_name)
                        except OSError, v:
                            pass # probably open on windows
                        else:
                            size -= fsize
                    finally:
                        lock.close()

                    if size <= target:
                        break

            logger.debug("%s   reduced blob cache size: %s",
                         thread.get_ident(), size)

    finally:
        check_lock.close()

def _lock_blob(path):
    lockfilename = os.path.join(os.path.dirname(path), '.lock')
    n = 0
    while 1:
        try:
            return zc.lockfile.LockFile(lockfilename)
        except zc.lockfile.LockError:
            time.sleep(0.01)
            n += 1
            if n > 60000:
                raise
        else:
            break

def _has_files(dirname):
    """Return True if a directory has any visible files."""
    names = os.listdir(dirname)
    if not names:
        return False
    for name in names:
        if not name.startswith('.'):
            return True
    return False
