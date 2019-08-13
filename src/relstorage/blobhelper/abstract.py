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

import os
import time

import ZODB.blob

from ZODB.POSException import StorageTransactionError

from relstorage._compat import iteritems
from relstorage._compat import MAC

from .util import lock_blob

class AbstractBlobHelper(object):
    """
    Stores blobs on the filesystem. This base class
    handles everything that doesn't depend on whether or
    not the disk storage is canonical (`shared_blob_dir`)
    or just a cache.
    """

    # _txn_blobs: {oid_bytes->filename}; contains blob data for the
    # currently uncommitted transaction.
    _txn_blobs = None

    #: A ZODB.blob.FilesystemHelper object. Subclasses must create.
    fshelper = None

    def __init__(self, options, adapter, fshelper):
        self.options = options
        self.adapter = adapter
        self.blob_dir = options.blob_dir
        self.fshelper = fshelper
        self.new_instance_kwargs = {}

    def __repr__(self):
        return "<%s at 0x%x blob_dir=%r txn_blobs=%s>" % (
            type(self).__name__,
            id(self),
            self.blob_dir,
            len(self._txn_blobs) if self._txn_blobs is not None else None
        )

    def new_instance(self, adapter):
        return type(self)(
            self.options,
            adapter,
            self.fshelper,
            **self.new_instance_kwargs
        )

    def clear_temp(self):
        self._txn_blobs = None

    def begin(self):
        if self._txn_blobs is not None:
            raise StorageTransactionError("Already in a transaction.")
        self._txn_blobs = {}

    @property
    def txn_has_blobs(self):
        return bool(self._txn_blobs)

    def close(self):
        self._txn_blobs = None
        self.fshelper = None
        self.options = None
        self.adapter = None
        self.new_instance_kwargs = None

    def _get_lockable_blob_filename(self, oid, serial):
        # Create the directory if needed
        self.fshelper.getPathForOID(oid, True)
        return self.fshelper.getBlobFilename(oid, serial)

    def _lock_blob_for_download(self, oid, serial):
        blob_filename = self._get_lockable_blob_filename(oid, serial)
        return lock_blob(blob_filename)

    if MAC:
        _lock_blob_for_open = _lock_blob_for_download
    else:
        _lock_blob_for_open = lambda self, oid, serial: None

    def loadBlob(self, cursor, oid, serial):
        return self._loadBlobInternal(cursor, oid, serial)

    def _loadBlobInternal(self, cursor, oid, serial, blob_lock=None):
        raise NotImplementedError

    @staticmethod
    def _accessed(filename):
        try:
            os.utime(filename, (time.time(), os.stat(filename).st_mtime))
        except OSError:
            pass # We tried. :)
        return filename

    def _cachedLoadBlobInternal(self, oid, serial):
        # Load a blob.
        # Note that the thread that cleans the blob cache up when it reaches
        # a maximum size could remove the blob file by the time the caller
        # gets the filename, so it could be gone.
        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        if os.path.exists(blob_filename):
            return self._accessed(blob_filename)

    def _openCommittedBlobFileInternal(self, cursor, oid, serial, blob, open_lock):
        blob_filename = self._loadBlobInternal(cursor, oid, serial, open_lock)
        if blob is None:
            result = open(blob_filename, 'rb')
        else:
            result = ZODB.blob.BlobFile(blob_filename, 'r', blob)
        return result

    def openCommittedBlobFile(self, cursor, oid, serial, blob=None):
        # First, try to make sure the file exists on disk.
        #
        # Next, open and return it. This would be expected to either
        # return a file we can read, or raise a FileNotFoundError.
        # Sadly, on some platforms (macOS 10.14.5 with APFS on Python
        # 3), this has a race condition with a concurrent unlink
        # syscall from the cache cleaner, such that the open succeeds,
        # but reading fails to return any data, depending on how the
        # open and unlink syscalls are interleaved. So we must be sure
        # to prevent the two from overlapping; we do that by holding
        # the lock. See https://github.com/zodb/relstorage/issues/219
        #
        # Unfortunately, but not unexpectedly, this about doubles the
        # amount of time it takes to open blobs that are already
        # present. So we jump through some hoops to only do this on
        # platforms that we know need it.
        blob_lock = self._lock_blob_for_open(oid, serial)
        try:
            try:
                return self._openCommittedBlobFileInternal(cursor, oid, serial, blob, blob_lock)
            except IOError:
                # An IOError here should mean that the file couldn't
                # be opened, probably because the cache cleaner came
                # through and deleted it. If we had already opened a
                # lock, then there's nothing we can do (the cache
                # cleaner wouldn't have been able to delete it).
                # However, we do test that we retry in that case.
                if blob_lock is None:
                    blob_lock = self._lock_blob_for_download(oid, serial)
                # If we didn't have the lock, we need to try again with the lock.
                return self._openCommittedBlobFileInternal(cursor, oid, serial, blob, blob_lock)
        finally:
            if blob_lock is not None:
                blob_lock.close()

    def temporaryDirectory(self):
        return self.fshelper.temp_dir

    def _doStoreBlob(self, store_func,
                     oid, serial, data, blobfilename, txn):
        """Storage API: store a blob object."""
        # Grab the file right away. That way, if we don't have enough
        # room for a copy, we'll know now rather than in tpc_finish.
        # Also, this relieves the client of having to manage the file
        # (or the directory contianing it).
        self.fshelper.getPathForOID(oid, create=True)
        fd, temp_path = self.fshelper.blob_mkstemp(oid, serial)
        os.close(fd)

        # It's a bit odd (and impossible on windows) to rename over an
        # existing file. We'll use the temporary file name as a base.
        temp_path += '-'
        ZODB.blob.rename_or_copy_blob(blobfilename, temp_path)
        os.remove(temp_path[:-1])
        self._add_blob_to_transaction(oid, temp_path)

        store_func(oid, serial, data, txn)
        return temp_path

    def _add_blob_to_transaction(self, oid, filename):
        old_filename = self._txn_blobs.get(oid)
        if old_filename is not None and old_filename != filename:
            ZODB.blob.remove_committed(old_filename)
        self._txn_blobs[oid] = filename

    def _move_blobs_into_place(self, tid):
        if not self._txn_blobs:
            return 0
        if not tid:
            raise StorageTransactionError("No TID for blobs")
        assert isinstance(tid, bytes)
        # We now have a transaction ID, so rename all the blobs
        # accordingly. This is very unlikely to fail. If we're
        # not using a shared blob-dir, it doesn't matter much if it fails;
        # source data is safely in the database, we'd just have some extra temporary
        # files. (Though we don't want that exception to populate from tpc_finish.)
        #
        # In fact, ClientStorage does this in tpc_finish for blob cache dirs.
        # It's not been reported as a problem there, so probably it really does
        # rarely fail. Exceptions from tpc_finish are a VERY BAD THING.
        total_size = 0
        for oid, sourcename in self._txn_blobs.items():
            size = os.stat(sourcename).st_size
            total_size += size
            targetname = self.fshelper.getBlobFilename(oid, tid)
            if sourcename != targetname:
                lock = lock_blob(targetname)
                try:
                    ZODB.blob.rename_or_copy_blob(sourcename, targetname)
                finally:
                    lock.close()
                self._txn_blobs[oid] = targetname
        return total_size

    def vote(self, tid=None):
        """
        Does nothing.
        """

    def finish(self, tid): # pylint:disable=unused-argument
        """
        Ends the transaction. Subclasses must call.
        """
        self.clear_temp()

    def _abort_filename(self, filename):
        """
        Needs to do something for shared blob dirs.
        """

    def abort(self):
        try:
            if not self._txn_blobs:
                return

            for _oid, filename in iteritems(self._txn_blobs):
                if os.path.exists(filename):
                    ZODB.blob.remove_committed(filename)
                    self._abort_filename(filename)
        finally:
            self.clear_temp()
