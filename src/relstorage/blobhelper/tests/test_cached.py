##############################################################################
#
# Copyright (c) 2008,2019 Zope Foundation and Contributors.
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

# pylint:disable=too-many-public-methods,unused-argument
from __future__ import absolute_import
from __future__ import print_function

import errno
import os
import tempfile

from unittest import mock

import zc.lockfile

from . import test_blobhelper
from .test_blobhelper import write_file
from .test_blobhelper import read_file
from .test_blobhelper import test_oid
from .test_blobhelper import test_tid

# pylint:disable=protected-access

class _FlakyLock(object):
    """A lock whose close() always fails like the Windows unlock race."""
    def __init__(self, fp):
        self._fp = fp
    def close(self):
        raise zc.lockfile.LockError("Couldn't unlock 'x.lock'")

class CacheBlobHelperTest(test_blobhelper.BlobHelperTest):
    # Tests that only apply to cache dirs

    shared_blob_dir = False
    move_method = 'finish'

    def test_ctor_with_private_blob_dir(self):
        blobhelper = self._make_default()
        self.assertIsNotNone(blobhelper.fshelper)
        self.assertIsNotNone(blobhelper.cache_checker)
        from ZODB.blob import LAYOUTS
        self.assertEqual(blobhelper.fshelper.layout, LAYOUTS['zeocache'])

    def test_new_instance(self):
        class DummyAdapter2(object):
            pass

        blobhelper = self._make_default()
        blobhelper2 = blobhelper.new_instance(DummyAdapter2())
        # The adapter we passed is assigned, not the one the original object
        # had cached.
        self.assertNotIsInstance(blobhelper.adapter, DummyAdapter2)
        self.assertIsInstance(blobhelper2.adapter, DummyAdapter2)
        self.assertIs(blobhelper.fshelper, blobhelper2.fshelper)
        # pylint:disable-next=no-member
        self.assertIs(blobhelper.cache_checker, blobhelper2.cache_checker)


    def test_download_found(self):
        fn = os.path.join(self.blob_dir, '0001')
        blobhelper = self._make_default()
        blobhelper.download_blob(None, test_oid, test_tid, fn)
        self.assertTrue(os.path.exists(fn))

    def test_download_not_found(self):
        fn = os.path.join(self.blob_dir, '0001')
        blobhelper = self._make_default(download_action=None)
        blobhelper.download_blob(None, test_oid, test_tid, fn)
        self.assertFalse(os.path.exists(fn))

    def test_download_blob_tolerates_lost_rename_race(self):
        # On Windows, os.rename over a file held open by a concurrent
        # reader raises PermissionError. The target name embeds (oid, tid),
        # so an existing target holds identical content, just placed by the
        # racer; the download must succeed using it and drop our temp copy.
        blobhelper = self._make_default()
        fn = os.path.join(self.blob_dir, '0001')
        write_file(fn, 'winner here')
        with mock.patch('os.rename',
                        side_effect=PermissionError(errno.EACCES, 'denied')):
            blobhelper.download_blob(None, test_oid, test_tid, fn)
        self.assertEqual(read_file(fn), 'winner here')
        self.assertFalse(os.path.exists(fn + '.tmp'))

    def test_loadBlob_tolerates_lock_unlock_race(self):
        # On Windows, closing the directory lock can intermittently raise
        # LockError("Couldn't unlock ...") when another lock holder in the
        # same process already released the process-wide lock region. The
        # read must still succeed and the lock's fd must still be closed.
        blobhelper = self._make_default()
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        fp = tempfile.TemporaryFile()
        with mock.patch('relstorage.blobhelper.cached.lock_blob',
                        return_value=_FlakyLock(fp)):
            res = blobhelper.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)
        self.assertEqual(read_file(fn), 'blob here')
        self.assertTrue(fp.closed)

    def test_remove_blob_at_path_tolerates_lock_unlock_race(self):
        # Same unlock race as above, but on the cache cleaner path: it must
        # not kill the cleaner thread either.
        from relstorage.blobhelper.cached import _BlobCacheSizeChecker
        fn = os.path.join(self.blob_dir, 'victim.blob')
        write_file(fn, '0123456789')
        fp = tempfile.TemporaryFile()
        with mock.patch('relstorage.blobhelper.cached.lock_blob',
                        return_value=_FlakyLock(fp)):
            size = _BlobCacheSizeChecker.remove_blob_at_path(fn)
        self.assertEqual(size, 10)
        self.assertFalse(os.path.exists(fn))
        self.assertTrue(fp.closed)

    def test_openCommittedBlobFile_retries_transient_failures(self):
        # A transient IOError (e.g., the Windows "delete pending" race
        # with the cache cleaner) retries the whole open; success on a
        # later attempt returns the file.
        blobhelper = self._make_default()
        calls = []
        orig_internal = blobhelper._openCommittedBlobFileInternal
        def flaky_internal(cursor, oid, serial, blob, lock):
            calls.append(1)
            if len(calls) < 3:
                raise IOError("transient")
            return orig_internal(cursor, oid, serial, blob, lock)
        blobhelper._openCommittedBlobFileInternal = flaky_internal
        with blobhelper.openCommittedBlobFile(None, test_oid, test_tid) as f:
            self.assertEqual(f.read(), b'blob here')
        self.assertEqual(len(calls), 3)

    def test_openCommittedBlobFile_gives_up_after_retries(self):
        # ...but a persistent failure still surfaces after a bounded
        # number of attempts instead of retrying forever.
        blobhelper = self._make_default()
        calls = []
        def always_fail(cursor, oid, serial, blob, lock):
            calls.append(1)
            raise IOError("persistent")
        blobhelper._openCommittedBlobFileInternal = always_fail
        self.assertRaises(IOError, blobhelper.openCommittedBlobFile,
                          None, test_oid, test_tid)
        self.assertEqual(len(calls), 3)

    def test_rename_downloaded_blob_raises_when_target_never_usable(self):
        # If the rename keeps failing and no usable file appears, the
        # error surfaces (callers with retry, like openCommittedBlobFile,
        # may still recover).
        blobhelper = self._make_default()
        fn = os.path.join(self.blob_dir, '0001')
        write_file(fn + '.tmp', 'partial')
        with mock.patch('os.rename',
                        side_effect=PermissionError(errno.EACCES, 'denied')):
            self.assertRaises(PermissionError, blobhelper._rename_downloaded_blob,
                              fn + '.tmp', fn, retries=3)

    def test_upload_without_tid(self):
        fn = os.path.join(self.blob_dir, '0001')
        blobhelper = self._make_default()
        blobhelper.upload_blob(None, test_oid, None, fn)
        self.assertEqual(self.uploaded, (1, None, fn))

    def test_upload_with_tid(self):
        fn = os.path.join(self.blob_dir, '0001')
        blobhelper = self._make_default()
        blobhelper.upload_blob(None, test_oid, test_tid, fn)
        self.assertEqual(self.uploaded, (1, 2, fn))

    def test_loadBlob_unshared_exists(self):
        blobhelper = self._make_default()
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        res = blobhelper.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_loadBlob_unshared_download(self):
        blobhelper = self._make_default()
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        res = blobhelper.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_loadBlob_unshared_missing(self):
        blobhelper = self._make_default(download_action=None)
        from ZODB.POSException import POSKeyError
        self.assertRaises(POSKeyError, blobhelper.loadBlob, None, test_oid, test_tid)

    def test_openCommittedBlobFile_as_file(self):
        blobhelper = self._make_default()
        with blobhelper.openCommittedBlobFile(None, test_oid, test_tid) as f:
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_as_blobfile(self):
        blobhelper = self._make_default()
        from ZODB.blob import Blob
        from ZODB.blob import BlobFile
        b = Blob()
        with blobhelper.openCommittedBlobFile(None, test_oid, test_tid, blob=b) as f:
            self.assertEqual(f.__class__, BlobFile)
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_retry_as_file(self):
        loadBlob_calls = []
        blobhelper = self._make_default()
        orig_loadBlobInternal = blobhelper._loadBlobInternal
        def loadBlob_wrapper(cursor, oid, serial, blob_lock):
            fn = orig_loadBlobInternal(cursor, oid, serial, blob_lock)
            if not loadBlob_calls:
                # Use remove_committed to handle read-only cache files
                # on Windows (downloaded blobs are set_not_writable).
                from ZODB.blob import remove_committed
                remove_committed(fn)
                loadBlob_calls.append(1)
            return fn

        blobhelper._loadBlobInternal = loadBlob_wrapper
        with blobhelper.openCommittedBlobFile(None, test_oid, test_tid) as f:
            self.assertEqual(loadBlob_calls, [1])
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_retry_as_blobfile(self):
        loadBlob_calls = []
        blobhelper = self._make_default()

        orig_loadBlobInternal = blobhelper._loadBlobInternal
        def loadBlob_wrapper(cursor, oid, serial, blob_lock):
            fn = orig_loadBlobInternal(cursor, oid, serial, blob_lock)
            if not loadBlob_calls:
                # Use remove_committed to handle read-only cache files
                # on Windows (downloaded blobs are set_not_writable).
                from ZODB.blob import remove_committed
                remove_committed(fn)
                loadBlob_calls.append(1)
            return fn

        blobhelper._loadBlobInternal = loadBlob_wrapper
        from ZODB.blob import Blob
        from ZODB.blob import BlobFile
        b = Blob()
        with blobhelper.openCommittedBlobFile(None, test_oid, test_tid, b) as f:
            self.assertEqual(loadBlob_calls, [1])
            self.assertEqual(f.__class__, BlobFile)
            self.assertEqual(f.read(), b'blob here')

    def test_storeBlob_unshared(self):
        called = []
        dummy_txn = object()

        def store_func(oid, tid, data, txn):
            called.append((oid, tid, data, txn))

        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')

        blobhelper = self._make_default()
        self.assertFalse(blobhelper.txn_has_blobs)
        blobhelper.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                             fn, '', dummy_txn)
        self.assertFalse(os.path.exists(fn))
        self.assertTrue(blobhelper.txn_has_blobs)
        self.assertEqual(called,
                         [(test_oid, test_tid, 'blob pickle', dummy_txn)])
        self.assertEqual(self.uploaded[:2], (1, None))
        target_fn = self.uploaded[2]
        self.assertEqual(read_file(target_fn), 'here a blob')

    def test_restoreBlob_unshared(self):
        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')
        blobhelper = self._make_default()
        blobhelper.restoreBlob(None, test_oid, test_tid, fn)
        self.assertEqual(self.uploaded[:2], (1, 2))
        target_fn = self.uploaded[2]
        self.assertEqual(read_file(target_fn), 'here a blob')

    def test_copy_undone_unshared(self):
        blobhelper = self._make_default()
        blobhelper.copy_undone(None, None)
        self.assertFalse(blobhelper.txn_has_blobs)

    def test_after_pack_unshared(self):
        blobhelper = self._make_default()
        blobhelper.after_pack(None, None)  # No-op
