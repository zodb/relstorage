"""Tests of relstorage.blobhelper"""
# pylint:disable=too-many-public-methods,unused-argument

import os

from . import test_blobhelper
from .test_blobhelper import write_file
from .test_blobhelper import read_file
from .test_blobhelper import test_oid
from .test_blobhelper import test_tid


class SharedBlobHelperTest(test_blobhelper.BlobHelperTest):
    # Tests that only apply to shared blob dirs

    shared_blob_dir = True

    def test_ctor_with_shared_blob_dir(self):
        blobhelper = self._make_default()
        self.assertIsNotNone(blobhelper.fshelper)
        self.assertFalse(hasattr(blobhelper, 'cache_checker'))
        from ZODB.blob import LAYOUTS
        self.assertEqual(blobhelper.fshelper.layout, LAYOUTS['bushy'])

    def test_openCommittedBlobFile_retry_fail_on_shared(self):
        loadBlob_calls = []

        blobhelper = self._make_default()
        orig_loadBlobInternal = blobhelper._loadBlobInternal
        def loadBlob_wrapper(cursor, oid, serial, open_lock):
            loadBlob_calls.append(1)
            fn = orig_loadBlobInternal(cursor, oid, serial, open_lock)
            os.remove(fn)
            return fn

        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')

        blobhelper._loadBlobInternal = loadBlob_wrapper

        from ZODB.POSException import POSKeyError
        with self.assertRaises(POSKeyError):
            blobhelper.openCommittedBlobFile(None, test_oid, test_tid)
        self.assertEqual(loadBlob_calls, [1, 1])

    def test_loadBlob_shared_missing(self):
        blobhelper = self._make_default()
        from ZODB.POSException import POSKeyError
        with self.assertRaises(POSKeyError):
            blobhelper.loadBlob(None, test_oid, test_tid)

    def test_storeBlob_shared(self):
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

    def test_restoreBlob_shared(self):
        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')
        blobhelper = self._make_default()
        blobhelper.restoreBlob(None, test_oid, test_tid, fn)
        self.assertFalse(os.path.exists(fn))
        target_fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(target_fn), 'here a blob')

    def test_copy_undone_shared(self):
        blobhelper = self._make_default()
        copied = [(1, 1), (11, 1)]
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_oid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        blobhelper.copy_undone(copied, test_tid)
        self.assertTrue(blobhelper.txn_has_blobs)
        fn2 = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(fn2), 'blob here')

    def test_after_pack_shared_with_history(self):
        blobhelper = self._make_default()
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        blobhelper.after_pack(1, 2)
        self.assertFalse(os.path.exists(fn))

    def test_after_pack_shared_without_history(self):
        blobhelper = self._make_default(keep_history=False)
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        blobhelper.after_pack(1, 2)
        self.assertFalse(os.path.exists(fn))
