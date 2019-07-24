"""Tests of relstorage.blobhelper"""
# pylint:disable=too-many-public-methods,unused-argument

import os
import tempfile

from ZODB.blob import remove_committed_dir
from ZODB.POSException import StorageTransactionError

from hamcrest import assert_that
from nti.testing.matchers import validly_provides


from relstorage._compat import PY3
from relstorage.interfaces import IBlobHelper

from . import TestCase

test_oid = b'\0' * 7 + b'\x01'
test_tid = b'\0' * 7 + b'\x02'


def write_file(fn, data):
    assert isinstance(data, str)
    with open(fn, 'w') as f:
        f.write(data)


def read_file(fn):
    with open(fn, 'r') as f:
        return f.read()


class BlobHelperTest(TestCase):

    shared_blob_dir = True
    move_method = 'vote'

    def setUp(self):
        self.uploaded = None
        self.blob_dir = tempfile.mkdtemp()

    def tearDown(self):
        remove_committed_dir(self.blob_dir)

    def _class(self):
        from relstorage.blobhelper import BlobHelper
        return BlobHelper

    def _make(self, *args, **kw):
        return self._class()(*args, **kw)

    def _make_default(self, cache_size=None,
                      download_action='write', keep_history=True):
        test = self

        class DummyOptions(object):
            blob_dir = self.blob_dir
            shared_blob_dir = self.shared_blob_dir
            blob_cache_size = cache_size

        class DummyMover(object):
            def download_blob(self, cursor, oid_int, tid_int, filename):
                if download_action == 'write':
                    write_file(filename, 'blob here')
                    return 9
                return 0

            def upload_blob(self, cursor, oid_int, tid_int, filename):
                test.uploaded = (oid_int, tid_int, filename)

        class DummyAdapter(object):
            mover = DummyMover()

            def __init__(self):
                self.keep_history = keep_history

        blobhelper = self._make(DummyOptions(), DummyAdapter())
        blobhelper.begin()
        return blobhelper

    def test_provides(self):
        assert_that(self._make_default(), validly_provides(IBlobHelper))

    def test_cannot_begin_twice(self):
        blobhelper = self._make_default()
        with self.assertRaises(StorageTransactionError):
            blobhelper.begin()

    def test_begin_after_abort(self):
        blobhelper = self._make_default()
        blobhelper.abort()
        blobhelper.begin()

    def test_begin_after_finish(self):
        blobhelper = self._make_default()
        blobhelper.finish(b'tid')
        blobhelper.begin()

    def test_loadBlob_shared_exists(self):
        blobhelper = self._make_default()
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        res = blobhelper.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_storeBlob_replace(self):
        called = []
        dummy_txn = object()

        def store_func(oid, tid, data, txn):
            called.append((oid, tid, data, txn))

        fn1 = os.path.join(self.blob_dir, 'newblob')
        write_file(fn1, 'here a blob')
        fn2 = os.path.join(self.blob_dir, 'newblob2')
        write_file(fn2, 'there a blob')

        blobhelper = self._make_default()
        self.assertFalse(blobhelper.txn_has_blobs)
        blobhelper.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                             fn1, '', dummy_txn)
        blobhelper.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                             fn2, '', dummy_txn)
        self.assertFalse(os.path.exists(fn1))
        self.assertFalse(os.path.exists(fn2))
        self.assertTrue(blobhelper.txn_has_blobs)
        target_fn = blobhelper._txn_blobs[test_oid]
        self.assertEqual(read_file(target_fn), 'there a blob')

    def test_move_into_place(self):
        blobhelper = self._make_default()
        d = blobhelper.fshelper.getPathForOID(test_oid, create=True)
        fn1 = os.path.join(d, 'newblob')
        write_file(fn1, 'here a blob')
        blobhelper._txn_blobs = {test_oid: fn1}
        move_method = getattr(blobhelper, self.move_method)
        move_method(test_tid)
        fn2 = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(fn2), 'here a blob')

    def test_abort(self):
        blobhelper = self._make_default()
        d = blobhelper.fshelper.getPathForOID(test_oid, create=True)
        fn1 = os.path.join(d, 'newblob')
        write_file(fn1, 'here a blob')
        blobhelper._txn_blobs = {test_oid: fn1}
        blobhelper.abort()
        fn2 = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertFalse(os.path.exists(fn1))
        self.assertFalse(os.path.exists(fn2))


class SharedBlobHelperTest(BlobHelperTest):
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


class CacheBlobHelperTest(BlobHelperTest):
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
            if not PY3:
                self.assertEqual(f.__class__, file) # pylint:disable=undefined-variable
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
                os.remove(fn)
                loadBlob_calls.append(1)
            return fn

        blobhelper._loadBlobInternal = loadBlob_wrapper
        with blobhelper.openCommittedBlobFile(None, test_oid, test_tid) as f:
            self.assertEqual(loadBlob_calls, [1])
            if not PY3:
                self.assertEqual(f.__class__, file) # pylint:disable=undefined-variable
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_retry_as_blobfile(self):
        loadBlob_calls = []
        blobhelper = self._make_default()

        orig_loadBlobInternal = blobhelper._loadBlobInternal
        def loadBlob_wrapper(cursor, oid, serial, blob_lock):
            fn = orig_loadBlobInternal(cursor, oid, serial, blob_lock)
            if not loadBlob_calls:
                os.remove(fn)
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


class NoBlobHelperTest(TestCase):

    def _class(self):
        from relstorage.blobhelper import NoBlobHelper
        return NoBlobHelper

    def makeOne(self):
        return self._class()()

    def test_provides(self):
        assert_that(self.makeOne(), validly_provides(IBlobHelper))
