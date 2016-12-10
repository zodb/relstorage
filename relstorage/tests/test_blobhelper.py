"""Tests of relstorage.blobhelper"""
# pylint:disable=too-many-public-methods,unused-argument
from relstorage.tests.util import support_blob_cache
import os
import unittest
import tempfile
from ZODB.blob import remove_committed_dir
from relstorage._compat import PY3


test_oid = b'\0' * 7 + b'\x01'
test_tid = b'\0' * 7 + b'\x02'


class BlobHelperTest(unittest.TestCase):

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

    def _make_default(self, shared=True, cache_size=None,
                      download_action='write', keep_history=True):
        test = self

        class DummyOptions(object):
            blob_dir = self.blob_dir
            shared_blob_dir = shared
            blob_cache_size = cache_size

        class DummyMover(object):
            def download_blob(self, cursor, oid_int, tid_int, filename):
                if download_action == 'write':
                    write_file(filename, 'blob here')
                    return 9
                else:
                    return 0

            def upload_blob(self, cursor, oid_int, tid_int, filename):
                test.uploaded = (oid_int, tid_int, filename)

        class DummyAdapter(object):
            mover = DummyMover()

            def __init__(self):
                self.keep_history = keep_history

        obj = self._make(DummyOptions(), DummyAdapter())
        return obj

    def test_ctor_with_shared_blob_dir(self):
        obj = self._make_default()
        self.assertTrue(obj.fshelper is not None)
        self.assertTrue(obj.cache_checker is not None)
        from ZODB.blob import LAYOUTS
        self.assertEqual(obj.fshelper.layout, LAYOUTS['bushy'])

    def test_ctor_with_private_blob_dir(self):
        if support_blob_cache:
            obj = self._make_default(shared=False)
            self.assertTrue(obj.fshelper is not None)
            self.assertTrue(obj.cache_checker is not None)
            from ZODB.blob import LAYOUTS
            self.assertEqual(obj.fshelper.layout, LAYOUTS['zeocache'])
        else:
            self.assertRaises(ValueError, self._make_default, shared=False)

    def test_new_instance(self):
        class DummyAdapter2(object):
            pass

        obj = self._make_default()
        obj2 = obj.new_instance(DummyAdapter2())
        self.assertFalse(isinstance(obj.adapter, DummyAdapter2))
        self.assertTrue(isinstance(obj2.adapter, DummyAdapter2))
        self.assertTrue(obj.fshelper is obj2.fshelper)
        self.assertTrue(obj.cache_checker is obj2.cache_checker)

    def test_download_found(self):
        fn = os.path.join(self.blob_dir, '0001')
        obj = self._make_default()
        obj.download_blob(None, test_oid, test_tid, fn)
        self.assertTrue(os.path.exists(fn))

    def test_download_not_found(self):
        fn = os.path.join(self.blob_dir, '0001')
        obj = self._make_default(download_action=None)
        obj.download_blob(None, test_oid, test_tid, fn)
        self.assertFalse(os.path.exists(fn))

    def test_upload_without_tid(self):
        fn = os.path.join(self.blob_dir, '0001')
        obj = self._make_default()
        obj.upload_blob(None, test_oid, None, fn)
        self.assertEqual(self.uploaded, (1, None, fn))

    def test_upload_with_tid(self):
        fn = os.path.join(self.blob_dir, '0001')
        obj = self._make_default()
        obj.upload_blob(None, test_oid, test_tid, fn)
        self.assertEqual(self.uploaded, (1, 2, fn))

    def test_loadBlob_shared_exists(self):
        obj = self._make_default()
        fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        res = obj.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_loadBlob_shared_missing(self):
        obj = self._make_default()
        from ZODB.POSException import POSKeyError
        self.assertRaises(POSKeyError, obj.loadBlob, None, test_oid, test_tid)

    def test_loadBlob_unshared_exists(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False)
        fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        res = obj.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_loadBlob_unshared_download(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False)
        fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        res = obj.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_loadBlob_unshared_missing(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False, download_action=None)
        from ZODB.POSException import POSKeyError
        self.assertRaises(POSKeyError, obj.loadBlob, None, test_oid, test_tid)

    def test_openCommittedBlobFile_as_file(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False)
        with obj.openCommittedBlobFile(None, test_oid, test_tid) as f:
            if not PY3:
                self.assertEqual(f.__class__, file) # pylint:disable=undefined-variable
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_as_blobfile(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False)
        from ZODB.blob import Blob
        from ZODB.blob import BlobFile
        b = Blob()
        with obj.openCommittedBlobFile(None, test_oid, test_tid, blob=b) as f:
            self.assertEqual(f.__class__, BlobFile)
            self.assertEqual(f.read(), b'blob here')


    def test_openCommittedBlobFile_retry_as_file(self):
        if not support_blob_cache:
            return

        loadBlob_calls = []

        def loadBlob_wrapper(cursor, oid, serial):
            fn = loadBlob(cursor, oid, serial)
            os.remove(fn)
            loadBlob_calls.append(1)
            return fn

        obj = self._make_default(shared=False)
        loadBlob, obj.loadBlob = obj.loadBlob, loadBlob_wrapper
        with obj.openCommittedBlobFile(None, test_oid, test_tid) as f:
            self.assertEqual(loadBlob_calls, [1])
            if not PY3:
                self.assertEqual(f.__class__, file) # pylint:disable=undefined-variable
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_retry_as_blobfile(self):
        if not support_blob_cache:
            return

        loadBlob_calls = []

        def loadBlob_wrapper(cursor, oid, serial):
            fn = loadBlob(cursor, oid, serial)
            os.remove(fn)
            loadBlob_calls.append(1)
            return fn

        obj = self._make_default(shared=False)
        loadBlob, obj.loadBlob = obj.loadBlob, loadBlob_wrapper
        from ZODB.blob import Blob
        from ZODB.blob import BlobFile
        b = Blob()
        with obj.openCommittedBlobFile(None, test_oid, test_tid, b) as f:
            self.assertEqual(loadBlob_calls, [1])
            self.assertEqual(f.__class__, BlobFile)
            self.assertEqual(f.read(), b'blob here')

    def test_openCommittedBlobFile_retry_fail_on_shared(self):
        if not support_blob_cache:
            return

        loadBlob_calls = []

        def loadBlob_wrapper(cursor, oid, serial):
            fn = loadBlob(cursor, oid, serial)
            os.remove(fn)
            loadBlob_calls.append(1)
            return fn

        obj = self._make_default(shared=True)
        fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')

        loadBlob, obj.loadBlob = obj.loadBlob, loadBlob_wrapper
        from ZODB.POSException import POSKeyError
        self.assertRaises(POSKeyError, obj.openCommittedBlobFile,
                          None, test_oid, test_tid)
        self.assertEqual(loadBlob_calls, [1])

    def test_storeBlob_shared(self):
        called = []
        dummy_txn = object()

        def store_func(oid, tid, data, version, txn):
            called.append((oid, tid, data, version, txn))

        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')

        obj = self._make_default()
        self.assertFalse(obj.txn_has_blobs)
        obj.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                      fn, '', dummy_txn)
        self.assertFalse(os.path.exists(fn))
        self.assertTrue(obj.txn_has_blobs)
        self.assertEqual(called,
                         [(test_oid, test_tid, 'blob pickle', '', dummy_txn)])

    def test_storeBlob_unshared(self):
        if not support_blob_cache:
            return

        called = []
        dummy_txn = object()

        def store_func(oid, tid, data, version, txn):
            called.append((oid, tid, data, version, txn))

        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')

        obj = self._make_default(shared=False)
        self.assertFalse(obj.txn_has_blobs)
        obj.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                      fn, '', dummy_txn)
        self.assertFalse(os.path.exists(fn))
        self.assertTrue(obj.txn_has_blobs)
        self.assertEqual(called,
                         [(test_oid, test_tid, 'blob pickle', '', dummy_txn)])
        self.assertEqual(self.uploaded[:2], (1, None))
        target_fn = self.uploaded[2]
        self.assertEqual(read_file(target_fn), 'here a blob')

    def test_storeBlob_replace(self):
        called = []
        dummy_txn = object()

        def store_func(oid, tid, data, version, txn):
            called.append((oid, tid, data, version, txn))

        fn1 = os.path.join(self.blob_dir, 'newblob')
        write_file(fn1, 'here a blob')
        fn2 = os.path.join(self.blob_dir, 'newblob2')
        write_file(fn2, 'there a blob')

        obj = self._make_default()
        self.assertFalse(obj.txn_has_blobs)
        obj.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                      fn1, '', dummy_txn)
        obj.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                      fn2, '', dummy_txn)
        self.assertFalse(os.path.exists(fn1))
        self.assertFalse(os.path.exists(fn2))
        self.assertTrue(obj.txn_has_blobs)
        target_fn = obj._txn_blobs[test_oid]
        self.assertEqual(read_file(target_fn), 'there a blob')

    def test_restoreBlob_shared(self):
        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')
        obj = self._make_default()
        obj.restoreBlob(None, test_oid, test_tid, fn)
        self.assertFalse(os.path.exists(fn))
        target_fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(target_fn), 'here a blob')

    def test_restoreBlob_unshared(self):
        if not support_blob_cache:
            return

        fn = os.path.join(self.blob_dir, 'newblob')
        write_file(fn, 'here a blob')
        obj = self._make_default(shared=False)
        obj.restoreBlob(None, test_oid, test_tid, fn)
        self.assertEqual(self.uploaded[:2], (1, 2))
        target_fn = self.uploaded[2]
        self.assertEqual(read_file(target_fn), 'here a blob')

    def test_copy_undone_unshared(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False)
        obj.copy_undone(None, None)
        self.assertFalse(obj.txn_has_blobs)

    def test_copy_undone_shared(self):
        obj = self._make_default()
        copied = [(1, 1), (11, 1)]
        fn = obj.fshelper.getBlobFilename(test_oid, test_oid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        obj.copy_undone(copied, test_tid)
        self.assertTrue(obj.txn_has_blobs)
        fn2 = obj.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(fn2), 'blob here')

    def test_after_pack_unshared(self):
        if not support_blob_cache:
            return

        obj = self._make_default(shared=False)
        obj.after_pack(None, None)  # No-op

    def test_after_pack_shared_with_history(self):
        obj = self._make_default()
        fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        obj.after_pack(1, 2)
        self.assertFalse(os.path.exists(fn))

    def test_after_pack_shared_without_history(self):
        obj = self._make_default(keep_history=False)
        fn = obj.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        obj.after_pack(1, 2)
        self.assertFalse(os.path.exists(fn))

    def test_vote(self):
        obj = self._make_default()
        d = obj.fshelper.getPathForOID(test_oid, create=True)
        fn1 = os.path.join(d, 'newblob')
        write_file(fn1, 'here a blob')
        obj._txn_blobs = {test_oid: fn1}
        obj.vote(test_tid)
        fn2 = obj.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(fn2), 'here a blob')

    def test_abort(self):
        obj = self._make_default()
        d = obj.fshelper.getPathForOID(test_oid, create=True)
        fn1 = os.path.join(d, 'newblob')
        write_file(fn1, 'here a blob')
        obj._txn_blobs = {test_oid: fn1}
        obj.abort()
        fn2 = obj.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertFalse(os.path.exists(fn1))
        self.assertFalse(os.path.exists(fn2))


def write_file(fn, data):
    assert isinstance(data, str)
    with open(fn, 'w') as f:
        f.write(data)


def read_file(fn):
    with open(fn, 'r') as f:
        return f.read()


def test_suite():
    suite = unittest.TestSuite()
    for klass in [
            BlobHelperTest,
        ]:
        suite.addTest(unittest.makeSuite(klass, "test"))

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
