"""
Test that copying a blob object does not eagerly duplicate
the blob file in the local blob cache.

When RelStorage is configured without shared_blob_dir
(CacheBlobHelper), the blob_dir is a local cache and blobs
are persisted in the relational database. The cache is
populated lazily when a blob is actually accessed (loadBlob /
openCommittedBlobFile), not as a side effect of storing or
copying an object.

This test simulates the Copy/Paste workflow via storeBlob:
  1. Store original blob for oid1/tid1, finish -> no cache file
     is created eagerly; it appears only after first access.
  2. Store copied blob for oid2/tid2 with identical content
     (simulating a new ZODB object created by copy/paste with
     same blob bytes), finish -> must NOT create a second cache
     file until the copy is actually accessed.
"""

import os
import tempfile
import hashlib

from relstorage.tests import TestCase
from ZODB.blob import remove_committed_dir

# Use distinct OIDs/TIDs to simulate copy to a new object
oid1 = b'\0' * 7 + b'\x01'
tid1 = b'\0' * 7 + b'\x02'
oid2 = b'\0' * 7 + b'\x03'  # new OID from copy/paste
tid2 = b'\0' * 7 + b'\x04'

# Blob content to mimic a production file (1 MiB, identical for copy)
BLOB_CONTENT = b'x' * (1024 * 1024)


class CachedBlobCopyTest(TestCase):

    def setUp(self):
        self.blob_dir = tempfile.mkdtemp()
        self.uploaded = []

    def tearDown(self):
        remove_committed_dir(self.blob_dir)

    def _make_cache_blobhelper(self):
        from relstorage.blobhelper import BlobHelper

        test = self

        class DummyOptions:
            blob_dir = self.blob_dir
            shared_blob_dir = False
            blob_cache_size = 8 * 1024 * 1024 * 1024  # 8GB as in production
            blob_cache_size_check = 10
            blob_cache_size_check_external = False
            keep_history = False

        class DummyMover:  # pylint:disable=too-few-public-methods
            def download_blob(self, cursor, oid_int, tid_int, filename):  # pylint:disable=unused-argument
                # Simulate download by writing known content if needed
                with open(filename, 'wb') as f:
                    f.write(BLOB_CONTENT)
                return len(BLOB_CONTENT)

            def upload_blob(self, cursor, oid_int, tid_int, filename):  # pylint:disable=unused-argument
                test.uploaded.append((oid_int, tid_int, filename))
                assert os.path.exists(filename)

        class DummyAdapter:
            mover = DummyMover()
            keep_history = False

        helper = BlobHelper(DummyOptions(), DummyAdapter())
        helper.begin()
        return helper

    def _count_blob_files(self):
        count = 0
        sizes = []
        hashes = []
        for dirpath, _, filenames in os.walk(self.blob_dir):
            for f in filenames:
                if f.endswith('.blob'):
                    fp = os.path.join(dirpath, f)
                    count += 1
                    sizes.append(os.stat(fp).st_size)
                    with open(fp, 'rb') as fh:
                        h = hashlib.sha256(fh.read()).hexdigest()
                        hashes.append(h)
        return count, sizes, hashes

    def test_copy_does_not_eagerly_duplicate_blob_in_cache(self):  # pylint:disable=too-many-locals,too-many-statements
        """
        Simulate:
          1. Upload new file -> storeBlob oid1/tid1, finish.
             No cache file is created eagerly.
          2. Access oid1 -> cache file appears.
          3. Copy/paste -> storeBlob oid2/tid2 with same bytes, finish.
             No second cache file is created eagerly; it appears
             only after the copy is accessed.
        """
        helper = self._make_cache_blobhelper()

        # --- Step 1: original upload ---
        orig_src = os.path.join(self.blob_dir, 'orig_src.tmp')
        with open(orig_src, 'wb') as f:
            f.write(BLOB_CONTENT)

        def store_func(oid, serial, data, txn):  # pylint:disable=unused-argument
            pass

        dummy_txn = object()
        helper.storeBlob(None, store_func, oid1, None, b'pickle', orig_src, '', dummy_txn)
        self.assertFalse(os.path.exists(orig_src), "storeBlob should consume source file")
        helper.finish(tid1)

        fn1 = helper.fshelper.getBlobFilename(oid1, tid1)
        # Cached blobs are lazy: store + finish must NOT eagerly create
        # a cache file. The blob is persisted in the DB and cached only
        # on first access via loadBlob.
        self.assertFalse(os.path.exists(fn1),
                         "cached blob should NOT be eagerly cached after finish (lazy)")
        count1, _, _ = self._count_blob_files()
        self.assertEqual(count1, 0, "after first store, 0 .blob files should exist (lazy cache)")
        # Verify that the blob is available via lazy load
        loaded_fn1 = helper.loadBlob(None, oid1, tid1)
        self.assertTrue(os.path.exists(loaded_fn1))
        self.assertEqual(loaded_fn1, fn1)
        with open(loaded_fn1, 'rb') as fh:
            self.assertEqual(hashlib.sha256(fh.read()).hexdigest(),
                             hashlib.sha256(BLOB_CONTENT).hexdigest())
        count1_after_load, sizes1, _ = self._count_blob_files()
        self.assertEqual(count1_after_load, 1, "after first access, 1 .blob file should exist")
        self.assertEqual(sizes1[0], len(BLOB_CONTENT))

        # --- Step 2: copy/paste to new OID ---
        helper.begin()

        copy_src = os.path.join(self.blob_dir, 'copy_src.tmp')
        with open(copy_src, 'wb') as f:
            f.write(BLOB_CONTENT)

        helper.storeBlob(None, store_func, oid2, None, b'pickle', copy_src, '', dummy_txn)
        self.assertFalse(os.path.exists(copy_src))

        helper.finish(tid2)

        fn2 = helper.fshelper.getBlobFilename(oid2, tid2)
        count2, _, hashes2 = self._count_blob_files()

        self.assertEqual(
            count2, 1,
            f"Copy eagerly duplicated blob in cache. "
            f"Expected 1 blob file (lazy cache), got {count2} files. "
            f"Files: {list(self._iter_blob_files())} "
            f"Hashes: {hashes2} "
            f"fn2 exists={os.path.exists(fn2)}"
        )

        # Verify lazy load for the copy
        if not os.path.exists(fn2):
            loaded_fn = helper.loadBlob(None, oid2, tid2)
            self.assertTrue(os.path.exists(loaded_fn))
            self.assertEqual(loaded_fn, fn2)
            count3, _, _ = self._count_blob_files()
            self.assertEqual(count3, 2, "after first access, cache should then contain 2 files")

        helper.close()

    def _iter_blob_files(self):
        for dirpath, _, filenames in os.walk(self.blob_dir):
            for f in filenames:
                if f.endswith('.blob'):
                    yield os.path.join(dirpath, f)
