"""
Caching of blob data.

(This test is adapted from ZEO/tests/zeo_blob_cache.test in ZODB3.)

We support 2 modes for providing clients access to blob data:

shared
    Blob data are shared via a network file system.  The client shares
    a common blob directory with the server.

non-shared
    Blob data are loaded from the database and cached locally.
    A maximum size for the blob data can be set and data are removed
    when the size is exceeded.

This test is only for the non-shared (aka, "cached") case.

"""
from __future__ import absolute_import
from __future__ import print_function

import os
import threading
import time

import random2

from ZODB.blob import Blob
import transaction

import relstorage.blobhelper
from . import TestBlobMixin


class TestBlobCacheMixin(TestBlobMixin):

    # Here, we passed a blob_cache_size parameter, which specifies a
    # target blob cache size. This is not a hard limit, but rather a
    # target. It defaults to no limit. We also passed a
    # blob_cache_size_check option. The blob_cache_size_check option
    # specifies the number of bytes, as a percent of the target that
    # can be written or downloaded from the server before the cache
    # size is checked. The blob_cache_size_check option defaults to
    # 100. We passed 10, to check after writing 10% of the target
    # size.
    DEFAULT_BLOB_STORAGE_KWARGS = {
        'blob_cache_size': 3000,
        'blob_cache_size_check': 10
    }

    MAX_CLEANUP_THREADS = 1

    def setUp(self):
        super(TestBlobCacheMixin, self).setUp()
        # We're going to wait for any threads we started to finish, so...
        self._old_threads = list(threading.enumerate())

        # We want to check for name collisions in the blob cache dir.
        # We'll try to provoke name collisions by reducing the number
        # of cache directory subdirectories.
        self._orig_blob_cache_layout_size = relstorage.blobhelper.BlobCacheLayout.size
        relstorage.blobhelper.BlobCacheLayout.size = 11

        # We want to be notified when blob cache cleanup completes
        self.cleanup_finished = threading.Condition()
        self.cleanup_finished.acquire() # Don't alert until we're watching!
        run_lock = self.cleanup_lock = threading.Semaphore(self.MAX_CLEANUP_THREADS)
        self._orig_check_blob_cache_size = relstorage.blobhelper._check_blob_cache_size
        def _check(*args):
            t = threading.current_thread()
            threading.current_thread().name = 'Blob Cache Cleanup'  + t.name
            # The BlobHelper fires off threads constantly. Even though there's some
            # internal locking, it uses the filesystem, and at a massive rate that
            # can be problematic on some platforms. We don't want too many of them to stack up,
            # so we gate with an in-memory lock.
            if not run_lock.acquire(False):
                return
            try:
                self._orig_check_blob_cache_size(*args)
            except: # pylint:disable=bare-except
                import traceback; traceback.print_exc()
            finally:
                run_lock.release()
                # Ok, someone was specifically waiting on us. But that
                # doesn't mean that we were the ones to actually clean up
                # the directory! If it was already locked, it's possible we
                # didn't do anything. That's why we wait to acquire all the outstanding
                # cleanup locks.
                self.cleanup_finished.acquire()
                self.cleanup_finished.notify()
                self.cleanup_finished.release()


        relstorage.blobhelper._check_blob_cache_size = _check

    def tearDown(self):
        # Let the shrink run as many more times as it needs to, if it's waiting.
        self.cleanup_finished.release()
        relstorage.blobhelper._check_blob_cache_size = self._orig_check_blob_cache_size
        # Let them finish.
        for t in threading.enumerate():
            if t not in self._old_threads:
                t.join(10)
        self._old_threads = []
        relstorage.blobhelper.BlobCacheLayout.size = self._orig_blob_cache_layout_size
        super(TestBlobCacheMixin, self).tearDown()

    # Set this and BLOB_SIZE to create a total of 10,000
    # bytes of data
    BLOB_COUNT = 100
    BLOB_SIZE = 100

    def _blob_numbers(self):
        return range(1, 1 + self.BLOB_COUNT)

    def _data_for_blob_number(self, i):
        return (str(i) * self.BLOB_SIZE).encode('ascii')

    def _populate(self):
        conn = self.database.open()
        for i in self._blob_numbers():
            conn.root()[i] = Blob()
            with conn.root()[i].open('w') as f:
                f.write(self._data_for_blob_number(i))
                f.flush()
        transaction.commit()
        conn.close()

    def _verify_blob_number(self, i, conn, mode='r'):
        data_for_blob = self._data_for_blob_number(i)

        blob = conn.root()[i]
        with blob.open(mode) as f:
            f_str = "%s - %s" % (f, id(f))
            read = f.read()
        self.assertEqual(
            read,
            data_for_blob,
            (i, blob, blob._p_blob_committed, f_str, mode, threading.current_thread()))

    def _verify_all_blobs(self, mode='r'):
        conn = self.database.open()
        for blob_number in self._blob_numbers():
            self._verify_blob_number(blob_number, conn, mode)
        conn.close()

    def _size_blobs_in_directory(self):
        size = 0
        d = self.blob_storage.blobhelper.blob_dir
        for base, _, files in os.walk(d):
            for f in files:
                if f.endswith('.blob'):
                    try:
                        size += os.stat(os.path.join(base, f)).st_size
                    except OSError:
                        if os.path.exists(os.path.join(base, f)):
                            raise
        return size

    def _wait_for_shrink_to_run(self):
        # We already have this Condition acquired, and we keep it that way.
        # Wait for someone to notify us that a cleanup has finished.
        self.cleanup_finished.wait()
        # But don't go on to check until *all* outstanding cleanups have finished,
        # just in case we don't get notified by the one that did all the work.
        for _ in range(self.MAX_CLEANUP_THREADS):
            self.cleanup_lock.acquire()
        for _ in range(self.MAX_CLEANUP_THREADS):
            self.cleanup_lock.release()

    def test_exceed_size_and_shrink(self):
        self._populate()
        # We've committed 10000 bytes of data, but our target size is 3000.  We
        # expect to have not much more than the target size in the cache blob
        # directory.
        self.assertGreater(self._size_blobs_in_directory(), 2000)
        self._wait_for_shrink_to_run()
        self.assertLess(self._size_blobs_in_directory(), 5000)


        # If we read all of the blobs, data will be downloaded again, as
        # necessary, but the cache size will remain not much bigger than the
        # target:
        self._verify_all_blobs()
        self._wait_for_shrink_to_run()
        self.assertLess(self._size_blobs_in_directory(), 5000)

        self._verify_all_blobs()
        self._wait_for_shrink_to_run()
        self.assertLess(self._size_blobs_in_directory(), 5000)

        # This time using the 'committed' mode
        self._verify_all_blobs('c')

    def test_many_clients(self):
        # Now let see if we can stress things a bit.  We'll create many clients
        # and get them to pound on the blobs all at once to see if we can
        # provoke problems:
        self._populate()

        # Deterministic random numbers so we can track down any failures.
        random = random2.Random(42)

        verification_errors = []

        def verify_a_blob(i, conn, mode):
            try:
                self._verify_blob_number(i, conn, mode)
            except Exception as e: # pylint:disable=broad-except
                # relying on the GIL here.
                verification_errors.append(e)

        def client():
            conn = self.database.open()
            try:
                for i in range(300):
                    time.sleep(0)
                    i = random.randint(1, 100)
                    verify_a_blob(i, conn, 'r')
                    verify_a_blob(i, conn, 'c')
            finally:
                conn.close()


        threads = [threading.Thread(target=client) for i in range(10)]
        for thread in threads:
            thread.setDaemon(True)
            thread.start()

        for thread in threads:
            thread.join(99)
            self.assertFalse(thread.is_alive())

        for thread in threads:
            self.assertFalse(thread.is_alive())

        self._wait_for_shrink_to_run()
        self.assertLess(self._size_blobs_in_directory(), 5000)
        __traceback_info__ = verification_errors
        self.assertEmpty(verification_errors)
