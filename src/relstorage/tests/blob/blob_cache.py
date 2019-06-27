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

import random2

from ZODB.blob import Blob
import transaction

import relstorage.blobhelper
from relstorage.tests.util import RUNNING_ON_CI
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
    # Too many client threads really slows us down because of the GIL,
    # but we do need some concurrency.
    CLIENT_COUNT = 4 if not RUNNING_ON_CI else 2

    def setUp(self):
        super(TestBlobCacheMixin, self).setUp()
        # We're going to wait for any threads we started to finish, so...
        self._old_threads = list(threading.enumerate())

        run_lock = threading.Semaphore(self.MAX_CLEANUP_THREADS)
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

        relstorage.blobhelper._check_blob_cache_size = _check

    def _wait_for_all_spawned_threads_to_finish(self):
        # pylint:disable=method-hidden

        # Do this only once, at the end.
        to_join = set(threading.enumerate()) - set(self._old_threads)
        for t in to_join:
            t.join(10)
        self._wait_for_all_spawned_threads_to_finish = lambda: None

    def tearDown(self):
        # # Let the shrink run as many more times as it needs to, if it's waiting.
        # self.cleanup_finished.release()
        self._wait_for_all_spawned_threads_to_finish()
        relstorage.blobhelper._check_blob_cache_size = self._orig_check_blob_cache_size
        self._old_threads = []
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
            read = f.read()
        self.assertEqual(
            read,
            data_for_blob)

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

    def _wait_for_shrinks_to_finish(self):
        # We already have this Condition acquired, and we keep it that way.
        # Wait for someone to notify us that a cleanup has finished.
        # We only allow one at a time, so this one did what was necessary.
        self._wait_for_all_spawned_threads_to_finish()
        size = self._size_blobs_in_directory()
        self.assertLess(size, 5000)

    def test_exceed_size_and_shrink(self):
        self._populate()
        # We've committed 10000 bytes of data, but our target size is 3000.  We
        # expect to have not much more than the target size in the cache blob
        # directory. At the end of the process. Concurrently, cleanups
        # will be going on, racing with us to read.

        # If we read all of the blobs, data will be downloaded again, as
        # necessary, but the cache size will remain not much bigger than the
        # target:
        self._verify_all_blobs()
        self._verify_all_blobs()

        # This time using the 'committed' mode
        self._verify_all_blobs('c')
        self._wait_for_shrinks_to_finish()


    def test_many_clients(self):
        # Now let see if we can stress things a bit.  We'll create many clients
        # and get them to pound on the blobs all at once to see if we can
        # provoke problems:
        self._populate()

        verification_errors = []

        def verify_a_blob(i, conn, mode):
            try:
                self._verify_blob_number(i, conn, mode)
            except Exception as e: # pylint:disable=broad-except
                # relying on the GIL here.
                verification_errors.append(e)

        def client(client_num):
            # Deterministic random numbers so we can track down any failures.
            random = random2.Random(client_num)
            conn = self.database.open()
            try:
                for i in range(300):
                    i = random.randint(1, 100)
                    verify_a_blob(i, conn, 'r')
                    verify_a_blob(i, conn, 'c')
            finally:
                conn.close()


        threads = [threading.Thread(target=client, args=(i,))
                   for i in range(self.CLIENT_COUNT)]
        for thread in threads:
            thread.setDaemon(True)
            thread.start()

        for thread in threads:
            thread.join(10)

        del threads

        self._wait_for_shrinks_to_finish()
        __traceback_info__ = verification_errors
        self.assertEmpty(verification_errors)
