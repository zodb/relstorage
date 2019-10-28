##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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

import atexit
import collections
import os
import random
import stat
import struct
import tempfile
import time
import unittest
from hashlib import md5

import transaction
import ZODB.blob
import ZODB.interfaces
import ZODB.tests.StorageTestBase
import ZODB.tests.util
from ZODB.blob import Blob
from ZODB.DB import DB
from ZODB.serialize import referencesf

from relstorage.tests import TestCase
from relstorage.tests.util import USE_SMALL_BLOBS
from relstorage.tests.util import MinimalTestLayer as BaseTestLayer
from relstorage.tests.RecoveryStorage import IteratorDeepCompare
from relstorage.tests.blob.blob_packing import TestBlobPackHistoryPreservingMixin
from relstorage.tests.blob.blob_packing import TestBlobPackHistoryFreeMixin
from relstorage.tests.blob.blob_connection import TestConnectionBlobMixin
from relstorage.tests.blob.blob_importexport import TestBlobImportExportMixin
from relstorage.tests.blob.blob_transaction import TestBlobTransactionMixin
from relstorage.tests.blob.blob_cache import TestBlobCacheMixin
from relstorage.tests.blob.blob_cache import TestBlobCacheExternalCleanupMixin
from relstorage.tests.blob import TestBlobMixin

class BlobTestBase(TestCase,
                   ZODB.tests.StorageTestBase.StorageTestBase):

    def setUp(self):
        super(BlobTestBase, self).setUp()
        try:
            self._storage = self.create_storage() # pylint:disable=no-member
        except:
            self.tearDown()
            raise


class BlobUndoTests(BlobTestBase):

    def testUndoWithoutPreviousVersion(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        root['blob'] = Blob()
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()
        transaction.manager.manager.free(transaction.get())

        # the blob footprint object should exist no longer
        self.assertRaises(KeyError, root.__getitem__, 'blob')
        database.close()

    def testUndo(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        blob = Blob()
        with blob.open('w') as f:
            f.write(b'this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        with blob.open('w') as f:
            f.write(b'this is state 2')
        transaction.commit()


        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as f:
            data = f.read()
        self.assertEqual(data, b'this is state 1')

        database.close()

    def testUndoAfterConsumption(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        with open('consume1', 'wb') as f: f.write(b'this is state 1')
        blob = Blob()
        blob.consumeFile('consume1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        with open('consume2', 'wb') as f: f.write(b'this is state 2')
        blob.consumeFile('consume2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as f:
            data = f.read()
        self.assertEqual(data, b'this is state 1')

        database.close()

    def testRedo(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        with blob.open('w') as f:
            f.write(b'this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        with blob.open('w') as f:
            f.write(b'this is state 2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as f:
            self.assertEqual(f.read(), b'this is state 1')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as f:
            self.assertEqual(f.read(), b'this is state 2')

        database.close()

    def testRedoOfCreation(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        with blob.open('w') as f:
            f.write(b'this is state 1')
        root['blob'] = blob
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertRaises(KeyError, root.__getitem__, 'blob')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as f:
            self.assertEqual(f.read(), b'this is state 1')

        database.close()


class RecoveryBlobStorage(BlobTestBase,
                          IteratorDeepCompare):

    _dst = None

    def setUp(self):
        BlobTestBase.setUp(self)
        self._dst = self.create_storage('dest') # pylint:disable=no-member

    def tearDown(self):
        if self._dst is not None:
            self._dst.close()
        BlobTestBase.tearDown(self)

    # Requires a setUp() that creates a self._dst destination storage
    def testSimpleBlobRecovery(self):
        self.assertTrue(
            ZODB.interfaces.IBlobStorageRestoreable.providedBy(
                self._storage)
            )
        db = DB(self._storage)
        conn = db.open()
        root = conn.root()
        root._p_activate()
        root[1] = ZODB.blob.Blob()
        transaction.commit()
        root[2] = ZODB.blob.Blob()
        with root[2].open('w') as f:
            f.write(b'some data')
        transaction.commit()
        root[3] = ZODB.blob.Blob()
        with root[3].open('w') as f:
            f.write(
                (b''.join(struct.pack(">I", random.randint(0, (1<<32)-1))
                          for i in range(random.randint(10000, 20000)))
                )[:-random.randint(1, 4)]
            )
        transaction.commit()
        root[2] = ZODB.blob.Blob()
        with root[2].open('w') as f:
            f.write(b'some other data')
        transaction.commit()
        __traceback_info__ = self._storage, self._dst
        self._dst.copyTransactionsFrom(self._storage)

        self.compare(self._storage, self._dst)

        conn.close()
        db.close()


class LargeBlobTest(BlobTestBase): # pragma: no cover
    """Test large blob upload and download.

    Note that this test excercises the blob storage and only makes sense
    when shared_blob_support=False.

    """
    # Only run when selecting -a 2 or higher, or --all if we're using
    # huge blobs.
    level = 2 if not USE_SMALL_BLOBS else 0
    testsize = 0 # Set on the auto-generated parent class

    with open(__file__, 'rb') as _f:
        # Just use the this module as the source of our data
        # Capture it at import time because test cases may
        # chdir(), and we may not have an absolute path in __file__,
        # depending on how they are run.
        _random_file_data = _f.read().replace(b'\n', b'').split()
    del _f


    def _random_file(self, size, fd):
        """
        Create a random data of at least the given size, writing to fd.

        See
        http://jessenoller.com/2008/05/30/making-re-creatable-random-data-files-really-fast-in-python/
        for the technique used.

        Returns the md5 sum of the file contents for easy comparison.
        """
        def fdata():
            seed = "1092384956781341341234656953214543219"
            words = self._random_file_data
            a = collections.deque(words)
            b = collections.deque(seed)
            while True:
                yield b' '.join(list(a)[0:1024])
                a.rotate(int(b[0]))
                b.rotate(1)
        datagen = fdata()
        bytes = 0
        hasher = md5()
        while bytes < size:
            data = next(datagen)
            hasher.update(data)
            fd.write(data)
            bytes += len(data)
        return hasher.hexdigest()


    def _md5sum(self, fd):
        hasher = md5()
        blocksize = hasher.block_size << 8
        for data in iter(lambda: fd.read(blocksize), b''):
            hasher.update(data)
        return hasher.hexdigest()

    def testLargeBlob(self):
        # Large blobs are chunked into multiple pieces, we want to know
        # if they come out the same way they went in.
        db = DB(self._storage)
        conn = db.open()
        blob = conn.root()[1] = ZODB.blob.Blob()
        blob_file = blob.open('w')
        signature = self._random_file(self.testsize, blob_file)
        blob_file.close()
        transaction.commit()
        conn.close()

        # Clear the cache
        for base, _dir, files in os.walk('.'):
            for f in files:
                if f.endswith('.blob'):
                    ZODB.blob.remove_committed(os.path.join(base, f))

        # Re-download blob
        conn = db.open()
        with conn.root()[1].open('r') as blob:
            self.assertEqual(self._md5sum(blob), signature)

        conn.close()
        db.close()

class TestThingsPreviouslyDocTests(TestBlobMixin,
                                   TestCase):

    def test_packing_with_uncommitted_data_non_undoing(self):
        """
        This covers regression for bug #130459.

        When uncommitted data exists it formerly was written to the root of the
        blob_directory and confused our packing strategy. We now use a separate
        temporary directory that is ignored while packing.
        """

        blob_storage = self.blob_storage
        database = self.database
        connection = database.open()
        root = connection.root()
        root['blob'] = Blob()
        connection.add(root['blob'])
        with root['blob'].open('w') as f: _ = f.write(b'test')

        blob_storage.pack(time.time(), referencesf)

        # Clean up:

        transaction.abort()
        connection.close()
        blob_storage.close()
        database.close()

    def test_packing_with_uncommitted_data_undoing(self):
        """
        This covers regression for bug #130459.

        When uncommitted data exists it formerly was written to the root of the
        blob_directory and confused our packing strategy. We now use a separate
        temporary directory that is ignored while packing.
        """
        blob_storage = self.blob_storage
        database = self.database
        connection = database.open()
        root = connection.root()
        root['blob'] = Blob()
        connection.add(root['blob'])
        with root['blob'].open('w') as f:
            f.write(b'test')

        blob_storage.pack(time.time(), referencesf)

        transaction.abort()
        connection.close()
        database.close()
        blob_storage.close()

    def test_blob_file_permissions(self):
        blob_storage = self.blob_storage
        conn = ZODB.connection(blob_storage)
        conn.root.x = ZODB.blob.Blob(b'test')
        conn.transaction_manager.commit()

        # Blobs have the readability of their parent directories:
        READABLE = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        path = conn.root.x.committed()
        self.assertEqual(
            (os.stat(path).st_mode & READABLE),
            (os.stat(os.path.dirname(path)).st_mode & READABLE))

        # The committed file isn't writable:
        WRITABLE = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
        self.assertEqual(0, os.stat(path).st_mode & WRITABLE)
        conn.close()

    def test_loadblob_tmpstore(self):
        """
        This is a test for assuring that the TmpStore's loadBlob implementation
        falls back correctly to loadBlob on the backend.
        """

        # First, let's setup a regular database and store a blob:

        blob_storage = self.blob_storage
        database = self.database
        connection = database.open()
        root = connection.root()
        root['blob'] = Blob()
        connection.add(root['blob'])
        with root['blob'].open('w') as f:
            f.write(b'test')
        transaction.commit()
        blob_oid = root['blob']._p_oid
        tid = connection._storage.lastTransaction()

        # Now we open a database with a TmpStore in front:

        from ZODB.Connection import TmpStore
        tmpstore = TmpStore(blob_storage)

        # We can access the blob correctly:
        self.assertEqual(
            tmpstore.loadBlob(blob_oid, tid),
            blob_storage.loadBlob(blob_oid, tid))

        connection.close()
        blob_storage.close()
        tmpstore.close()
        database.close()

    def test_do_not_depend_on_cwd(self):
        bs = self.blob_storage
        here = os.getcwd()
        os.mkdir('evil')
        os.chdir('evil')
        db = DB(bs)
        conn = db.open()
        conn.root()['blob'] = ZODB.blob.Blob()
        with conn.root()['blob'].open('w') as f:
            f.write(b'data')
        transaction.commit()
        os.chdir(here)
        with conn.root()['blob'].open() as f:
            data = f.read()
        self.assertEqual(data, b'data')

        bs.close()

    def test_savepoint_cleanup(self):
        """Make sure savepoint data gets cleaned up."""

        bs = self.blob_storage
        tdir = bs.temporaryDirectory()
        os.listdir(tdir)
        self.assertEmpty(os.listdir(tdir))

        db = self.database
        conn = db.open()
        conn.root().b = ZODB.blob.Blob()
        with conn.root().b.open('w') as f: _ = f.write(b'initial')
        _ = transaction.savepoint()
        self.assertEqual(1, len(os.listdir(tdir)))

        transaction.abort()
        savepoint_dir = os.path.join(tdir, 'savepoint')
        self.assertFalse(
            os.path.exists(savepoint_dir) and len(os.listdir(savepoint_dir)) > 0)

        conn.root().b = ZODB.blob.Blob()
        with conn.root().b.open('w') as f: _ = f.write(b'initial')
        transaction.commit()
        with conn.root().b.open('w') as f: _ = f.write(b'1')
        _ = transaction.savepoint()
        transaction.abort()
        self.assertFalse(os.path.exists(savepoint_dir) and len(os.listdir(savepoint_dir)) > 0)

        db.close()


class BlobTestLayer(BaseTestLayer):

    __bases__ = ()
    __module__ = ''

    def __init__(self, name):
        BaseTestLayer.__init__(self, name)

    def setUp(self):
        self.here = os.getcwd()
        # In the past, this used dir=os.getcwd(), meaning we
        # could pollute our source directory if cleanup failed.
        # Why?
        self.tmp = tempfile.mkdtemp(self.__name__)
        os.chdir(self.tmp)

        # sigh. tearDown isn't called when a layer is run in a sub-process.
        atexit.register(clean, self.tmp)

    def tearDown(self):
        os.chdir(self.here)
        rmtree(self.tmp)

    def testSetUp(self):
        transaction.abort()

    def testTearDown(self):
        transaction.abort()


def clean(tmp):
    if os.path.isdir(tmp):
        rmtree(tmp)

def rmtree(path):
    """Remove a tree without causing Windows file access errors"""
    # copied from setupstack.py
    for cpath, dirs, files in os.walk(path, False):
        for fname in files:
            fname = os.path.join(cpath, fname)
            os.chmod(fname, stat.S_IWUSR)
            os.remove(fname)
        for dname in dirs:
            dname = os.path.join(cpath, dname)
            os.rmdir(dname)
    os.rmdir(path)


class TestBlobPackingHP(TestBlobPackHistoryPreservingMixin,
                        TestCase):
    pass

class TestBlobPackingHF(TestBlobPackHistoryFreeMixin,
                        TestCase):
    pass

class TestConnectionBlob(TestConnectionBlobMixin,
                         TestCase):
    pass

class TestBlobImportExport(TestBlobImportExportMixin,
                           TestCase):
    pass

class TestBlobTransaction(TestBlobTransactionMixin,
                          TestCase):
    pass

class TestBlobCache(TestBlobCacheMixin,
                    TestCase):
    pass

class TestBlobCacheExternalCleanup(TestBlobCacheExternalCleanupMixin,
                                   TestCase):
    pass

def storage_reusable_suite(prefix, factory,
                           test_blob_storage_recovery=False,
                           test_packing=False,
                           test_undo=True,
                           keep_history=True,
                           test_blob_cache=False,
                           large_blob_size=None,
                           storage_is_available=True):
    """Return a test suite for a generic IBlobStorage.

    Pass a factory taking a name and a blob directory name.
    """

    suite = unittest.TestSuite()

    def create_storage(name='data', blob_dir=None, zap=True, **kw):
        if blob_dir is None:
            blob_dir = '%s.blobs' % name
        storage = factory(name, blob_dir, **kw)
        if zap:
            storage.zap_all()
        return storage

    def add_test_based_on_test_class(klass, **attr):
        attr.update(create_storage=staticmethod(create_storage))
        new_class = klass.__class__(
            prefix + klass.__name__,
            (klass, ),
            attr,
        )
        new_class.__module__ = klass.__module__
        new_class = unittest.skipUnless(storage_is_available, str(storage_is_available))(new_class)
        suite.addTest(unittest.makeSuite(new_class))

    add_test_based_on_test_class(TestBlobTransaction)
    add_test_based_on_test_class(TestBlobImportExport)
    add_test_based_on_test_class(TestThingsPreviouslyDocTests)
    add_test_based_on_test_class(TestConnectionBlob)
    if test_blob_cache:
        add_test_based_on_test_class(TestBlobCache)
        add_test_based_on_test_class(TestBlobCacheExternalCleanup)
    if test_packing:
        base = TestBlobPackingHP if keep_history else TestBlobPackingHF
        add_test_based_on_test_class(base)
    if test_blob_storage_recovery:
        add_test_based_on_test_class(RecoveryBlobStorage)
    if test_undo:
        add_test_based_on_test_class(BlobUndoTests)
    if large_blob_size:
        add_test_based_on_test_class(LargeBlobTest, testsize=large_blob_size)

    suite.layer = BlobTestLayer('BlobTests')

    return suite
