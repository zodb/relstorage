from __future__ import print_function
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

from ZODB.blob import Blob
from ZODB.DB import DB
import doctest

import atexit
import collections
import datetime
import os
import random
import re
import stat
import struct
import sys
import tempfile
import time
import transaction
import unittest
import ZODB.blob
import ZODB.interfaces
from relstorage.tests.RecoveryStorage import IteratorDeepCompare
import ZODB.tests.StorageTestBase
import ZODB.tests.util
from zope.testing import renormalizing


from hashlib import md5


def new_time():
    """Create a _new_ time stamp.

    This method also makes sure that after retrieving a timestamp that was
    *before* a transaction was committed, that at least one second passes so
    the packing time actually is before the commit time.

    """
    now = new_time = time.time()
    while new_time <= now:
        new_time = time.time()
    time.sleep(1)
    return new_time


with open(__file__, 'rb') as _f:
    # Just use the this module as the source of our data
    # Capture it at import time because test cases may
    # chdir(), and we may not have an absolute path in __file__,
    # depending on how they are run.
    _random_file_data = _f.read().replace(b'\n', b'').split()
del _f


def random_file(size, fd):
    """Create a random data of at least the given size, writing to fd.

    See http://jessenoller.com/2008/05/30/making-re-creatable-random-data-files-really-fast-in-python/
    for the technique used.

    Returns the md5 sum of the file contents for easy comparison.

    """
    def fdata():
        seed = "1092384956781341341234656953214543219"
        words = _random_file_data
        a = collections.deque(words)
        b = collections.deque(seed)
        while True:
            yield b' '.join(list(a)[0:1024])
            a.rotate(int(b[0]))
            b.rotate(1)
    datagen = fdata()
    bytes = 0
    md5sum = md5()
    while bytes < size:
        data = next(datagen)
        md5sum.update(data)
        fd.write(data)
        bytes += len(data)
    return md5sum.hexdigest()


def md5sum(fd):
    md5sum = md5()
    blocksize = md5sum.block_size << 8
    for data in iter(lambda: fd.read(blocksize), b''):
        md5sum.update(data)
    return md5sum.hexdigest()


def sizeof_fmt(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0


class BlobTestBase(ZODB.tests.StorageTestBase.StorageTestBase):

    def setUp(self):
        ZODB.tests.StorageTestBase.StorageTestBase.setUp(self)
        self._storage = self.create_storage()


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

    def setUp(self):
        BlobTestBase.setUp(self)
        self._dst = self.create_storage('dest')

    def tearDown(self):
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
        conn.root()[1] = ZODB.blob.Blob()
        transaction.commit()
        conn.root()[2] = ZODB.blob.Blob()
        with conn.root()[2].open('w') as f:
            f.write(b'some data')
        transaction.commit()
        conn.root()[3] = ZODB.blob.Blob()
        with conn.root()[3].open('w') as f:
            f.write(
                (b''.join(struct.pack(">I", random.randint(0, (1<<32)-1))
                          for i in range(random.randint(10000, 20000)))
                )[:-random.randint(1, 4)]
            )
        transaction.commit()
        conn.root()[2] = ZODB.blob.Blob()
        with conn.root()[2].open('w') as f:
            f.write(b'some other data')
        transaction.commit()
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)

        conn.close()
        db.close()


class LargeBlobTest(BlobTestBase):
    """Test large blob upload and download.

    Note that this test excercises the blob storage and only makes sense
    when shared_blob_support=False.

    """
    level = 2 # Only run when selecting -a 2 or higher, or --all
    testsize = 0 # Set on the auto-generated parent class

    def _log(self, msg):
        print('%s [%s]: %s' % (
            datetime.datetime.now().isoformat(' '),
            self.__class__.__name__, msg))

    def testLargeBlob(self):
        # Large blobs are chunked into multiple pieces, we want to know
        # if they come out the same way they went in.
        db = DB(self._storage)
        conn = db.open()
        blob = conn.root()[1] = ZODB.blob.Blob()
        size = sizeof_fmt(self.testsize)
        self._log('Creating %s blob file' % size)
        blob_file = blob.open('w')
        signature = random_file(self.testsize, blob_file)
        blob_file.close()
        self._log('Committing %s blob file' % size)
        transaction.commit()
        conn.close()

        # Clear the cache
        for base, _dir, files in os.walk('.'):
            for f in files:
                if f.endswith('.blob'):
                    ZODB.blob.remove_committed(os.path.join(base, f))

        # Re-download blob
        self._log('Caching %s blob file' % size)
        conn = db.open()
        with conn.root()[1].open('r') as blob:
            self._log('Creating signature for %s blob cache' % size)
            self.assertEqual(md5sum(blob), signature)

        conn.close()
        db.close()


def packing_with_uncommitted_data_non_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> from ZODB.serialize import referencesf

    >>> blob_storage = create_storage()
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> with root['blob'].open('w') as f: _ = f.write(b'test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> transaction.abort()
    >>> connection.close()
    >>> blob_storage.close()
    >>> database.close()

    """

def packing_with_uncommitted_data_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> import transaction
    >>> from ZODB.serialize import referencesf

    >>> blob_storage = create_storage()
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> with root['blob'].open('w') as f: _ = f.write(b'test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> transaction.abort()
    >>> connection.close()
    >>> database.close()
    >>> blob_storage.close()
    """


def secure_blob_directory():
    """
    This is a test for secure creation and verification of secure settings of
    blob directories.

    >>> blob_storage = create_storage(blob_dir='blobs')

    Two directories are created:

    >>> os.path.isdir('blobs')
    True
    >>> tmp_dir = os.path.join('blobs', 'tmp')
    >>> os.path.isdir(tmp_dir)
    True

    They are only accessible by the owner:

    >>> os.stat('blobs').st_mode
    16832
    >>> os.stat(tmp_dir).st_mode
    16832

    These settings are recognized as secure:

    >>> blob_storage.fshelper.isSecure('blobs')
    True
    >>> blob_storage.fshelper.isSecure(tmp_dir)
    True

    After making the permissions of tmp_dir more liberal, the directory is
    recognized as insecure:

    >>> os.chmod(tmp_dir, 0o40711)
    >>> blob_storage.fshelper.isSecure(tmp_dir)
    False

    Clean up:

    >>> blob_storage.close()

    """

# On windows, we can't create secure blob directories, at least not
# with APIs in the standard library, so there's no point in testing
# this.
if sys.platform == 'win32':
    del secure_blob_directory

def loadblob_tmpstore():
    """
    This is a test for assuring that the TmpStore's loadBlob implementation
    falls back correctly to loadBlob on the backend.

    First, let's setup a regular database and store a blob:

    >>> blob_storage = create_storage()
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> with root['blob'].open('w') as f: _ = f.write(b'test')
    >>> import transaction
    >>> transaction.commit()
    >>> blob_oid = root['blob']._p_oid
    >>> tid = connection._storage.lastTransaction()

    Now we open a database with a TmpStore in front:

    >>> from ZODB.Connection import TmpStore
    >>> tmpstore = TmpStore(blob_storage)

    We can access the blob correctly:

    >>> tmpstore.loadBlob(blob_oid, tid) == blob_storage.loadBlob(blob_oid, tid)
    True

    Clean up:

    >>> connection.close()
    >>> blob_storage.close()
    >>> tmpstore.close()
    >>> database.close()
    """

def do_not_depend_on_cwd():
    """
    >>> bs = create_storage()
    >>> here = os.getcwd()
    >>> os.mkdir('evil')
    >>> os.chdir('evil')
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root()['blob'] = ZODB.blob.Blob()
    >>> with conn.root()['blob'].open('w') as f: _ = f.write(b'data')
    >>> transaction.commit()
    >>> os.chdir(here)
    >>> with conn.root()['blob'].open() as f: f.read()
    'data'

    >>> bs.close()
    """

if False:
    # ZODB 3.8 fails this test because it creates a single
    # 'savepoints' directory.
    def savepoint_isolation():
        """Make sure savepoint data is distinct accross transactions

        >>> bs = create_storage()
        >>> db = DB(bs)
        >>> conn = db.open()
        >>> conn.root().b = ZODB.blob.Blob()
        >>> conn.root().b.open('w').write('initial')
        >>> transaction.commit()
        >>> conn.root().b.open('w').write('1')
        >>> _ = transaction.savepoint()
        >>> tm = transaction.TransactionManager()
        >>> conn2 = db.open(transaction_manager=tm)
        >>> conn2.root().b.open('w').write('2')
        >>> _ = tm.savepoint()
        >>> conn.root().b.open().read()
        '1'
        >>> conn2.root().b.open().read()
        '2'
        >>> transaction.abort()
        >>> tm.commit()
        >>> conn.sync()
        >>> conn.root().b.open().read()
        '2'
        >>> db.close()
        """

def savepoint_cleanup():
    """Make sure savepoint data gets cleaned up.

    >>> bs = create_storage()
    >>> tdir = bs.temporaryDirectory()
    >>> os.listdir(tdir)
    []

    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root().b = ZODB.blob.Blob()
    >>> with conn.root().b.open('w') as f: _ = f.write(b'initial')
    >>> _ = transaction.savepoint()
    >>> len(os.listdir(tdir))
    1
    >>> transaction.abort()
    >>> savepoint_dir = os.path.join(tdir, 'savepoint')
    >>> os.path.exists(savepoint_dir) and len(os.listdir(savepoint_dir)) > 0
    False
    >>> conn.root().b = ZODB.blob.Blob()
    >>> with conn.root().b.open('w') as f: _ = f.write(b'initial')
    >>> transaction.commit()
    >>> with conn.root().b.open('w') as f: _ = f.write(b'1')
    >>> _ = transaction.savepoint()
    >>> transaction.abort()
    >>> os.path.exists(savepoint_dir) and len(os.listdir(savepoint_dir)) > 0
    False

    >>> db.close()
    """


def setUp(test):
    ZODB.tests.util.setUp(test)

def tearDown(test):
    ZODB.tests.util.tearDown(test)


class MinimalTestLayer(object):

    __bases__ = ()
    __module__ = ''

    def __init__(self, name):
        self.__name__ = name

    def setUp(self):
        self.here = os.getcwd()
        self.tmp = tempfile.mkdtemp(self.__name__, dir=os.getcwd())
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
    for path, dirs, files in os.walk(path, False):
        for fname in files:
            fname = os.path.join(path, fname)
            os.chmod(fname, stat.S_IWUSR)
            os.remove(fname)
        for dname in dirs:
            dname = os.path.join(path, dname)
            os.rmdir(dname)
    os.rmdir(path)

checker = renormalizing.RENormalizing([
    # Python 3 bytes add a "b".
    (re.compile(r'b(".*?")'), r"\1"),
    (re.compile(r"b('.*?')"), r"\1"),
    # Windows shows result from 'u64' as long?
    (re.compile(r"(\d+)L"), r"\1"),
    # Python 3 adds module name to exceptions.
    (re.compile("ZODB.POSException.ConflictError"), r"ConflictError"),
    (re.compile("ZODB.POSException.POSKeyError"), r"POSKeyError"),
    (re.compile("ZODB.POSException.ReadConflictError"), r"ReadConflictError"),
    (re.compile("ZODB.POSException.Unsupported"), r"Unsupported"),
    (re.compile("ZODB.interfaces.BlobError"), r"BlobError"),
    # XXX document me
    (re.compile(r'\%(sep)s\%(sep)s' % dict(sep=os.path.sep)), '/'),
    (re.compile(r'\%(sep)s' % dict(sep=os.path.sep)), '/'),
])

try:
    file_type = file
except NameError:
    # Py3: Python 3 does not have a file type.
    import io
    file_type = io.BufferedReader

def storage_reusable_suite(prefix, factory,
                           test_blob_storage_recovery=False,
                           test_packing=False,
                           test_undo=True,
                           keep_history=True,
                           pack_test_name='blob_packing.txt',
                           test_blob_cache=False,
                           large_blob_size=None):
    """Return a test suite for a generic IBlobStorage.

    Pass a factory taking a name and a blob directory name.
    """
    def setup(test):
        setUp(test)
        def create_storage(name='data', blob_dir=None, **kw):
            if blob_dir is None:
                blob_dir = '%s.bobs' % name
            return factory(name, blob_dir, **kw)

        test.globs['create_storage'] = create_storage
        test.globs['file_type'] = file_type

    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite(
        "blob_connection.txt", "blob_importexport.txt",
        "blob_transaction.txt",
        setUp=setup, tearDown=tearDown,
        optionflags=doctest.ELLIPSIS,
        checker=checker,
        ))
    if test_packing:
        suite.addTest(doctest.DocFileSuite(
            pack_test_name,
            setUp=setup, tearDown=tearDown,
            checker=checker,
            ))
    if test_blob_cache:
        suite.addTest(doctest.DocFileSuite(
            "blob_cache.test",
            setUp=setup, tearDown=tearDown,
            checker=checker,
        ))
    suite.addTest(doctest.DocTestSuite(
        setUp=setup, tearDown=tearDown,
        checker=checker,
        ))

    def create_storage(self, name='data', blob_dir=None, **kw):
        if blob_dir is None:
            blob_dir = '%s.bobs' % name
        return factory(name, blob_dir, **kw)

    def add_test_based_on_test_class(class_, **attr):
        attr.update(create_storage=create_storage)
        new_class = class_.__class__(
            prefix+class_.__name__, (class_, ),
            attr,
            )
        suite.addTest(unittest.makeSuite(new_class))

    if test_blob_storage_recovery:
        add_test_based_on_test_class(RecoveryBlobStorage)
    if test_undo:
        add_test_based_on_test_class(BlobUndoTests)
    if large_blob_size:
        add_test_based_on_test_class(LargeBlobTest, testsize=large_blob_size)

    suite.layer = MinimalTestLayer(prefix+'BlobTests')

    return suite
