"""
Transaction support for Blobs tests
"""
from __future__ import absolute_import
from __future__ import print_function

import os

from ZODB.interfaces import BlobError
from ZODB.interfaces import IStorageUndoable
from ZODB.blob import Blob
from ZODB.POSException import ConflictError
from ZODB.POSException import ConnectionStateError
from ZODB.POSException import POSKeyError

import transaction

from relstorage.blobhelper.interfaces import IAuthoritativeBlobHelper
from relstorage.blobhelper.interfaces import ICachedBlobHelper
from . import TestBlobMixin

class TestBlobTransactionMixin(TestBlobMixin):

    DATA1 = b'this is blob 1'

    def _make_blob(self, data=DATA1):
        blob1 = Blob()
        with blob1.open('w') as file:
            file.write(data)
        return blob1

    def _make_and_commit_blob(self, data=DATA1, close=True):
        """
        Stores a blob at root['blob1']
        """
        blob = self._make_blob(data)
        conn = self.database.open()
        root1 = conn.root()
        root1['blob1'] = blob
        transaction.commit()
        if close:
            conn.close()
            return blob
        return blob, conn

    def _check_blob_committed(self, blob, uncommitted_fname=None):
        self.assertIsNone(blob._p_blob_uncommitted)
        if uncommitted_fname:
            self.assertFalse(os.path.exists(uncommitted_fname), uncommitted_fname)

    def _check_blob_uncommitted(self, blob):
        uncommitted_fname = blob._p_blob_uncommitted
        self.assertTrue(os.path.exists(uncommitted_fname), uncommitted_fname)
        return uncommitted_fname

    def _check_blob_contents(self, blob, expected, mode='r'):
        with blob.open(mode) as fp:
            data = fp.read()
        self.assertEqual(data, expected)

    def test_abort_transaction(self):
        # Putting a Blob into a Connection works like any other Persistent object::
        blob1 = self._make_blob()
        conn = self.database.open()
        root1 = conn.root()
        self.assertNotIn('blob1', root1)
        root1['blob1'] = blob1
        self.assertIn('blob1', root1)
        self.assertEqual(root1._p_status, 'changed')
        # Aborting a blob add leaves the blob unchanged:

        transaction.abort()
        # Note that debugging with something like pudb that automatically
        # displays local variables will invalidate this: it will already
        # have been loaded again.
        self.assertEqual(root1._p_status, 'ghost')
        self.assertNotIn('blob1', root1)

        self.assertIsNone(blob1._p_oid)
        self.assertIsNone(blob1._p_jar)
        self._check_blob_contents(blob1, self.DATA1)

        # It doesn't clear the file because there is no previously committed version:
        uncommitted_fname = self._check_blob_uncommitted(blob1)

        # Let's put the blob back into the root and commit the change:
        root1['blob1'] = blob1
        transaction.commit()

        # Now, if we make a change and abort it, we'll return to the committed
        # state:
        new_data = b'this is new blob 1'
        with blob1.open('w') as file:
            file.write(b'this is new blob 1')
        self._check_blob_contents(blob1, new_data)
        uncommitted_fname = self._check_blob_uncommitted(blob1)

        transaction.abort()
        self._check_blob_committed(blob1, uncommitted_fname)
        self._check_blob_contents(blob1, self.DATA1)
        conn.close()

    def test_multiple_open_handles(self):
        # Opening a blob gives us a filehandle.  Getting data out of the
        # resulting filehandle is accomplished via the filehandle's read method::
        self._make_and_commit_blob()

        connection2 = self.database.open()
        root2 = connection2.root()
        blob1a = root2['blob1']

        blob1afh1 = blob1a.open("r")
        self.assertEqual(blob1afh1.read(), self.DATA1)


        # Let's make another filehandle for read only to blob1a. Each file
        # handle has a reference to the (same) underlying blob::
        blob1afh2 = blob1a.open("r")
        self.assertIs(blob1afh2.blob, blob1afh1.blob)

        # Let's close the first filehandle we got from the blob::
        blob1afh1.close()

        # Let's abort this transaction, and ensure that the other filehandle that we
        # opened are still open::
        transaction.abort()
        self.assertEqual(blob1afh2.read(), self.DATA1)
        blob1afh2.close()

        connection2.close()

    def test_append_makes_dirty(self):
        # If we open a blob for append, writing any number of bytes to the
        # blobfile should result in the blob being marked "dirty" in the
        # connection (we just aborted above, so the object should be "clean"
        # when we start)::
        blob, conn = self._make_and_commit_blob(close=False)
        self.assertFalse(blob._p_changed)
        self._check_blob_contents(blob, self.DATA1)
        with blob.open('a') as blob1afh3:
            self.assertTrue(blob._p_changed)
            blob1afh3.write(b'woot!')

        transaction.abort()
        conn.close()

    def test_multiple_blobs(self):
        # We can open more than one blob object during the course of a single
        # transaction, and we can write to a single blob using
        # multiple handles.
        blob1, conn1 = self._make_and_commit_blob(close=False)
        root = conn1.root()
        with blob1.open('a') as f:
            f.write(b'woot')

        blob1_second_object = root['blob1']
        self.assertEqual(blob1_second_object._p_oid, blob1._p_oid)
        self.assertIs(blob1_second_object, blob1)

        with blob1_second_object.open('a') as f:
            f.write(b'!')

        blob2 = Blob()
        blob2_contents = b'this is blob 2'
        with blob2.open('w') as f:
            f.write(blob2_contents)
            root['blob2'] = blob2
        transaction.commit()

        # Since we committed the current transaction above, the aggregate
        # changes we've made to blob, blob1a (these refer to the same object) and
        # blob2 (a different object) should be evident::
        blob1_contents = self.DATA1 + b'woot!'
        self._check_blob_contents(blob1, blob1_contents)
        self._check_blob_contents(blob1_second_object, blob1_contents)
        self._check_blob_contents(blob2, blob2_contents)

        transaction.abort()
        conn1.close()

        if IStorageUndoable.providedBy(self.blob_storage):
            # Whether or not we're using a shared or unshared
            # blob-dir, when we keep history we will have three blob
            # files on disk: two revisions of blob1, and one revision
            # of blob2
            self.assertEqual(3, self._count_blobs_in_directory())
        else:
            # If we are a shared blob directory, we didn't remove anything;
            # that waits until pack time.
            if IAuthoritativeBlobHelper.providedBy(self.blob_storage.blobhelper):
                self.assertEqual(3, self._count_blobs_in_directory())
            else:
                # We will just have two blobs on disk. The earlier revision
                # was automatically removed.
                self.assertTrue(ICachedBlobHelper.providedBy(self.blob_storage.blobhelper))
                self.assertEqual(2, self._count_blobs_in_directory())

    def test_persistent_blob_handle(self):
        # We shouldn't be able to persist a blob filehandle at commit time
        # (although the exception which is raised when an object cannot be
        # pickled appears to be particulary unhelpful for casual users at the
        # moment)::
        blob1, conn = self._make_and_commit_blob(close=False)
        root = conn.root()
        with blob1.open('r') as f:
            root['wontwork'] = f
            with self.assertRaises(TypeError):
                transaction.commit()

        # Abort for good measure::

        transaction.abort()
        conn.close()

    def test_write_conflict(self):
        # Attempting to change a blob simultaneously from two different
        # connections should result in a write conflict error::
        self._make_and_commit_blob()

        tm1 = transaction.TransactionManager()
        tm2 = transaction.TransactionManager()
        database = self.database
        conn3 = database.open(transaction_manager=tm1)
        conn4 = database.open(transaction_manager=tm2)
        root3 = conn3.root()
        root4 = conn4.root()
        blob1c3 = root3['blob1']
        blob1c4 = root4['blob1']
        conn3_data = b'this is from connection 3'
        conn4_data = b'this is from connection 4'
        with blob1c3.open('w') as blob1c3fh1:
            blob1c3fh1.write(conn3_data)
        with blob1c4.open('w') as blob1c4fh1:
            blob1c4fh1.write(conn4_data)
        tm1.commit()
        self._check_blob_contents(root3['blob1'], conn3_data)
        with self.assertRaises(ConflictError):
            tm2.commit()

        # After the conflict, the winning transaction's result is visible on both
        # connections::
        tm2.abort()
        self._check_blob_contents(root4['blob1'], conn3_data)

        conn3.close()
        conn4.close()

    def test_commit_while_open(self):
        # You can't commit a transaction while blob files are open:
        conn = self.database.open()
        blob = self._make_blob()
        conn.root()['blob1'] = blob
        f = blob.open('w')
        with self.assertRaisesRegex(ValueError, "Can't commit with opened blobs"):
            transaction.commit()

        f.close()
        transaction.abort()

        # Open for reading, if we haven't changed anything, is fine
        f = blob.open('r')
        transaction.commit()
        f.close()

        # But not if we've made a change
        blob.open('w').close()
        f = blob.open('r')
        with self.assertRaisesRegex(ValueError, "Can't commit with opened blobs"):
            transaction.commit()
        f.close()
        transaction.abort()
        conn.close()

    def _list_savepoints(self):
        blob_dir = self.blob_storage.blobhelper.blob_dir
        return [name for name in os.listdir(os.path.join(blob_dir, 'tmp'))
                if name.startswith('savepoint')]

    def test_optimistic_savepoints(self):
        # We do support optimistic savepoints:
        blob, conn = self._make_and_commit_blob(close=False)
        data2 = b" Here's more data."
        with blob.open("a") as blob_fh:
            blob_fh.write(data2)
        self._check_blob_contents(blob, self.DATA1 + data2)

        _ = transaction.savepoint(optimistic=True)
        self._check_blob_contents(blob, self.DATA1 + data2)

        # Savepoints store the blobs in temporary directories in the
        # temporary directory of the blob storage:
        self.assertEqual(
            1,
            len(self._list_savepoints()))
        # After committing the transaction, the temporary savepoint
        # files are moved to the committed location again:

        transaction.commit()
        self.assertEmpty(self._list_savepoints())
        transaction.abort()
        conn.close()

    def test_non_optimistic_savepoints(self):
        # We support non-optimistic savepoints too:

        blob, conn = self._make_and_commit_blob(close=False)
        new_data = b' More data.'
        with blob.open('a') as f:
            f.write(new_data)
        self._check_blob_contents(blob, self.DATA1 + new_data)

        savepoint = transaction.savepoint()

        # Again, the savepoint creates a new savepoints directory:
        self.assertEqual(1, len(self._list_savepoints()))

        with blob.open("w") as f:
            f.write(b"Completely unrelated.")
        savepoint.rollback()

        self._check_blob_contents(blob, self.DATA1 + new_data)
        transaction.abort()

        # The savepoint blob directory gets cleaned up on an abort:
        self.assertEmpty(self._list_savepoints())
        conn.close()

    def test_reading_outside_transaction(self):
        # Reading Blobs outside of a transaction
        # --------------------------------------

        # If you want to read from a Blob outside of transaction boundaries (e.g. to
        # stream a file to the browser), committed method to get the name of a
        # file that can be opened.
        data = b"I'm a happy blob."
        blob = self._make_and_commit_blob(data)

        # Of course, we can't do that if the connection has been completely closed
        with self.assertRaises(ConnectionStateError):
            blob.committed()

        conn = self.database.open()

        path = blob.committed()
        with open(path, 'rb') as fp:
            read = fp.read()
        self.assertEqual(read, data)


        # We can also read committed data by calling open with a 'c' flag:
        self._check_blob_contents(blob, data, 'c')

        # This doesn't prevent us from opening the blob for writing:

        with blob.open('w') as file:
            file.write(b'x')
        self._check_blob_contents(blob, b'x')
        # This doesn't change the committed contents
        with open(path, 'rb') as fp:
            read = fp.read()
        self.assertEqual(read, data)

        # Although we can't use the 'c' flag anymore
        with self.assertRaisesRegex(BlobError, 'Uncommitted changes'):
            self._check_blob_contents(blob, data, 'c')
        transaction.abort()
        conn.close()

    def test_committed_when_uncommitted(self):
        # An exception is raised if we call committed on a blob that has
        # uncommitted changes:
        blob = Blob()
        with self.assertRaisesRegex(BlobError, "Uncommitted changes"):
            blob.committed()

        with self.assertRaisesRegex(BlobError, "Uncommitted changes"):
            blob.open('c')

        blob = self._make_blob()
        conn = self.database.open()
        root = conn.root()
        root['blob'] = blob
        with self.assertRaisesRegex(BlobError, "Uncommitted changes"):
            blob.committed()

        with self.assertRaisesRegex(BlobError, "Uncommitted changes"):
            blob.open('c')

        transaction.savepoint()
        with self.assertRaisesRegex(BlobError, "Uncommitted changes"):
            blob.committed()
        with self.assertRaisesRegex(BlobError, "Uncommitted changes"):
            blob.open('c')

        transaction.commit()
        self._check_blob_contents(blob, self.DATA1, 'c')

    def test_committed_cannot_open_for_write(self):
        # You can't open a committed blob file for writing:
        blob, conn = self._make_and_commit_blob(close=False)
        # Raises IOError on Python2, and PersmissionError on Python 3,
        # but PermissionError extends OSError (which is the new name for IOError)
        with self.assertRaises(IOError):
            open(blob.committed(), 'w')
        conn.close()

    def test_tpc_abort(self):
        # If a transaction is aborted in the middle of 2-phase commit, any data
        # stored are discarded.
        blob_storage = self.blob_storage
        blob, conn = self._make_and_commit_blob(close=False)

        olddata, oldserial = blob_storage.load(blob._p_oid, '')
        t = transaction.get()
        blob_storage.tpc_begin(t)
        with open('blobfile', 'wb') as file:
            file.write(b'This data should go away')
        blob_storage.storeBlob(blob._p_oid, oldserial, olddata, 'blobfile',
                               '', t)
        new_oid = blob_storage.new_oid()
        with open('blobfile2', 'wb') as file:
            file.write(b'This data should go away too')
        blob_storage.storeBlob(new_oid, b'\0' * 8, olddata, 'blobfile2',
                               '', t)
        self.assertFalse(blob_storage.tpc_vote(t))

        blob_storage.tpc_abort(t)

        # Now, the serial for the existing blob should be the same:

        self.assertEqual(blob_storage.load(blob._p_oid, ''),
                         (olddata, oldserial))

        # The old data should be unaffected:
        with open(blob_storage.loadBlob(blob._p_oid, oldserial), 'rb') as fp:
            data = fp.read()
        self.assertEqual(data, self.DATA1)


        # Similarly, the new object wasn't added to the storage:
        with self.assertRaises(POSKeyError):
            blob_storage.load(new_oid, '')

        conn.close()
