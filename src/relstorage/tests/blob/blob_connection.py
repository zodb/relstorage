"""
Connection support for Blobs tests
"""
from __future__ import absolute_import
from __future__ import print_function

from ZODB.interfaces import IBlob
from ZODB.blob import Blob
import transaction

from . import TestBlobMixin

class TestConnectionBlobMixin(TestBlobMixin):

    def testPuttingInConnection(self):
        # Putting a Blob into a Connection works like every other object:
       # Connections handle Blobs specially. To demonstrate that, we
        # first need a Blob with some data:

        self.blob = blob = Blob()
        with blob.open("w") as f:
            f.write(b"I'm a happy Blob.")

        connection = self.database.open()
        root = connection.root()
        root['myblob'] = blob
        transaction.commit()
        connection.close()

    def testCommitWithoutOpen(self):
        # We can also commit a transaction that seats a blob into place without:
        # calling the blob's open method:
        transaction.begin()
        connection = self.database.open()
        anotherblob = Blob()
        root = connection.root()
        root['anotherblob'] = anotherblob
        transaction.commit()
        connection.close()

    def testGettingStuffOut(self):
        # Getting stuff out of there works similarly:
        self.testPuttingInConnection()

        transaction2 = transaction.TransactionManager()
        connection2 = self.database.open(transaction_manager=transaction2)
        root = connection2.root()
        blob2 = root['myblob']
        self.assertTrue(IBlob.providedBy(blob2))

        with blob2.open("r") as f:
            data = f.read()
        self.assertEqual(data, b"I'm a happy Blob.")
        transaction2.abort()
        connection2.close()

    def testMVCC(self):
        # MVCC also works.
        self.testPuttingInConnection()

        connection = self.database.open()
        transaction3 = transaction.TransactionManager()
        connection3 = self.database.open(transaction_manager=transaction3)
        with connection.root()['myblob'].open('w') as f:
            f.write(b'I am an ecstatic Blob.')
        transaction.commit()
        with connection3.root()['myblob'].open('r') as f:
            data = f.read()
        self.assertEqual(data, b"I'm a happy Blob.")

        transaction3.abort()
        connection.close()
        connection3.close()
