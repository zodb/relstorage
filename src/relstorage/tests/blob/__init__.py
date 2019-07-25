"""This package contains test code copied from ZODB 3.9 with minor alterations.

It is especially useful for testing RelStorage + ZODB 3.8.
"""

from nti.testing.time import MonotonicallyIncreasingTimeLayerMixin

from ZODB.DB import DB
from ZODB.tests.util import setUp
from ZODB.tests.util import tearDown


class TestBlobMixin(object):

    DEFAULT_BLOB_STORAGE_KWARGS = {}

    def setUp(self):
        super(TestBlobMixin, self).setUp()
        setUp(self)
        self._timer = MonotonicallyIncreasingTimeLayerMixin()
        self._timer.testSetUp()
        try:
            self.blob_storage = self.create_storage(**self.DEFAULT_BLOB_STORAGE_KWARGS)
        except:
            # If setUp() raises an exception, tearDown is never called.
            # That's bad: ZODB.tests.util.setUp() changes directories and
            # monkeys with the contents of the stdlib tempfile.
            tearDown(self)
            raise
        self.database = DB(self.blob_storage)

    def tearDown(self):
        self.database.close()
        self.blob_storage.close()
        self._timer.testTearDown()
        tearDown(self)
        super(TestBlobMixin, self).tearDown()
