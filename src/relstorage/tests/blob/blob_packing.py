"""
Packing support for blob data
=============================
"""
from __future__ import absolute_import
from __future__ import print_function

import os

from ZODB.serialize import referencesf
from ZODB.blob import Blob
import transaction

from . import TestBlobMixin

class TestBlobPackHistoryPreservingMixin(TestBlobMixin):
    """
    We need a database with an undoing blob supporting storage.
    """

    def setUp(self):
        super(TestBlobPackHistoryPreservingMixin, self).setUp()
        # The 8 bytes for each TID up to BLOB_REVISION_COUNT
        self.tids = []
        # The timestamps taken just *before* the corresponding
        # TID. These are used as times to pack to.
        self.times = []
        self.oid = None
        self.fns = []
        self._populate()

    #: How many transactions we make with the blob
    BLOB_REVISION_COUNT = 5

    def _populate(self):
        """
        Put some revisions of a blob object in our database and on the
        filesystem.
        """
        from ZODB.utils import u64 as bytes8_to_int64

        connection1 = self.database.open()
        root = connection1.root()

        tids = self.tids = []
        times = self.times = []
        blob = Blob()

        for i in range(self.BLOB_REVISION_COUNT):
            transaction.begin()
            with blob.open('w') as f:
                f.write(b'this is blob data ' + str(i).encode())
            if 'blob' not in root:
                root['blob'] = blob
            transaction.commit()

            blob._p_activate()
            tid = blob._p_serial
            tids.append(tid)
            tid_int = bytes8_to_int64(tid)

            times.append(tid_int - 1)

        blob._p_activate()

        self.oid = oid = root['blob']._p_oid
        fshelper = self.blob_storage.blobhelper.fshelper
        self.fns = [fshelper.getBlobFilename(oid, x) for x in tids]
        connection1.close()

    def _checkFirstCountNotExist(self, count, fns):
        for _ in range(count):
            fn = fns.pop(0)
            self.assertFalse(os.path.exists(fn), fn)
        return fns

    def _checkAllExist(self, fns):
        for f in fns:
            self.assertTrue(os.path.exists(f), f)

    def _pack_at_time_index(self, time_index=0, count_not_exist=0):
        if time_index is None:
            # Use now
            packtime = -1
        else:
            packtime = self.times[time_index]
        self.blob_storage.pack(packtime, referencesf)
        fns = list(self.fns)
        if count_not_exist:
            fns = self._checkFirstCountNotExist(count_not_exist, fns)
        self._checkAllExist(fns)

    def testPack(self):
        # Initially all 5 revisions are present
        self._checkAllExist(self.fns)

        # Packing before the first time changes nothing
        self._pack_at_time_index(0, count_not_exist=0)

        # Ditto for the second
        self._pack_at_time_index(1, count_not_exist=0)

        # Now we start to lose the first one
        self._pack_at_time_index(2, count_not_exist=1)

        # And the second
        self._pack_at_time_index(3, count_not_exist=2)

        # And the third
        self._pack_at_time_index(4, count_not_exist=3)

        # Packing to now leaves only one revision.
        self._pack_at_time_index(None, count_not_exist=4)

        # If we delete the object and do a pack, it should get rid of the most current
        # revision as well as the entire directory:
        dir_name = os.path.split(self.fns[0])[0]
        conn = self.database.open()
        transaction.begin()
        root = conn.root()
        del root['blob']
        transaction.commit()
        conn.close()
        self._pack_at_time_index(None, count_not_exist=5)
        self.assertFalse(os.path.exists(dir_name))

class TestBlobPackHistoryFreeMixin(TestBlobPackHistoryPreservingMixin):
    def _checkFirstCountNotExist(self, count, fns):
        # We don't actually remove anything, until we remove
        # everything; there's really only one file name.
        remaining_fns = fns
        if count == self.BLOB_REVISION_COUNT:
            s = super(TestBlobPackHistoryFreeMixin, self)
            remaining_fns = s._checkFirstCountNotExist(count, fns)
        return remaining_fns
