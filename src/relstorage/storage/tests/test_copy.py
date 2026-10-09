# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
"""
Tests for copyTransactionsFrom helpers.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import os
import pickle
import shutil
import tempfile

from zope.interface import implementer
from ZODB.blob import Blob
from ZODB.interfaces import IStorageCurrentRecordIteration
from ZODB.utils import p64

from relstorage.storage.copy import Copy
from relstorage.tests import TestCase


class _FakeLoadConnection(object):
    """
    Stands in for a RelStorage load connection.

    Only the isolated-connection support needed by the history-free
    copier is implemented.
    """

    def __init__(self, isolated_cursor):
        self.isolated_cursor = isolated_cursor

    @contextlib.contextmanager
    def isolated_connection(self):
        yield self.isolated_cursor


@implementer(IStorageCurrentRecordIteration)
class MockRecordIternextSource(object):
    """
    A fake history-free source storage yielding blob records.

    It emulates drivers (like PyMySQL) that cannot execute a second
    query on the load connection while its server-side record
    iteration cursor is still active: the regular
    ``openCommittedBlobFile()`` entry point, which uses the load
    connection, refuses to run while an iteration is in progress.
    """

    def __init__(self, records):
        self._records = list(records)
        self.iteration_open = False
        self.openCommittedBlobFile_calls = []
        self.isolated_cursor = object()
        # pylint:disable-next=invalid-name
        self._load_connection = _FakeLoadConnection(self.isolated_cursor)
        self.blobhelper = self.FakeBlobHelper(self)

    class FakeBlobHelper(object):
        def __init__(self, storage):
            self.storage = storage
            self.calls = []

        def openCommittedBlobFile(self, cursor, oid, serial, blob=None): # pylint:disable=unused-argument
            assert cursor is self.storage.isolated_cursor
            self.calls.append((oid, serial))
            return open(self.storage.blob_path, 'rb') # pylint:disable=consider-using-with

    blob_path = None

    def __len__(self):
        return len(self._records)

    def record_iternext(self, next=None): # pylint:disable=redefined-builtin
        if next is None:
            if not self._records:
                raise StopIteration
            self.iteration_open = True
            oid, tid, state = self._records[0]
            return oid, tid, state, 1
        index = next
        if index >= len(self._records):
            self.iteration_open = False
            raise StopIteration
        oid, tid, state = self._records[index]
        return oid, tid, state, index + 1

    def openCommittedBlobFile(self, oid, serial, blob=None): # pylint:disable=unused-argument
        # This is the load-connection based entry point. It must not be
        # used while the record iteration cursor is active.
        assert not self.iteration_open, (
            "openCommittedBlobFile used the load connection "
            "during an active record iteration"
        )
        self.openCommittedBlobFile_calls.append((oid, serial))
        return open(self.blob_path, 'rb') # pylint:disable=consider-using-with


class _FakeDestTPC(object):
    keep_history = False

    def __init__(self):
        self.begun = []
        self.finished = []

    def tpc_begin(self, txn, tid, status=''):
        self.begun.append((txn, tid, status))

    def tpc_vote(self, txn):
        pass

    def tpc_finish(self, txn):
        self.finished.append(txn)


class _FakeDestRestore(object):
    def __init__(self):
        self.restored = []
        self.restored_blobs = []

    def _crs_transform_record_data(self, data):
        return data

    def restore(self, oid, serial, data, prev_txn, version, txn): # pylint:disable=unused-argument
        self.restored.append((oid, serial, data))

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, txn): # pylint:disable=unused-argument
        with open(blobfilename, 'rb') as f:
            blob_bytes = f.read()
        self.restored_blobs.append((oid, serial, data, blob_bytes))


class _FakeDestBlobHelper(object):
    def __init__(self, temp_dir):
        self.temp_dir = temp_dir

    def temporaryDirectory(self):
        return self.temp_dir


class TestHistoryFreeCopierBlobDownload(TestCase):

    def test_blob_download_avoids_load_connection_during_iteration(self):
        temp_dir = tempfile.mkdtemp(prefix='rs-copy-test')
        self.addCleanup(shutil.rmtree, temp_dir, True)

        blob_path = os.path.join(temp_dir, 'source.blob')
        with open(blob_path, 'wb') as f:
            f.write(b'blob-bytes')

        oid = p64(1)
        tid = p64(0x1234)
        data = pickle.dumps(Blob, 2)

        source = MockRecordIternextSource([(oid, tid, data)])
        source.blob_path = blob_path

        tpc = _FakeDestTPC()
        restore = _FakeDestRestore()
        blobhelper = _FakeDestBlobHelper(temp_dir)

        Copy(blobhelper, tpc, restore).copyTransactionsFrom(source)

        # The blob was restored with its full content...
        self.assertEqual(len(restore.restored_blobs), 1)
        restored_oid, restored_tid, restored_data, restored_bytes = restore.restored_blobs[0]
        self.assertEqual((restored_oid, restored_tid, restored_data), (oid, tid, data))
        self.assertEqual(restored_bytes, b'blob-bytes')
        # ...and the source's load-connection entry point was never used
        # while the record iteration was active.
        self.assertEqual(source.openCommittedBlobFile_calls, [])
        self.assertEqual(source.blobhelper.calls, [(oid, tid)])
        self.assertEqual(len(tpc.finished), 1)
