##############################################################################
#
# Copyright (c) 2008,2019 Zope Foundation and Contributors.
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
# pylint:disable=too-many-public-methods,unused-argument
from __future__ import absolute_import
from __future__ import print_function

import os
import tempfile

from ZODB.blob import remove_committed_dir
from ZODB.POSException import StorageTransactionError

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.tests import TestCase

from ..interfaces import IBlobHelper

test_oid = b'\0' * 7 + b'\x01'
test_tid = b'\0' * 7 + b'\x02'


def write_file(fn, data):
    assert isinstance(data, str)
    with open(fn, 'w') as f:
        f.write(data)


def read_file(fn):
    with open(fn, 'r') as f:
        return f.read()


class BlobHelperTest(TestCase):

    shared_blob_dir = True
    move_method = 'vote'

    def setUp(self):
        self.uploaded = None
        self.blob_dir = tempfile.mkdtemp()

    def tearDown(self):
        remove_committed_dir(self.blob_dir)

    def _class(self):
        from relstorage.blobhelper import BlobHelper
        return BlobHelper

    def _make(self, *args, **kw):
        return self._class()(*args, **kw)

    def _make_default(self, cache_size=None,
                      download_action='write', keep_history=True):
        test = self

        class DummyOptions(object):
            blob_dir = self.blob_dir
            shared_blob_dir = self.shared_blob_dir
            blob_cache_size = cache_size

        class DummyMover(object):
            def download_blob(self, cursor, oid_int, tid_int, filename):
                if download_action == 'write':
                    write_file(filename, 'blob here')
                    return 9
                return 0

            def upload_blob(self, cursor, oid_int, tid_int, filename):
                test.uploaded = (oid_int, tid_int, filename)

        class DummyAdapter(object):
            mover = DummyMover()

            def __init__(self):
                self.keep_history = keep_history

        blobhelper = self._make(DummyOptions(), DummyAdapter())
        blobhelper.begin()
        return blobhelper

    def test_provides(self):
        assert_that(self._make_default(), validly_provides(IBlobHelper))

    def test_cannot_begin_twice(self):
        blobhelper = self._make_default()
        with self.assertRaises(StorageTransactionError):
            blobhelper.begin()

    def test_begin_after_abort(self):
        blobhelper = self._make_default()
        blobhelper.abort()
        blobhelper.begin()

    def test_begin_after_finish(self):
        blobhelper = self._make_default()
        blobhelper.finish(b'tid')
        blobhelper.begin()

    def test_loadBlob_shared_exists(self):
        blobhelper = self._make_default()
        fn = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        os.makedirs(os.path.dirname(fn))
        write_file(fn, 'blob here')
        res = blobhelper.loadBlob(None, test_oid, test_tid)
        self.assertEqual(fn, res)

    def test_storeBlob_replace(self):
        called = []
        dummy_txn = object()

        def store_func(oid, tid, data, txn):
            called.append((oid, tid, data, txn))

        fn1 = os.path.join(self.blob_dir, 'newblob')
        write_file(fn1, 'here a blob')
        fn2 = os.path.join(self.blob_dir, 'newblob2')
        write_file(fn2, 'there a blob')

        blobhelper = self._make_default()
        self.assertFalse(blobhelper.txn_has_blobs)
        blobhelper.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                             fn1, '', dummy_txn)
        blobhelper.storeBlob(None, store_func, test_oid, test_tid, 'blob pickle',
                             fn2, '', dummy_txn)
        self.assertFalse(os.path.exists(fn1))
        self.assertFalse(os.path.exists(fn2))
        self.assertTrue(blobhelper.txn_has_blobs)
        target_fn = blobhelper._txn_blobs[test_oid]
        self.assertEqual(read_file(target_fn), 'there a blob')

    def test_move_into_place(self):
        blobhelper = self._make_default()
        d = blobhelper.fshelper.getPathForOID(test_oid, create=True)
        fn1 = os.path.join(d, 'newblob')
        write_file(fn1, 'here a blob')
        blobhelper._txn_blobs = {test_oid: fn1}
        move_method = getattr(blobhelper, self.move_method)
        move_method(test_tid)
        fn2 = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertEqual(read_file(fn2), 'here a blob')

    def test_abort(self):
        blobhelper = self._make_default()
        d = blobhelper.fshelper.getPathForOID(test_oid, create=True)
        fn1 = os.path.join(d, 'newblob')
        write_file(fn1, 'here a blob')
        blobhelper._txn_blobs = {test_oid: fn1}
        blobhelper.abort()
        fn2 = blobhelper.fshelper.getBlobFilename(test_oid, test_tid)
        self.assertFalse(os.path.exists(fn1))
        self.assertFalse(os.path.exists(fn2))


class NoBlobHelperTest(TestCase):

    def _class(self):
        from relstorage.blobhelper import NoBlobHelper
        return NoBlobHelper

    def makeOne(self):
        return self._class()()

    def test_provides(self):
        assert_that(self.makeOne(), validly_provides(IBlobHelper))
