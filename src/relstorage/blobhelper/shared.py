##############################################################################
#
# Copyright (c) 2009,2019 Zope Foundation and Contributors.
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

import os
import ZODB.blob

from ZODB.utils import p64

from zope.interface import implementer

from relstorage.interfaces import POSKeyError
from .interfaces import IAuthoritativeBlobHelper
from .abstract import AbstractBlobHelper


logger = __import__('logging').getLogger(__name__)


@implementer(IAuthoritativeBlobHelper)
class SharedBlobHelper(AbstractBlobHelper):

    NEEDS_DB_LOCK_TO_VOTE = True
    NEEDS_DB_LOCK_TO_FINISH = False

    def __init__(self, options, adapter, fshelper=None):
        assert options.shared_blob_dir

        if fshelper is None:
            # Share files over NFS or similar
            fshelper = ZODB.blob.FilesystemHelper(options.blob_dir)
            fshelper.create()
        super(SharedBlobHelper, self).__init__(options, adapter, fshelper)

    def _loadBlobInternal(self, cursor, oid, serial, blob_lock=None):
        blob_filename = self._cachedLoadBlobInternal(oid, serial)
        if not blob_filename:
            # All the blobs are in a shared directory. If the file
            # isn't here, it's not anywhere.
            raise POSKeyError(oid, serial=serial, blob_filename=blob_filename)
        return blob_filename

    def storeBlob(self, _cursor, store_func,
                  oid, serial, data, blobfilename, version, txn):
        assert not version
        self._doStoreBlob(store_func, oid, serial, data, blobfilename, txn)

    def restoreBlob(self, _cursor, oid, serial, blobfilename):
        self.fshelper.getPathForOID(oid, create=True)
        targetname = self.fshelper.getBlobFilename(oid, serial)
        ZODB.blob.rename_or_copy_blob(blobfilename, targetname)

    def copy_undone(self, copied, tid):
        """
        After an undo operation, copy the matching blobs forward.

        The copied parameter is a list of ``(oid_int, tid_int)``.
        """
        for oid_int, old_tid_int in copied:
            oid = p64(oid_int)
            old_tid = p64(old_tid_int)
            orig_fn = self.fshelper.getBlobFilename(oid, old_tid)
            if not os.path.exists(orig_fn):
                # not a blob
                continue

            new_fn = self.fshelper.getBlobFilename(oid, tid)
            with open(orig_fn, 'rb') as orig, open(new_fn, 'wb') as new:
                ZODB.utils.cp(orig, new)

            self._add_blob_to_transaction(oid, new_fn)

    @staticmethod
    def _has_files(dirname):
        """Return True if a directory has any visible files."""
        names = os.listdir(dirname)
        if not names:
            return False
        for name in names:
            if not name.startswith('.'):
                return True
        return False

    def after_pack(self, oid_int, tid_int):
        """
        Called after an object state has been removed by packing.

        Removes the corresponding blob file.
        """
        oid = p64(oid_int)
        tid = p64(tid_int)
        fn = self.fshelper.getBlobFilename(oid, tid)
        if self.adapter.keep_history:
            # remove only the revision just packed
            if os.path.exists(fn):
                ZODB.blob.remove_committed(fn)
                dirname = os.path.dirname(fn)
                if not self._has_files(dirname):
                    ZODB.blob.remove_committed_dir(dirname)
        else:
            # remove all revisions
            dirname = os.path.dirname(fn)
            if os.path.exists(dirname):
                for name in os.listdir(dirname):
                    ZODB.blob.remove_committed(os.path.join(dirname, name))
                ZODB.blob.remove_committed_dir(dirname)

    def vote(self, tid=None):
        self._move_blobs_into_place(tid)

    def _abort_filename(self, filename):
        dirname = os.path.dirname(filename)
        if not self._has_files(dirname):
            ZODB.blob.remove_committed_dir(dirname)
