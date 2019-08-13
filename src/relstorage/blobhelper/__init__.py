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
"""Blob management utilities needed by RelStorage.

Most of this code is lifted from ZODB/ZEO.
"""
from __future__ import absolute_import
from __future__ import print_function


from ZODB.POSException import Unsupported
from zope.interface import implementer

from .interfaces import INoBlobHelper


__all__ = [
    'BlobHelper',
]


@implementer(INoBlobHelper)
class NoBlobHelper(object):
    # pylint:disable=unused-argument

    __slots__ = ()

    NEEDS_DB_LOCK_TO_FINISH = False
    NEEDS_DB_LOCK_TO_VOTE = False

    shared_blob_helper = False
    txn_has_blobs = False
    shared_blob_dir = None

    def new_instance(self, adapter):
        return self

    vote = finish = lambda self, _tid=None: None
    begin = abort = clear_temp = close = lambda self: None

    def after_pack(self, oid_int, tid_int):
        """
        Because there cannot be blobs, this method has nothing to do.
        """

    def copy_undone(self, copied, tid):
        """
        Because there cannot be blobs, this method has nothing to do.
        """

    def loadBlob(self, cursor, oid, serial):
        raise Unsupported("No blob directory is configured.")

    def openCommittedBlobFile(self, cursor, oid, serial, blob=None):
        raise Unsupported("No blob directory is configured.")

    def temporaryDirectory(self):
        raise Unsupported("No blob directory is configured.")

    def storeBlob(self, cursor, store_func,
                  oid, serial, data, blobfilename, version, txn):
        raise Unsupported("No blob directory is configured.")

    def restoreBlob(self, cursor, oid, serial, blobfilename):
        raise Unsupported("No blob directory is configured.")

    @property
    def fshelper(self):
        raise AttributeError("NoBlobHelper has no 'fshelper'")

    def __repr__(self):
        return "<NoBlobHelper>"


def BlobHelper(options, adapter):
    if options is None or not options.blob_dir:
        return NoBlobHelper()

    # Prevent warnings from runpy that these were found in sys.modules
    # before executing .cached.
    from .cached import CacheBlobHelper
    from .shared import SharedBlobHelper

    if options.shared_blob_dir:
        return SharedBlobHelper(options, adapter)
    return CacheBlobHelper(options, adapter)
