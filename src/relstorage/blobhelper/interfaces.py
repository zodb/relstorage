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
Interfaces for top-level RelStorage components.

These interfaces aren't meant to be considered public, they exist to
serve as documentation and for validation of RelStorage internals.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import Interface
from zope.interface import Attribute

# pylint:disable=inherit-non-class, no-self-argument, no-method-argument
# pylint:disable=too-many-ancestors

class IBlobHelper(Interface):
    """
    Blob support for RelStorage.

    There is one `IBlobHelper` per storage instance. Each
    `IBlobHelper` instance has access to the associated adapter as
    well as shared instances of ``fshelper`` (a
    ``ZODB.blob.FilesystemHelper``) and ``cache_checker`` (a
    BlobCacheChecker).
    """

    NEEDS_DB_LOCK_TO_FINISH = Attribute("Boolean")
    NEEDS_DB_LOCK_TO_VOTE = Attribute("Boolean")

    def new_instance(adapter):
        """
        Create a new instance for use in a new MVCC storage.
        """

    ###
    # Reading
    ###

    def loadBlob(cursor, oid, serial):
        pass

    def openCommittedBlobFile(cursor, oid, serial, blob=None):
        pass

    ###
    # Writing.
    #
    # This is only valid to do after a call to :meth:`begin`.
    ###

    def temporaryDirectory():
        pass

    def storeBlob(cursor, store_func,
                  oid, serial, data, blobfilename, version, txn):
        """Storage API: store a blob object."""


    ###
    # Transactions
    ###

    txn_has_blobs = Attribute("Does the transaction this object is joined to include blobs?")

    def begin():
        """
        Start a new transaction.
        """

    def vote(tid=None):
        """
        Check the transaction can be committed.

        If the *tid* is None, meaning it hasn't been allocated yet,
        then, if this implementation requires a TID in order to vote,
        it may raise an `StorageTransactionError`. If that happens,
        lock the database, allocated a TId, and try again.

        As an implementation note, does nothing *unless* we have a
        shared blob dir.
        """

    def finish(tid):
        """
        Finalize the transaction.

        As an implementation note, does nothing *if* we have a shared
        blob dir.
        """

    def abort():
        """
        Abort the transaction.
        """

    ###
    # Undo
    ###

    def copy_undone(copied, tid):
        """
        After an undo operation, copy the matching blobs forward.

        The copied parameter is a list of ``(integer oid, integer tid)``.

        Does nothing if not a ``shared_blob_dir``.
        """

    def restoreBlob(cursor, oid, serial, blobfilename):
        pass

    ###
    # Misc
    ###
    def after_pack(oid_int, tid_int):
        """
        Called after an object state has been removed by packing.

        Removes the corresponding blob file.
        """

    def close():
        pass


class IAuthoritativeBlobHelper(IBlobHelper):
    """
    A blob helper that has the only copy of the blob data.
    """

class ICachedBlobHelper(IBlobHelper):
    """
    A blob helper that is only a cache; the real data is elsewhere.
    """


class INoBlobHelper(IBlobHelper):
    """
    An object that does nothing with blobs.

    Used to avoid conditional logic in the main code. Methods that
    impact the use of the storage (user tries to store a blob but
    that's not possible, etc) should raise an error. Methods that are
    part of the internal workings of the storage and would have no
    side-effects (because there cannot be blobs) should quietly do nothing.
    """
