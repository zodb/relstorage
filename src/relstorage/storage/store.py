# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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
Implementation of store-related methods.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from .._compat import metricmethod
from .._compat import metricmethod_sampled
from .util import writable_storage_method
from .util import phase_dependent_aborts_early

logger = __import__('logging').getLogger(__name__)

class Storer(object):

    __slots__ = ()

    @phase_dependent_aborts_early
    @writable_storage_method
    @metricmethod_sampled
    def store(self, tpc_phase, oid, previous_tid, data, version, transaction):
        # Called by Connection.commit(), after tpc_begin has been called.
        assert not version, "Versions aren't supported"

        # If we get here and we're read-only, our phase will report that.
        tpc_phase.store(oid, previous_tid, data, transaction)

    @phase_dependent_aborts_early
    @writable_storage_method
    def restore(self, tpc_phase, oid, serial, data, version, prev_txn, transaction):
        # Like store(), but used for importing transactions.  See the
        # comments in FileStorage.restore().  The prev_txn optimization
        # is not used.
        # pylint:disable=unused-argument
        assert not version, "Versions aren't supported"
        # If we get here and we're read-only, our phase will report that.

        tpc_phase.restore(oid, serial, data, prev_txn, transaction)

    @phase_dependent_aborts_early
    @writable_storage_method
    def deleteObject(self, tpc_phase, oid, oldserial, transaction):
        # This method is only expected to be called from zc.zodbdgc
        # currently.
        return tpc_phase.deleteObject(oid, oldserial, transaction)


class BlobStorer(object):

    __slots__ = (
        'blobhelper',
        'store_connection',
    )

    def __init__(self, blobhelper, store_connection):
        self.blobhelper = blobhelper
        self.store_connection = store_connection

    @phase_dependent_aborts_early
    @writable_storage_method
    @metricmethod
    def storeBlob(self, tpc_phase, oid, serial, data, blobfilename, version, txn):
        """
        Stores data that has a BLOB attached.

        The blobfilename argument names a file containing blob data.
        The storage will take ownership of the file and will rename it
        (or copy and remove it) immediately, or at transaction-commit
        time.  The file must not be open.

        Returns nothing.
        """
        assert not version
        # We used to flush the batcher here, for some reason.
        store_func = tpc_phase.store
        cursor = self.store_connection.cursor
        self.blobhelper.storeBlob(cursor, store_func,
                                  oid, serial, data, blobfilename, version, txn)

    @phase_dependent_aborts_early
    @writable_storage_method
    def restoreBlob(self, tpc_phase, oid, serial, data, blobfilename, prev_txn, txn):
        """
        Write blob data already committed in a separate database

        See the restore and storeBlob methods.
        """
        tpc_phase.restoreBlob(oid, serial, data, blobfilename, prev_txn, txn)
