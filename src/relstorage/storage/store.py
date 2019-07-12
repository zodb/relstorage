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

from perfmetrics import Metric
from perfmetrics import metricmethod

logger = __import__('logging').getLogger(__name__)

class StoreMethodsMixin(object):

    _tpc_phase = None

    @Metric(method=True, rate=0.1)
    def store(self, oid, previous_tid, data, version, transaction):
        # Called by Connection.commit(), after tpc_begin has been called.
        assert not version, "Versions aren't supported"

        # If we get here and we're read-only, our phase will report that.
        self._tpc_phase.store(oid, previous_tid, data, transaction)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # Like store(), but used for importing transactions.  See the
        # comments in FileStorage.restore().  The prev_txn optimization
        # is not used.
        # pylint:disable=unused-argument
        assert not version, "Versions aren't supported"
        # If we get here and we're read-only, our phase will report that.

        self._tpc_phase.restore(oid, serial, data, prev_txn, transaction)

    def deleteObject(self, oid, oldserial, transaction):
        # This method is only expected to be called from zc.zodbdgc
        # currently.
        return self._tpc_phase.deleteObject(oid, oldserial, transaction)


class BlobStoreMethodsMixin(StoreMethodsMixin):

    blobhelper = None
    _store_cursor = None

    @metricmethod
    def storeBlob(self, oid, serial, data, blobfilename, version, txn):
        """Stores data that has a BLOB attached.

        The blobfilename argument names a file containing blob data.
        The storage will take ownership of the file and will rename it
        (or copy and remove it) immediately, or at transaction-commit
        time.  The file must not be open.

        Returns nothing.
        """
        assert not version
        # We used to flush the batcher here, for some reason.
        cursor = self._store_cursor
        self.blobhelper.storeBlob(cursor, self.store,
                                  oid, serial, data, blobfilename, version, txn)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, txn):
        """
        Write blob data already committed in a separate database

        See the restore and storeBlob methods.
        """
        self._tpc_phase.restoreBlob(oid, serial, data, blobfilename, prev_txn, txn)
