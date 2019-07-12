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
Implementation of `copyTransactionsFrom`.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import tempfile
import time

from ZODB.blob import is_blob_record
from ZODB.utils import u64 as bytes8_to_int64
from ZODB.utils import cp as copy_blob
from ZODB.POSException import POSKeyError


logger = __import__('logging').getLogger(__name__)

class Copy(object):

    __slots__ = (
        'blobhelper',
        'tpc',
        'restore',
    )

    def __init__(self, blobhelper, tpc, restore):
        self.blobhelper = blobhelper
        self.tpc = tpc
        self.restore = restore

    def copyTransactionsFrom(self, other):
        # pylint:disable=too-many-locals
        # adapted from ZODB.blob.BlobStorageMixin
        begin_time = time.time()
        txnum = 0
        total_size = 0
        blobhelper = self.blobhelper
        tpc = self.tpc
        restore = self.restore

        logger.info("Counting the transactions to copy.")
        num_txns = 0
        for _ in other.iterator():
            num_txns += 1
        logger.info("Copying %d transactions", num_txns)

        for trans in other.iterator():
            txnum += 1
            num_txn_records = 0

            tpc.tpc_begin(trans, trans.tid, trans.status)
            for record in trans:
                blobfile = None
                if is_blob_record(record.data):
                    try:
                        blobfile = other.openCommittedBlobFile(
                            record.oid, record.tid)
                    except POSKeyError:
                        pass
                if blobfile is not None:
                    fd, name = tempfile.mkstemp(
                        suffix='.tmp',
                        dir=blobhelper.temporaryDirectory())
                    os.close(fd)
                    with open(name, 'wb') as target:
                        copy_blob(blobfile, target)
                    blobfile.close()
                    restore.restoreBlob(record.oid, record.tid, record.data,
                                        name, record.data_txn, trans)
                else:
                    restore.restore(record.oid, record.tid, record.data,
                                    '', record.data_txn, trans)
                num_txn_records += 1
                if record.data:
                    total_size += len(record.data)
            tpc.tpc_vote(trans)
            tpc.tpc_finish(trans)

            pct_complete = '%1.2f%%' % (txnum * 100.0 / num_txns)
            elapsed = time.time() - begin_time
            if elapsed:
                rate = total_size / 1e6 / elapsed
            else:
                rate = 0.0
            rate_str = '%1.3f' % rate
            logger.info(
                "Copied tid %d,%5d records | %6s MB/s (%6d/%6d,%7s)",
                bytes8_to_int64(trans.tid), num_txn_records, rate_str,
                txnum, num_txns, pct_complete)

        elapsed = time.time() - begin_time
        logger.info(
            "All %d transactions copied successfully in %4.1f minutes.",
            txnum, elapsed / 60.0)
