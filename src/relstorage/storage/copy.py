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

    # Time in seconds between progress logging.
    log_interval = 60

    # Number of transactions to copy before checking if we should log.
    log_count = 10

    def copyTransactionsFrom(self, other):
        # pylint:disable=too-many-locals,too-many-statements,too-many-branches
        # adapted from ZODB.blob.BlobStorageMixin
        begin_time = time.time()
        log_at = begin_time + self.log_interval
        txnum = 0
        total_size = 0
        blobhelper = self.blobhelper
        tpc = self.tpc
        restore = self.restore

        logger.info("Counting the transactions to copy.")
        other_it = other.iterator()
        logger.debug("Opened the other iterator: %s", other_it)
        try:
            num_txns = len(other_it)
            if num_txns == 0:
                # Hmm, that shouldn't really be right, should it?
                # Try the other path.
                raise TypeError()
        except TypeError:
            logger.debug("Iterator %s doesn't support len()", other_it)
            num_txns = 0
            for _ in other_it:
                num_txns += 1
            other_it.close()
            other_it = other.iterator()
        logger.info("Copying %d transactions", num_txns)

        tmp_blobs_to_rm = []
        for trans in other_it:
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
                        dir=blobhelper.temporaryDirectory()
                    )
                    tmp_blobs_to_rm.append(name)
                    logger.debug("Copying %s to temporary blob file %s for upload",
                                 blobfile, name)

                    with os.fdopen(fd, 'wb') as target:
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

            for tmp_blob in tmp_blobs_to_rm:
                logger.debug("Removing temporary blob file %s", tmp_blob)
                try:
                    os.unlink(tmp_blob)
                except OSError:
                    pass
            del tmp_blobs_to_rm[:]

            if txnum % self.log_count == 0 and time.time() > log_at:
                now = time.time()
                log_at = now + self.log_interval

                pct_complete = '%1.2f%%' % (txnum * 100.0 / num_txns)
                elapsed = now - begin_time
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
