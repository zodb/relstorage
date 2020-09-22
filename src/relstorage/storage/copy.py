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
import logging
import tempfile

from ZODB.loglevels import TRACE
from ZODB.blob import is_blob_record
from ZODB.utils import cp as copy_blob
from ZODB.utils import readable_tid_repr
from ZODB.POSException import POSKeyError

from relstorage._compat import perf_counter
from relstorage._util import byte_display

logger = logging.getLogger(__name__)

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
        logger.info("Counting the transactions to copy.")
        other_it = other.iterator()
        logger.debug("Opened the other iterator: %s", other_it)
        num_txns, other_it = self.__get_num_txns_to_copy(other, other_it)
        logger.info("Copying %d transactions", num_txns)

        progress = _ProgressLogger(num_txns, other, self.__copy_transaction)

        try:
            for trans in other_it:
                progress(trans)
        finally:
            try:
                close = other_it.close
            except AttributeError:
                pass
            else:
                close()

        now = perf_counter()
        logger.info(
            "Copied transactions: %s",
            progress.display_at(now))

    def __copy_transaction(self, other, trans):
        # Originally adapted from ZODB.blob.BlobStorageMixin
        tpc = self.tpc
        num_txn_records = 0
        txn_data_size = 0
        num_blobs = 0
        tmp_blobs_to_rm = []

        tpc.tpc_begin(trans, trans.tid, trans.status)
        for record in trans:
            num_txn_records += 1
            if record.data:
                txn_data_size += len(record.data)

            blobfile = None
            if is_blob_record(record.data):
                try:
                    blobfile = other.openCommittedBlobFile(
                        record.oid, record.tid)
                except POSKeyError:
                    logger.exception("Failed to open blob to copy")
            if blobfile is not None:
                fd, name = tempfile.mkstemp(
                    suffix='.tmp',
                    dir=self.blobhelper.temporaryDirectory()
                )
                tmp_blobs_to_rm.append(name)
                logger.log(
                    TRACE,
                    "Copying %s to temporary blob file %s for upload",
                    blobfile, name)

                with os.fdopen(fd, 'wb') as target:
                    # If we don't get the length, ``copy_blob`` will.
                    old_pos = blobfile.tell()
                    blobfile.seek(0, 2)
                    length = blobfile.tell()
                    blobfile.seek(old_pos)

                    copy_blob(blobfile, target, length)
                    txn_data_size += length
                blobfile.close()
                self.restore.restoreBlob(record.oid, record.tid, record.data,
                                         name, record.data_txn, trans)
            else:
                self.restore.restore(record.oid, record.tid, record.data,
                                     '', record.data_txn, trans)

        tpc.tpc_vote(trans)
        tpc.tpc_finish(trans)

        num_blobs = len(tmp_blobs_to_rm)
        if num_blobs:
            for tmp_blob in tmp_blobs_to_rm:
                logger.log(TRACE, "Removing temporary blob file %s", tmp_blob)
                try:
                    os.unlink(tmp_blob)
                except OSError:
                    pass

        return num_txn_records, txn_data_size, num_blobs

    def __get_num_txns_to_copy(self, other, other_it):
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

        return num_txns, other_it


class _ProgressLogger(object):

    # Time in seconds between major progress logging.
    # (minor progress logging occurs every ``log_count`` commits)
    log_interval = 60

    # Number of transactions to copy before checking if we should perform a major
    # log.
    log_count = 100

    # Number of transactions to copy before performing a minor log.
    minor_log_count = 25

    minor_log_interval = 15

    minor_log_tx_record_count = 100
    minor_log_tx_size = 100 * 1024
    minor_log_copy_time_threshold = 1.0

    class _IntervalStats(object):
        __slots__ = (
            'begin_time',
            'txns_copied',
            'total_size',
        )

        def __init__(self, begin_time):
            self.begin_time = begin_time
            self.txns_copied = 0
            self.total_size = 0

        def display_at(self, now, total_num_txns, include_elapsed=False):
            pct_complete = '%1.2f%%' % (self.txns_copied * 100.0 / total_num_txns)
            elapsed_total = now - self.begin_time
            if elapsed_total:
                rate_mb = self.total_size / elapsed_total
                rate_tx = self.txns_copied / elapsed_total
            else:
                rate_mb = rate_tx = 0.0
            rate_mb_str = byte_display(rate_mb)
            rate_tx_str = '%1.3f' % rate_tx

            result = "%d/%d,%7s, %6s/s %6s TX/s, %s" % (
                self.txns_copied, total_num_txns, pct_complete,
                rate_mb_str, rate_tx_str,
                byte_display(self.total_size),
            )
            if include_elapsed:
                result += ' %4.1f minutes' % (elapsed_total / 60.0)
            return result

    def __init__(self, num_txns, other_storage, copy):
        self.num_txns = num_txns
        begin_time = perf_counter()
        self._entire_stats = self._IntervalStats(begin_time)
        self._interval_stats = self._IntervalStats(begin_time)

        self.log_at = begin_time + self.log_interval
        self.minor_log_at = begin_time + self.minor_log_interval
        self.debug_enabled = logger.isEnabledFor(logging.DEBUG)

        self._other_storage = other_storage
        self._copy = copy

    def display_at(self, now):
        return self._entire_stats.display_at(now, self.num_txns, True)

    def __call__(self, trans):
        begin_copy = perf_counter()
        result = self._copy(self._other_storage, trans)
        now = perf_counter()
        self._copied(now, now - begin_copy, trans, result)

    def _copied(self, now, copy_duration, trans, copy_result):
        entire_stats = self._entire_stats
        interval_stats = self._interval_stats

        entire_stats.txns_copied += 1
        interval_stats.txns_copied += 1
        total_txns_copied = self._entire_stats.txns_copied
        txn_byte_size = copy_result[1]

        entire_stats.total_size += txn_byte_size
        interval_stats.total_size += txn_byte_size

        if self.debug_enabled:
            num_txn_records, txn_byte_size, _num_txn_blobs = copy_result
            if (total_txns_copied % self.minor_log_count == 0 and now >= self.minor_log_at) \
               or txn_byte_size >= self.minor_log_tx_size \
               or num_txn_records >= self.minor_log_tx_record_count \
               or copy_duration >= self.minor_log_copy_time_threshold:
                self.minor_log_at = now + self.minor_log_interval
                logger.debug(
                    "Copied %s in %1.4fs",
                    self.__transaction_display(trans, copy_result),
                    copy_duration
                )

        if total_txns_copied % self.log_count and now >= self.log_at:
            self.log_at = now + self.log_interval
            self.__major_log(
                now,
                self.__transaction_display(trans, copy_result))
            self._interval_stats = self._IntervalStats(now)


    def __major_log(self, now, transaction_display):

        logger.info(
            "Copied %s | %60s | (%s)",
            transaction_display,
            self._interval_stats.display_at(now, self.num_txns),
            self._entire_stats.display_at(now, self.num_txns, True)
        )

    def __transaction_display(self, trans, copy_result):
        num_txn_records, txn_byte_size, num_txn_blobs = copy_result
        return 'transaction %s <%4d records, %3d blobs, %9s>' % (
            readable_tid_repr(trans.tid),
            num_txn_records, num_txn_blobs, byte_display(txn_byte_size)
        )
