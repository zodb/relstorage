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

from ZODB.Connection import TransactionMetaData
from ZODB.loglevels import TRACE
from ZODB.blob import is_blob_record
from ZODB.utils import cp as copy_blob
from ZODB.utils import readable_tid_repr
from ZODB.POSException import POSKeyError
from ZODB.interfaces import IStorageCurrentRecordIteration as IRecordIter

from relstorage._compat import perf_counter
from relstorage._util import byte_display

logger = logging.getLogger(__name__)

def close(it):
    try:
        c = it.close
    except AttributeError: # pragma: no cover
        pass
    else:
        c()

class Copy(object):

    __slots__ = (
        'blobhelper',
        'tpc',
        'restore',
    )

    def __init__(self, blobhelper, tpc, restore):
        self.blobhelper = blobhelper
        # In practice, both *tpc* and *restore* are the
        # RelStorage instance.
        self.tpc = tpc
        self.restore = restore

    def copyTransactionsFrom(self, other):
        # Just the interface, not the attribute, in case we have a
        # partial proxy.
        other_has_record_iternext = IRecordIter.providedBy(other)

        copier_factory = _HistoryFreeCopier
        if self.tpc.keep_history or not other_has_record_iternext:
            copier_factory = _HistoryPreservingCopier

        logger.info(
            "Copying transactions to %s "
            "from %s (supports IStorageCurrentRecordIteration? %s) "
            "using %s",
            self.tpc,
            other,
            other_has_record_iternext,
            copier_factory,
        )
        copier = copier_factory(other, self.blobhelper, self.tpc, self.restore)

        try:
            logger.info("Counting the %s to copy.", copier.units)
            num_txns = len(copier)
            logger.info("Copying %d %s%s", num_txns, copier.units, copier.initial_log_suffix)

            progress = copier.ProgressLogger(num_txns, copier)
            copier.copy(progress)
        finally:
            copier.close()

        now = perf_counter()
        logger.info(
            "Copied transactions: %s",
            progress.display_at(now))


class _AbstractCopier(object):

    units = 'transactions'
    initial_log_suffix = ''
    unit_abbrev_display = 'TX'

    __slots__ = (
        'storage',
        'restore',
        'blobhelper',
        'tpc',
        'temp_blobs_to_rm',
        'total_count',
    )

    def __init__(self, storage, blobhelper, tpc, restore):
        self.storage = storage
        self.restore = restore
        self.blobhelper = blobhelper
        self.tpc = tpc
        self.temp_blobs_to_rm = []
        self.total_count = None

    def before_major_log(self):
        raise NotImplementedError

    def __len__(self):
        if self.total_count is None:
            self.total_count = self._compute_total_count()
        return self.total_count

    def _compute_total_count(self):
        raise NotImplementedError

    def copy(self, progress):
        # type: (_ProgressLogger) -> None
        raise NotImplementedError

    @property
    def ProgressLogger(self):
        return _ProgressLogger

    def clean_temp_blobs(self):
        num_blobs = len(self.temp_blobs_to_rm)
        if num_blobs:
            for tmp_blob in self.temp_blobs_to_rm:
                logger.log(TRACE, "Removing temporary blob file %s", tmp_blob)
                try:
                    os.unlink(tmp_blob)
                except OSError:
                    pass
            del self.temp_blobs_to_rm[:]
        return num_blobs

    def close(self):
        self.clean_temp_blobs()
        self.storage = None

    def restore_one(self, active_txn_meta,
                    oid, tid, data):

        # The signature for both ``restore`` and ``restoreBlob``
        # is:
        #
        #   (oid, serial, data, (blobfilename|prev_txn), version, txn)
        #
        # Where ``txn`` is the TransactionMetaData object
        # originally passed to ``tpc_begin``. It is only used to
        # check that the same object has been passed.
        #
        # ``prev_txn`` is not used but would come from ``record.data_txn``

        txn_data_size = len(data) if data else 0

        blobfile = None
        if is_blob_record(data):
            try:
                blobfile = self.storage.openCommittedBlobFile(
                    oid, tid)
            except POSKeyError: # pragma: no cover
                logger.exception("Failed to open blob to copy")

        # We may not be able to read the data after this.
        data = self.restore._crs_transform_record_data(data)
        if blobfile is not None:
            fd, name = tempfile.mkstemp(
                suffix='.tmp',
                dir=self.blobhelper.temporaryDirectory()
            )
            self.temp_blobs_to_rm.append(name)
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
            self.restore.restoreBlob(oid, tid, data,
                                     name, None, active_txn_meta)
        else:
            self.restore.restore(oid, tid, data,
                                 '', None, active_txn_meta)

        return txn_data_size, blobfile is not None


class _HistoryPreservingCopier(_AbstractCopier):

    __slots__ = (
        'storage_it',
    )

    def __init__(self, storage, blobhelper, tpc, restore):
        super(_HistoryPreservingCopier, self).__init__(storage, blobhelper, tpc, restore)
        self.storage_it = storage.iterator()

    def _compute_total_count(self):
        try:
            num_txns = len(self.storage_it)
            if num_txns == 0:
                # Hmm, that shouldn't really be right, should it?
                # Try the other path.
                raise TypeError()
        except TypeError:
            logger.warning("Iterator %s doesn't support len(); counting manually", self.storage_it)
            num_txns = 0
            for _ in self.storage_it:
                num_txns += 1
            close(self.storage_it)
            self.storage_it = self.storage.iterator()

        return num_txns

    def before_major_log(self):
        pass

    def close(self):
        close(self.storage_it)
        self.storage_it = None
        super(_HistoryPreservingCopier, self).close()

    def copy(self, progress):
        # type: (_ProgressLogger) -> None
        for trans in self.storage_it:
            begin = perf_counter()
            num_txn_records, txn_data_size, num_blobs = self(trans)
            now = perf_counter()
            progress.copied_one(now, now - begin, trans, num_txn_records, txn_data_size, num_blobs)

    def __call__(self, trans):
        # Originally adapted from ZODB.blob.BlobStorageMixin
        tpc = self.tpc
        num_txn_records = 0
        txn_data_size = 0

        tpc.tpc_begin(trans, trans.tid, trans.status)
        for record in trans:
            num_txn_records += 1
            record_size, _was_blob = self.restore_one(
                trans,
                record.oid,
                record.tid,
                record.data
            )
            txn_data_size += record_size
        tpc.tpc_vote(trans)
        tpc.tpc_finish(trans)

        num_blobs = self.clean_temp_blobs()

        return num_txn_records, txn_data_size, num_blobs


class _RecordIternextIterator(object):

    __slots__ = (
        'storage',
        'cookie',
    )

    def __init__(self, storage):
        self.storage = storage
        self.cookie = self

    def __iter__(self): # pragma: no cover
        return self

    def __next__(self):
        if self.cookie is None:
            raise StopIteration

        if self.cookie is self:
            # First time in.
            self.cookie = None
        try:
            result = self.storage.record_iternext(self.cookie)
        except ValueError:  # pragma: no cover
            # FileStorage can raise this if the underlying storage
            # is completely empty.
            # See https://github.com/zopefoundation/ZODB/issues/330
            if self.cookie is None:
                raise StopIteration
            raise # pragma: no cover

        oid, tid, state, self.cookie = result
        return oid, tid, state

    next = __next__ # Py2


class _HistoryFreeTransactionMetaData(TransactionMetaData):
    num_records = 0
    num_records_since_last_log = 0
    num_blobs_since_last_log = 0
    record_size_since_last_log = 0
    first_oid = None
    last_oid = None

    def reset_interval(self):
        self.num_records_since_last_log = 0
        self.num_blobs_since_last_log = 0
        self.record_size_since_last_log = 0


class _HistoryFreeCopier(_AbstractCopier):
    # Issues with iternext:
    # - No way to specify a starting transaction (FileStorage and RelStorage
    #   iterate by OID and can specify a starting OID).
    #   So this silently breaks the ``incremental`` mode of zodbconvert;
    #   Things still work, we just copy all objects from the beginning, and
    #   if we copied some, stopped, then start again, and in the meantime the source
    #   storage was packed, we could wind up with extra objects. That's the case
    #   anyway, though. The documentation has been set up to warn
    #   about that. TODO: Figure out an incremental way.
    # - Doesn't support len(); Rather than iterating manually to find it,
    #   and thus downloading all the object states, or copying them into a local FileStorage,
    #   (which would use lots of temp space) we ask for the estimated size from the
    #   storage.
    #
    # Issues copying from history-preserving to history-free
    # using the regular iterator():
    #
    # - We could copy and discard the state for a single object
    #   many times. Doing way too much work.

    units = 'objects'
    unit_abbrev_display = 'obj'
    initial_log_suffix = '. CAUTION: This number is approximate.'

    __slots__ = (
        'trans_meta',
    )

    def __init__(self, *args, **kwargs):
        _AbstractCopier.__init__(self, *args, **kwargs)
        self.trans_meta = None

    def _compute_total_count(self):
        return len(self.storage)

    def __iter__(self):
        return _RecordIternextIterator(self.storage)

    def copy(self, progress):
        # type: (_HistoryFreeProgressLogger) -> None
        for oid, tid, state in self:
            begin = perf_counter()
            if self.trans_meta is None:
                self.trans_meta = _HistoryFreeTransactionMetaData()
                self.trans_meta.first_oid = oid
                self.tpc.tpc_begin(self.trans_meta, tid)

            self.trans_meta.last_oid = oid
            record_size, was_blob = self.restore_one(
                self.trans_meta,
                oid,
                tid,
                state
            )
            self.trans_meta.num_records += 1
            self.trans_meta.num_records_since_last_log += 1
            self.trans_meta.num_blobs_since_last_log += was_blob
            self.trans_meta.record_size_since_last_log += record_size

            now = perf_counter()

            progress.copied_one(now, now - begin, self.trans_meta, 1, record_size,
                                was_blob)

        # Perform the final commit if needed.
        self.before_major_log()

    @property
    def ProgressLogger(self):
        return _HistoryFreeProgressLogger

    def before_major_log(self):
        # Integrate committing with the major log intervals. This is because the
        # major log interval is time-based and tunable. And, up to a point, committing
        # a larger batch provides throughput improvements.
        if self.trans_meta is not None:
            logger.debug('Committing object batch count=%d', self.trans_meta.num_records)
            trans_meta = self.trans_meta
            self.trans_meta = None
            self.tpc.tpc_vote(trans_meta)
            self.tpc.tpc_finish(trans_meta)
            self.clean_temp_blobs()


class _ProgressLogger(object):

    # Time in seconds between major progress logging.
    # (minor progress logging occurs every ``minor_log_count`` commits)
    log_interval = 60

    # Number of units to copy before checking if we should perform a major
    # log.
    log_count = 100

    # Number of units to copy before checking if we should perform a minor log.
    minor_log_count = 25

    minor_log_interval = 15

    minor_log_tx_record_count = 100
    minor_log_tx_size = 500 * 1024
    minor_log_copy_time_threshold = 1.0

    debug_enabled = False

    class _IntervalStats(object):
        __slots__ = (
            'begin_time',
            'units_copied',
            'total_size',
        )

        def __init__(self, begin_time):
            self.begin_time = begin_time
            self.units_copied = 0
            self.total_size = 0

        def display_at(self, now, total_num_units, unit_abbrev_display,
                       include_elapsed=False):
            pct_complete = '%1.2f%%' % ((
                (self.units_copied * 100.0 if total_num_units else 1)
                /
                (total_num_units or 1)
            ))

            elapsed_total = now - self.begin_time
            rate_mb = rate_units = 0.0
            if elapsed_total:
                rate_mb = self.total_size / elapsed_total
                rate_units = self.units_copied / elapsed_total

            rate_mb_str = byte_display(rate_mb)
            rate_unit_str = '%1.2f' % rate_units

            result = "%d/%d,%7s, %6s/s %6s %s/s, %s" % (
                self.units_copied, total_num_units, pct_complete,
                rate_mb_str, rate_unit_str, unit_abbrev_display,
                byte_display(self.total_size),
            )
            if include_elapsed:
                result += ' %4.1f minutes' % (elapsed_total / 60.0)
            return result

    def __init__(self, num_txns, copier):
        self.num_txns = num_txns # type: int
        self.copier = copier # type: _AbstractCopier
        begin_time = perf_counter()
        self._entire_stats = self._IntervalStats(begin_time)
        self._interval_stats = self._IntervalStats(begin_time)

        self.log_at = begin_time + self.log_interval
        self.minor_log_at = begin_time + self.minor_log_interval
        self.debug_enabled = logger.isEnabledFor(logging.DEBUG) or type(self).debug_enabled

    def display_at(self, now):
        return self._entire_stats.display_at(now, self.num_txns, True)

    def copied_one(self, now, copy_duration, trans,
                   num_txn_records, txn_byte_size, num_txn_blobs):
        # type: (float, float, Any, int, int, int)
        entire_stats = self._entire_stats
        interval_stats = self._interval_stats

        entire_stats.units_copied += 1
        interval_stats.units_copied += 1
        total_units_copied = self._entire_stats.units_copied

        entire_stats.total_size += txn_byte_size
        interval_stats.total_size += txn_byte_size

        if self.debug_enabled and self._should_minor_log(
                now,
                total_units_copied,
                txn_byte_size,
                num_txn_records,
                copy_duration
        ):
            self.minor_log_at = now + self.minor_log_interval
            self.do_minor_log(trans, num_txn_records, txn_byte_size, num_txn_blobs, copy_duration)


        if self._should_major_log(now, total_units_copied):
            self.copier.before_major_log()
            now = perf_counter()
            self.log_at = now + self.log_interval
            self.__major_log(
                now,
                self.transaction_display(trans, num_txn_records, txn_byte_size, num_txn_blobs))
            self._interval_stats = self._IntervalStats(now)

    def _should_major_log(self, now, total_units_copied):
        return total_units_copied % self.log_count and now >= self.log_at

    def _should_minor_log(self, now, total_units_copied, txn_byte_size, num_txn_records,
                          copy_duration): # pragma: no cover
        # This is mocked out by test_zodbconvert so we always try to log.
        if (total_units_copied % self.minor_log_count == 0 and now >= self.minor_log_at):
            return True
        if txn_byte_size >= self.minor_log_tx_size:
            return True
        if num_txn_records >= self.minor_log_tx_record_count:
            return True
        if copy_duration >= self.minor_log_copy_time_threshold:
            return True
        return False

    def do_minor_log(self, trans, num_txn_records, txn_byte_size, num_txn_blobs, copy_duration):
        logger.debug(
            "Copied %s in %1.4fs",
            self.transaction_display(trans, num_txn_records,
                                       txn_byte_size, num_txn_blobs),
            copy_duration
        )

    def __major_log(self, now, transaction_display):
        logger.info(
            "Copied %s | %60s | (%s)",
            transaction_display,
            self._interval_stats.display_at(now, self.num_txns, self.copier.unit_abbrev_display),
            self._entire_stats.display_at(now, self.num_txns,
                                          self.copier.unit_abbrev_display,
                                          include_elapsed=True)
        )

    def transaction_display(self, trans, num_txn_records, txn_byte_size, num_txn_blobs):
        return 'transaction %s <%4d records, %3d blobs, %9s>' % (
            readable_tid_repr(trans.tid),
            num_txn_records, num_txn_blobs, byte_display(txn_byte_size)
        )


class _HistoryFreeProgressLogger(_ProgressLogger):
    minor_log_tx_size = 1024 * 1024 # Only log if a single object is larger than 1MB

    def do_minor_log(self, trans, _num_txn_records, txn_byte_size, _num_txn_blobs, _copy_duration):
        logger.debug("Copied %d objects of size %s (%d blobs) (Most recent oid=%s size=%s)",
                     trans.num_records_since_last_log,
                     byte_display(trans.record_size_since_last_log),
                     trans.num_blobs_since_last_log,
                     trans.last_oid,
                     byte_display(txn_byte_size))
        trans.reset_interval()

    def transaction_display(self, trans, _num_txn_records, _txn_byte_size, _num_txn_blobs):
        return "object range %s -> %s" % (
            trans.first_oid,
            trans.last_oid,
        )
