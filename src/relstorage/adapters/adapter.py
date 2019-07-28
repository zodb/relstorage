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
Base class for ``IRelStorageAdapter``.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from persistent.timestamp import TimeStamp
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from .._util import timestamp_at_unixtime
from ..options import Options

from ._abstract_drivers import _select_driver

class AbstractAdapter(object):

    options = None # type: Options
    driver_options = None # type: IDBDriverOptions
    locker = None # type: ILocker
    txncontrol = None # type: ITransactionControl
    mover = None # type: IObjectMover

    def _select_driver(self, options=None):
        return _select_driver(
            options or self.options or Options(),
            self.driver_options
        )

    def lock_database_and_choose_next_tid(self, cursor,
                                          username,
                                          description,
                                          extension):
        self.locker.hold_commit_lock(cursor, ensure_current=True)

        # Choose a transaction ID.
        #
        # Base the transaction ID on the current time, but ensure that
        # the tid of this transaction is greater than any existing
        # tid.
        last_tid = self.txncontrol.get_tid(cursor)
        now = time.time()
        stamp = timestamp_at_unixtime(now)
        stamp = stamp.laterThan(TimeStamp(int64_to_8bytes(last_tid)))
        tid = stamp.raw()

        tid_int = bytes8_to_int64(tid)
        self.txncontrol.add_transaction(cursor, tid_int, username, description, extension)
        return tid_int

    def tpc_prepare_phase1(self,
                           store_connection,
                           blobhelper,
                           ude,
                           commit=True, # pylint:disable=unused-argument
                           committing_tid_int=None,
                           after_selecting_tid=lambda tid: None):
        # Here's where we take the global commit lock, and
        # allocate the next available transaction id, storing it
        # into history-preserving DBs. But if someone passed us
        # a TID (``restore``), then it must already be in the DB, and the lock must
        # already be held.
        #
        # If we've prepared the transaction, then the TID must be in the
        # db, the lock must be held, and we must have finished all of our
        # storage actions. This is only expected to be the case when we have
        # a shared blob dir.

        cursor = store_connection.cursor
        if committing_tid_int is None:
            committing_tid_int = self.lock_database_and_choose_next_tid(
                cursor,
                *ude
            )

        # Move the new states into the permanent table
        # TODO: Figure out how to do as much as possible of this before holding
        # the commit lock. For example, use a dummy TID that we later replace.
        # (This has FK issues in HP dbs).
        txn_has_blobs = blobhelper.txn_has_blobs

        self.mover.move_from_temp(cursor, committing_tid_int, txn_has_blobs)

        after_selecting_tid(committing_tid_int)

        self.mover.update_current(cursor, committing_tid_int)
        prepared_txn_id = self.txncontrol.commit_phase1(
            store_connection, committing_tid_int)

        if commit:
            self.txncontrol.commit_phase2(store_connection, prepared_txn_id)

        return committing_tid_int, prepared_txn_id
