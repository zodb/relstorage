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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import implementer

from relstorage._util import Lazy

from ..interfaces import ILocker
from ..locker import AbstractLocker


@implementer(ILocker)
class Sqlite3Locker(AbstractLocker):

    _lock_share_clause = ''
    _lock_share_clause_nowait = ''
    _lock_current_caules = ''

    @Lazy
    def _lock_current_objects_query(self):
        # We exclusively lock the whole database at this point. There's no way to
        # avoid it. We also switch out of autocommit mode and start a transaction,
        # meaning our store cursor is no longer updating...but since we have an exclusive
        # lock, that's fine.
        return 'BEGIN IMMEDIATE TRANSACTION'

    def _lock_readCurrent_oids_for_share(self, cursor, current_oids, shared_locks_block):
        """
        This is a no-op.

        Once this is called, the parent class will call the after_lock_share callback,
        and the parent adapter will use that to check the recorded TIDs against the
        desired TIDs. Our store connection is still in autocommit mode at this point,
        so it will read the current data.
        """

    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        return True

    def release_commit_lock(self, cursor):
        # no action needed, locks released with transaction.
        pass

    def release_pack_lock(self, cursor):
        pass

    def hold_pack_lock(self, cursor):
        pass
