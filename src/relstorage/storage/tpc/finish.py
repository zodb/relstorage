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
The finishing states.
"""
from __future__ import absolute_import
from __future__ import print_function

from . import NotInTransaction

def Finish(vote_state):
    """
    The state we enter with tpc_finish.

    This is transient; once we successfully enter this state, we immediately return
    to the not-in-transaction state.
    """
    # It is assumed that self._lock.acquire was called before this
    # method was called.
    vote_state.storage._rollback_load_connection()
    txn = vote_state.prepared_txn
    assert txn is not None
    vote_state.storage._adapter.txncontrol.commit_phase2(
        vote_state.storage._store_conn,
        vote_state.storage._store_cursor,
        txn)
    vote_state.committing_tid_lock.release_commit_lock(vote_state.storage._store_cursor)
    vote_state.storage._cache.after_tpc_finish(vote_state.committing_tid_lock.tid)

    return NotInTransaction(vote_state.storage)
