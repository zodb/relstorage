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

def Finish(vote_state, committed_tid_int, needs_store_commit=True):
    """
    The state we enter with tpc_finish.

    This is transient; once we successfully enter this state, we immediately return
    to the not-in-transaction state.
    """
    # Bring the load connection to current status.
    vote_state.shared_state.load_connection.rollback_quietly()
    if needs_store_commit:
        # We may have already committed the store connection, so there's
        # no point doing so again. Also no point in rolling it back either.
        txn = vote_state.shared_state.prepared_txn
        assert txn is not None
        vote_state.shared_state.adapter.txncontrol.commit_phase2(
            vote_state.shared_state.store_connection,
            txn,
            vote_state.shared_state.load_connection)

    vote_state.committing_tid_lock.release_commit_lock(
        vote_state.shared_state.store_connection.cursor)
    vote_state.shared_state.cache.after_tpc_finish(vote_state.committing_tid_lock.tid,
                                                   vote_state.shared_state.temp_storage)

    # The vote caller is responsible for releasing the shared
    # resources in vote_state.shared_state.
    # XXX: Which may not make much sense?
    return vote_state.initial_state.with_committed_tid_int(
        committed_tid_int
    )
