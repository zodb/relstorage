# -*- coding: utf-8 -*-
"""
Tests for vote.py.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import unittest

from hamcrest import assert_that
from nti.testing.matchers import verifiably_provides

from relstorage.tests import MockAdapter as _MockAdapter
from ...interfaces import ITPCStateVoting
from .. import SharedTPCState


class MockConnection(object):

    in_critical_phase = False
    exited_critical_phase = False
    entered_critical_phase = False
    cursor = None

    def exit_critical_phase(self):
        self.exited_critical_phase = True

    def enter_critical_phase_until_transaction_end(self):
        self.entered_critical_phase = True

    def rollback_quietly(self):
        """Does nothing"""

class MockStoreConnectionPool(object):

    def __init__(self):
        self.conn = MockConnection()

    def borrow(self):
        return self.conn

    def replace(self, conn):
        assert conn is self.conn

class MockTxnControl(object):
    def commit_phase1(self, _store_conn, _tid_int):
        """Does nothing."""

    def commit_phase2(self, _store_conn, _txn, _load_conn):
        """Does nothing"""

class MockAdapter(_MockAdapter):
    DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = True

    txncontrol = MockTxnControl()

    def lock_database_and_move(self, *_args, **_kwargs):
        """Does nothing"""
        return 12345678, None


class MockCache(object):
    def after_tpc_finish(self, *_args):
        """does nothing"""

class MockStorage(object):

    def __init__(self):
        self._store_connection_pool = MockStoreConnectionPool()
        self._adapter = MockAdapter()
        self._load_connection = MockConnection()
        self._cache = MockCache()

class MockBeginState(object):

    def __init__(self, shared_state=None):
        self.shared_state = shared_state or SharedTPCState(None, MockStorage(), None)
        self.required_tids = {}
        self.ude = (b'user', b'description', b'extension')
        self.invalidated_oids = set()

class MockLockedBeginState(MockBeginState):

    def __init__(self, shared_state=None):
        from ..vote import DatabaseLockedForTid
        MockBeginState.__init__(self, shared_state)
        self.committing_tid_lock = DatabaseLockedForTid(
            b'12345678',
            12345678,
            self.shared_state.adapter
        )

class _InterfaceMixin(object):

    BeginState = MockBeginState

    def setUp(self):
        from perfmetrics import set_statsd_client
        from perfmetrics import statsd_client
        from perfmetrics.testing import FakeStatsDClient
        from .. import vote
        self.stat_client = FakeStatsDClient()
        self.__orig_client = statsd_client()
        self.__orig_sample_rate = vote.METRIC_SAMPLE_RATE
        vote.METRIC_SAMPLE_RATE = 1
        set_statsd_client(self.stat_client)

    def tearDown(self):
        from perfmetrics import set_statsd_client
        from .. import vote
        set_statsd_client(self.__orig_client)
        vote.METRIC_SAMPLE_RATE = self.__orig_sample_rate

    def _getClass(self):
        raise NotImplementedError

    def _makeOne(self):
        return self._getClass()(self.BeginState())

    def test_provides_interface(self):
        assert_that(self._makeOne(), verifiably_provides(ITPCStateVoting))

    def _check_lock_and_move_commit(self, committed):
        self.assertTrue(committed)

    def test_exits_critical_section(self):
        vote = self._makeOne()
        vote.shared_state.adapter.DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = False
        assert not vote.shared_state.load_connection.exited_critical_phase
        assert not vote.shared_state.store_connection.exited_critical_phase
        committed = vote._lock_and_move()
        self._check_lock_and_move_commit(committed)
        if committed:
            self.assertTrue(vote.shared_state.load_connection.exited_critical_phase)
            self.assertTrue(vote.shared_state.store_connection.exited_critical_phase)
        else:
            self.assertFalse(vote.shared_state.load_connection.exited_critical_phase)
            self.assertFalse(vote.shared_state.store_connection.exited_critical_phase)

    def test_not_exits_critical_section_vote_only(self):
        vote = self._makeOne()
        vote.shared_state.adapter.DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = False
        assert not vote.shared_state.load_connection.exited_critical_phase
        assert not vote.shared_state.store_connection.exited_critical_phase
        committed = vote._lock_and_move(vote_only=True)
        self.assertFalse(committed)
        self.assertFalse(vote.shared_state.load_connection.exited_critical_phase)
        self.assertFalse(vote.shared_state.store_connection.exited_critical_phase)
        self.assertTrue(vote.shared_state.load_connection.entered_critical_phase)
        self.assertTrue(vote.shared_state.store_connection.entered_critical_phase)

    def test_not_exits_critical_section_interleavable(self):
        vote = self._makeOne()
        vote.shared_state.adapter.DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = True
        assert not vote.shared_state.load_connection.exited_critical_phase
        assert not vote.shared_state.store_connection.exited_critical_phase
        committed = vote._lock_and_move()
        self._check_lock_and_move_commit(committed)
        self.assertFalse(vote.shared_state.load_connection.exited_critical_phase)
        self.assertFalse(vote.shared_state.store_connection.exited_critical_phase)

class _TPCFinishMixin(object):

    def test_tpc_finish_stats(self):
        from perfmetrics.testing.matchers import is_timer
        from hamcrest import contains_exactly
        vote = self._makeOne()
        vote.shared_state.transaction = self
        class InitialState(object):
            def with_committed_tid_int(self, *args):
                """Does nothing"""
        vote.shared_state.initial_state = InitialState()
        vote.lock_and_vote_times[0] = 1
        vote.lock_and_vote_times[1] = 1

        finish_times = [2, 3]
        def time():
            return finish_times.pop()

        vote.tpc_finish(vote.shared_state._storage, vote.transaction, _time=time)

        assert_that(self.stat_client,
                    contains_exactly(
                        # 2 - 1 as ms = 1000
                        is_timer('relstorage.storage.tpc_vote.objects_locked.t', '1000'),
                        # 3 - 1 as ms = 2000
                        is_timer('relstorage.storage.tpc_vote.between_vote_and_finish.t', '2000')
                    ))


class TestHistoryFree(_TPCFinishMixin, _InterfaceMixin, unittest.TestCase):

    def _getClass(self):
        from ..vote import HistoryFree
        return HistoryFree

class TestHistoryPreserving(_TPCFinishMixin, _InterfaceMixin, unittest.TestCase):

    BeginState = MockLockedBeginState

    def _getClass(self):
        from ..vote import HistoryPreserving
        return HistoryPreserving

class TestHistoryPreservingDeleteOnly(_InterfaceMixin, unittest.TestCase):

    BeginState = MockLockedBeginState

    def _getClass(self):
        from ..vote import HistoryPreservingDeleteOnly
        return HistoryPreservingDeleteOnly

    def _check_lock_and_move_commit(self, committed):
        self.assertFalse(committed)

class TestFunctions(unittest.TestCase):

    def _makeSharedState(self, initial_state=None, storage=None, transaction=None):
        return SharedTPCState(initial_state, storage, transaction)

    def _callFUT(self, ex, state, required_tids):
        from ..vote import add_details_to_lock_error
        add_details_to_lock_error(ex, state, required_tids)
        return ex

    def _check(self, kind):
        ex = kind('MESSAGE')
        state = self._makeSharedState()
        state.temp_storage.store_temp(1, b'abc', 42) # pylint:disable=no-member

        self._callFUT(ex, state, {1: 1})
        s = str(ex)
        self.assertIn('readCurrent {oid: tid}', s)
        self.assertIn('{1: 1}', s)
        self.assertIn('Previous TID', s)
        self.assertIn('42', s)
        self.assertIn('MESSAGE', s)

    def test_add_details_to_UnableToAcquireCommitLockError(self):
        from relstorage.adapters.interfaces import UnableToAcquireCommitLockError
        self._check(UnableToAcquireCommitLockError)

    def test_add_details_to_UnableToLockRowsToModifyError(self):
        from relstorage.adapters.interfaces import UnableToLockRowsToModifyError
        self._check(UnableToLockRowsToModifyError)

    def test_add_details_to_UnableToLockRowsToReadCurrentError(self):
        from relstorage.adapters.interfaces import UnableToLockRowsToReadCurrentError
        self._check(UnableToLockRowsToReadCurrentError)
