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

    def exit_critical_phase(self):
        self.exited_critical_phase = True

    def enter_critical_phase_until_transaction_end(self):
        self.entered_critical_phase = True

class MockStoreConnectionPool(object):

    def __init__(self):
        self.conn = MockConnection()

    def borrow(self):
        return self.conn

class MockTxnControl(object):
    def commit_phase1(self, _store_conn, _tid_int):
        """Does nothing."""

class MockAdapter(_MockAdapter):
    DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = True

    txncontrol = MockTxnControl()

    def lock_database_and_move(self, *_args, **_kwargs):
        """Does nothing"""
        return 12345678, None


class MockStorage(object):

    def __init__(self):
        self._store_connection_pool = MockStoreConnectionPool()
        self._adapter = MockAdapter()
        self._load_connection = MockConnection()

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


class TestHistoryFree(_InterfaceMixin, unittest.TestCase):

    def _getClass(self):
        from ..vote import HistoryFree
        return HistoryFree

class TestHistoryPreserving(_InterfaceMixin, unittest.TestCase):

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
