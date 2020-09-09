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

from ...interfaces import ITPCStateVoting
from .. import SharedTPCState

class MockStoreConnectionPool(object):

    def __init__(self):
        self.conn = object()

    def borrow(self):
        return self.conn

class MockStorage(object):

    def __init__(self):
        self._store_connection_pool = MockStoreConnectionPool()

class MockBeginState(object):

    def __init__(self, shared_state=None):
        self.shared_state = shared_state or SharedTPCState(None, MockStorage(), None)
        self.required_tids = {}
        self.ude = (b'user', b'description', b'extension')
        self.invalidated_oids = set()

class MockLockedBeginState(MockBeginState):

    def __init__(self, shared_state=None):
        from relstorage.tests import MockAdapter
        from ..vote import DatabaseLockedForTid
        MockBeginState.__init__(self, shared_state)
        self.committing_tid_lock = DatabaseLockedForTid(
            b'12345678',
            12345678,
            MockAdapter()
        )

class _InterfaceMixin(object):

    BeginState = MockBeginState

    def _getClass(self):
        raise NotImplementedError

    def _makeOne(self):
        return self._getClass()(self.BeginState())

    def test_provides_interface(self):
        assert_that(self._makeOne(), verifiably_provides(ITPCStateVoting))



class TestHistoryFree(_InterfaceMixin, unittest.TestCase):

    def _getClass(self):
        from ..vote import HistoryFree
        return HistoryFree

class TestHistoryPreserving(_InterfaceMixin, unittest.TestCase):

    BeginState = MockLockedBeginState

    def _getClass(self):
        from ..vote import HistoryPreserving
        return HistoryPreserving

class TestHistoryPreservingDeletOnly(_InterfaceMixin, unittest.TestCase):

    BeginState = MockLockedBeginState

    def _getClass(self):
        from ..vote import HistoryPreservingDeleteOnly
        return HistoryPreservingDeleteOnly
