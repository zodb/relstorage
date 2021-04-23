# -*- coding: utf-8 -*-
"""
Tests for the tpc module.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageTransactionError

from hamcrest import assert_that

from nti.testing.matchers import validly_provides

from ...interfaces import ITPCStateNotInTransaction


class TestNotInTransactionCommon(unittest.TestCase):
    def _makeOne(self, read_only=False, begin_factory=None):
        from .. import NotInTransaction
        return NotInTransaction(begin_factory, read_only)

    def test_verify_interface(self):
        assert_that(self._makeOne(), validly_provides(ITPCStateNotInTransaction))

    def test_is_false(self):
        self.assertFalse(self._makeOne())

    no_transaction_methods = (
        'store', 'restore', 'checkCurrentSerialInTransaction',
        'undo', 'deleteObject', 'restoreBlob',
        'tpc_finish', 'tpc_vote'
    )

    def test_no_transaction(self):
        meths = self.no_transaction_methods
        for meth in meths:
            with self.assertRaises(StorageTransactionError):
                getattr(self._makeOne(), meth)()

class TestNotInTransaction(TestNotInTransactionCommon):

    def test_abort_returns_self(self):
        inst = self._makeOne()
        self.assertIs(inst, inst.tpc_abort())

    def test_initial_state_is_self(self):
        inst = self._makeOne()
        self.assertIs(inst.initial_state, inst)

    def test_read_only_or_no_transaction(self):
        meths = (
            'store',
            'restore',
            'deleteObject',
            'undo',
            'restoreBlob',
        )

        read_only_inst = self._makeOne(read_only=True)
        for meth in meths:
            with self.assertRaises(ReadOnlyError):
                getattr(read_only_inst, meth)()

        writable_inst = self._makeOne(read_only=False)
        for meth in meths:
            with self.assertRaises(StorageTransactionError):
                getattr(writable_inst, meth)()

    def test_with_committed_tid_int(self):
        begin_factory = object()
        read_only = object()
        inst = self._makeOne(read_only, begin_factory)
        self.assertIs(inst.read_only, read_only)
        self.assertIs(inst.begin_factory, begin_factory)
        self.assertEqual(inst.last_committed_tid_int, 0)

        new_inst = inst.with_committed_tid_int(42)
        self.assertIs(new_inst.read_only, read_only)
        self.assertIs(new_inst.begin_factory, begin_factory)
        self.assertEqual(new_inst.last_committed_tid_int, 42)


class TestStale(TestNotInTransactionCommon):

    def _makeOne(self, read_only=False, begin_factory=None):
        from .. import Stale
        from .. import NotInTransaction
        return Stale(NotInTransaction(begin_factory, read_only), StorageTransactionError)

    def test_no_longer_stale(self):
        from .. import NotInTransaction
        inst = self._makeOne()
        self.assertIs(inst.stale(None), inst)
        self.assertIsInstance(inst.no_longer_stale(), NotInTransaction)

    # We piggyback test_no_transaction to mean the stale error
    no_transaction_methods = TestNotInTransactionCommon.no_transaction_methods + (
        'tpc_begin',
    )

class TestSharedTPCState(unittest.TestCase):

    def _makeOne(self, initial_state=None, storage=None, transaction=None):
        from .. import SharedTPCState
        return SharedTPCState(initial_state, storage, transaction)

    class Pool(object):
        replaced = False

        def __init__(self, conn):
            self.conn = conn

        def borrow(self):
            return self.conn

        def replace(self, obj):
            assert obj is self.conn
            self.replaced = True

    class Storage(object):
        def __init__(self, store_pool=None, load_conn=None, blobhelper=None):
            self._store_connection_pool = store_pool
            self._load_connection = load_conn
            self.blobhelper = blobhelper

    def setUp(self):
        from perfmetrics import set_statsd_client
        from perfmetrics import statsd_client
        from perfmetrics.testing import FakeStatsDClient
        self.stat_client = FakeStatsDClient()
        self.__orig_client = statsd_client()
        set_statsd_client(self.stat_client)

    def tearDown(self):
        from perfmetrics import set_statsd_client
        set_statsd_client(self.__orig_client)

    def test_store_connection_borrow_release(self):
        from .. import _CLOSED_CONNECTION
        conn = object()

        pool = self.Pool(conn)

        inst = self._makeOne(storage=self.Storage(pool))

        self.assertIs(inst.store_connection, conn)
        inst.release()
        self.assertTrue(pool.replaced)
        self.assertIs(inst.store_connection, _CLOSED_CONNECTION)

    def test_abort_load_connection_store_connection_load_fails(self):
        # Even if we access the load_connection first, the store_connection
        # is aborted first; aborting the store connection doesn't stop the load
        # connection from being aborted.
        from .. import _CLOSED_CONNECTION

        class LoadConn(object):
            aborted = False
            def drop(self):
                self.aborted = True

            exited = False
            def exit_critical_phase(self):
                self.exited = True

        store_conn = object()
        pool = self.Pool(store_conn)
        load_conn = LoadConn()

        storage = self.Storage(pool, load_conn)
        inst = self._makeOne(storage=storage)

        # Order matters
        self.assertIs(inst.load_connection, load_conn)
        self.assertIs(inst.store_connection, store_conn)

        with self.assertRaises(Exception):
            inst.abort(force=True)

        self.assertTrue(load_conn.aborted)
        self.assertTrue(load_conn.exited)
        self.assertTrue(pool.replaced)
        # This got None indicating an exception
        self.assertIsNone(inst.store_connection)
        self.assertIs(inst.load_connection, _CLOSED_CONNECTION)

    def test_blobhelper(self):
        class BlobHelper(object):
            began = False
            aborted = False
            cleared = False

            def begin(self):
                self.began = True
            def abort(self):
                self.aborted = True
            def clear_temp(self):
                self.cleared = True

            txn_has_blobs = True

        blobhelper = BlobHelper()
        storage = self.Storage(blobhelper=blobhelper)
        inst = self._makeOne(storage=storage)
        self.assertFalse(inst.has_blobs())
        self.assertNotIn('blobhelper', vars(inst))

        self.assertIs(inst.blobhelper, blobhelper)
        self.assertTrue(inst.has_blobs())
        self.assertTrue(blobhelper.began)
        inst.abort()
        self.assertIsNone(inst.blobhelper)
        self.assertFalse(inst.has_blobs())
        self.assertTrue(blobhelper.aborted)

        blobhelper = BlobHelper()
        storage = self.Storage(blobhelper=blobhelper)
        inst = self._makeOne(storage=storage)

        self.assertIs(inst.blobhelper, blobhelper)
        inst.release()
        self.assertTrue(blobhelper.cleared)
        self.assertIsNone(inst.blobhelper)

    def test_temp_storage(self):
        # pylint:disable=no-member
        inst = self._makeOne()
        self.assertFalse(inst.has_temp_data())
        self.assertNotIn('temp_storage', vars(inst))

        self.assertIs(inst.temp_storage, inst.temp_storage)
        self.assertIn('temp_storage', vars(inst))
        self.assertFalse(inst.has_temp_data())
        ts = inst.temp_storage
        inst.release()
        self.assertIsNone(ts._queue)
        self.assertIsNone(inst.temp_storage)
        self.assertFalse(inst.has_temp_data())

        inst = self._makeOne()
        ts = inst.temp_storage
        inst.abort()
        self.assertIsNone(ts._queue)
        self.assertIsNone(inst.temp_storage)
        self.assertFalse(inst.has_temp_data())

    def test_statsd_buf_abort(self):
        from perfmetrics.testing.matchers import is_timer
        from perfmetrics.testing.matchers import is_counter
        from hamcrest import contains_exactly

        inst = self._makeOne()
        self.assertEqual([], inst._statsd_buf)
        self.assertIn('_statsd_buf', inst.__dict__)
        inst.stat_count('count', 1)
        inst.stat_timing('timer', 1)
        inst.abort()
        self.assertIsNone(inst._statsd_buf)
        # Stats were sent
        assert_that(
            self.stat_client,
            contains_exactly(
                is_counter('count'),
                is_timer('timer')
            )
        )
        inst.release()
        self.assertIsNone(inst._statsd_buf)
        # but not sent again
        assert_that(
            self.stat_client,
            contains_exactly(
                is_counter('count'),
                is_timer('timer')
            )
        )

    def test_statsd_buf_release(self):
        from perfmetrics.testing.matchers import is_timer
        from perfmetrics.testing.matchers import is_counter
        from hamcrest import contains_exactly

        inst = self._makeOne()
        self.assertEqual([], inst._statsd_buf)
        self.assertIn('_statsd_buf', inst.__dict__)
        inst.stat_count('count', 1)
        inst.stat_timing('timer', 1)
        inst.release()
        self.assertIsNone(inst._statsd_buf)
        # Stats were sent
        assert_that(
            self.stat_client,
            contains_exactly(
                is_counter('count'),
                is_timer('timer')
            )
        )
        inst.abort()
        self.assertIsNone(inst._statsd_buf)
        # But not sent again
        assert_that(
            self.stat_client,
            contains_exactly(
                is_counter('count'),
                is_timer('timer')
            )
        )
