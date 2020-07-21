# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.tests import TestCase
from relstorage.adapters.tests.test_drivers import IDBDriverSupportsCriticalTestMixin
from ..import psycopg2



class GeventPsycopg2Driver(psycopg2.GeventPsycopg2Driver):
    GeventConnection = None


    def _create_connection(self, mod, *extra_slots):
        cls = type(self)
        if cls.GeventConnection is None:
            base = super(GeventPsycopg2Driver, self)._create_connection(mod, *extra_slots)

            class GeventConnection(base):
                def __init__(self, *args, **kwargs): # pylint:disable=super-init-not-called
                    self.connect_args = args
                    self.conect_kwargs = kwargs
            cls.GeventConnection = GeventConnection
        return cls.GeventConnection

    def _check_wait_callback(self):
        return True

class TestGeventPsycopg2(IDBDriverSupportsCriticalTestMixin,
                         TestCase):

    def _makeOne(self):
        try:
            return GeventPsycopg2Driver()
        except ImportError as e:
            self.skipTest(e)

    def test_sync_ends_critical_phase(self):
        driver = self._makeOne()
        conn = driver.connect()
        driver.enter_critical_phase_until_transaction_end(conn, None)
        self.assertTrue(conn.is_in_critical_phase())
        driver.sync_status_after_hidden_commit(conn)
        self.assertFalse(driver.is_in_critical_phase(conn, None))

    def test_execute_multiple_statement_with_hidden_commit(self):
        driver = self._makeOne()
        conn = driver.connect()
        driver.enter_critical_phase_until_transaction_end(conn, None)
        class MockCursor(object):
            def execute(self, *_args):
                pass

        self.assertTrue(conn.is_in_critical_phase())
        driver.execute_multiple_statement_with_hidden_commit(
            conn, MockCursor(), 'SELECT; COMMIT;', ()
        )
        self.assertFalse(driver.is_in_critical_phase(conn, None))
