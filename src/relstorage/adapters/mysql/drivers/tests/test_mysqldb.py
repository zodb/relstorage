# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage.tests import TestCase
from relstorage.adapters.tests.test_drivers import IDBDriverSupportsCriticalTestMixin
from ..import mysqldb



class GeventMySQLdbDriver(mysqldb.GeventMySQLdbDriver):
    GeventConnection = None

    @classmethod
    def _get_connection_class(cls):
        if cls.GeventConnection is None:
            from .. import _mysqldb_gevent
            class GeventConnection(_mysqldb_gevent.AbstractConnection):
                class GeventCursor(object):
                    def __init__(self, conn):
                        self.connection = conn
                    execute = lambda self, _q, _a: None
                    fetchall = lambda self: []
                    nextset = lambda self: None

                def __init__(self, *args, **kwargs): # pylint:disable=super-init-not-called
                    self.connect_args = args
                    self.conect_kwargs = kwargs

                _direct_commit = _direct_rollback = lambda self: None

                def cursor(self):
                    return self.GeventCursor(self)
            cls.GeventConnection = GeventConnection
        return cls.GeventConnection

class TestGeventMySQLdb(IDBDriverSupportsCriticalTestMixin,
                        TestCase):

    def _makeOne(self):
        try:
            return GeventMySQLdbDriver()
        except ImportError as e:
            self.skipTest(e)

    def test_callproc_exit_critical_phase(self):
        driver = self._makeOne()
        conn = driver.connect()

        driver.enter_critical_phase_until_transaction_end(conn, None)

        driver.callproc_multi_result(
            conn.cursor(),
            'do_something()',
            (),
            exit_critical_phase=True
        )

        self.assertFalse(driver.is_in_critical_phase(conn, None))
