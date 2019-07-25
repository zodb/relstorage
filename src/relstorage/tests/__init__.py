"""relstorage.tests package"""

import abc
import os
import unittest

import transaction

from ZODB.Connection import Connection
from ZODB.tests.util import clear_transaction_syncs

from relstorage._compat import ABC
from relstorage.options import Options
from relstorage.adapters.sql import DefaultDialect

try:
    from unittest import mock
except ImportError: # Python 2
    import mock

mock = mock

class TestCase(unittest.TestCase):
    """
    General tests that may use databases, connections and
    transactions, but don't have any specific requirements or
    framework to do so.

    This class supplies some supporting help for assertions and
    cleanups.
    """
    # Avoid deprecation warnings; 2.7 doesn't have
    # assertRaisesRegex or assertRegex
    assertRaisesRegex = getattr(
        unittest.TestCase,
        'assertRaisesRegex',
        None
    ) or getattr(unittest.TestCase, 'assertRaisesRegexp')
    assertRegex = getattr(
        unittest.TestCase,
        'assertRegex',
        None
    ) or getattr(unittest.TestCase, 'assertRegexpMatches')

    def setUp(self):
        super(TestCase, self).setUp()
        # This is done by ZODB.tests.util.TestCase, but
        # not stored anywhere.
        # XXX: As of ZODB 5.5.1, that class also doesn't handle
        # Python 3 correctly either. File a bug about this.
        name = self.__class__.__name__

        # Python 2
        mname = getattr(self, '_TestCase__testMethodName', '')
        if not mname:
            # Python 3
            mname = getattr(self, '_testMethodName', '')
        if mname:
            name += '-' + mname
        self.rs_temp_prefix = name
        self.addCleanup(clear_transaction_syncs)

    def __close_connection(self, connection):
        if connection.opened:
            try:
                connection.close()
            except KeyError:
                # From the transaction manager's list of syncs.
                # This can happen if clear_transaction_syncs()
                # has already been called.
                pass

            connection.close = lambda *_, **__: None

    def __close(self, thing):
        try:
            thing.close()
        except KeyError:
            # As for Connection; a DB closes all its connections.
            pass

    def _closing(self, o):
        """
        Close the object using its 'close' method *after* invoking
        all of the `tearDown` stack, and even running if `setUp`
        fails.

        This is just a convenience wrapper around `addCleanup`. This
        exists to make it easier to call.

        Failures (exceptions) in your cleanup function will still
        result in the test failing, but all registered cleanups will
        still be run.

        Returns the given object.
        """
        __traceback_info__ = o
        # Don't capture o.close now, it could get swizzled later to prevent it
        # from doing things. For example, DB.close() sets self.close to a no-op

        if isinstance(o, Connection):
            meth = self.__close_connection
        else:
            meth = self.__close
        self.addCleanup(meth, o)
        return o

    def tearDown(self):
        transaction.abort()
        super(TestCase, self).tearDown()

    def assertIsEmpty(self, container):
        self.assertLength(container, 0)

    assertEmpty = assertIsEmpty

    def assertLength(self, container, length):
        self.assertEqual(len(container), length, container)

class StorageCreatingMixin(ABC):

    keep_history = None # Override
    driver_name = None # Override.

    @abc.abstractmethod
    def make_adapter(self, options):
        # abstract method
        raise NotImplementedError

    @abc.abstractmethod
    def get_adapter_class(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_adapter_zconfig(self):
        """
        Return the part of the ZConfig string that makes the adapter.

        That is, return the <postgresql>, <mysql> or <oracle> section.

        Return text (unicode).
        """
        raise NotImplementedError

    def get_adapter_zconfig_replica_conf(self):
        return os.path.join(os.path.dirname(__file__), 'replicas.conf')

    @abc.abstractmethod
    def verify_adapter_from_zconfig(self, adapter):
        """
        Assert that the adapter configured from get_adapter_zconfig
        is properly configured.
        """
        raise NotImplementedError

    def _wrap_storage(self, storage):
        return storage

    def make_storage(self, zap=True, **kw):
        from . import util
        from relstorage.storage import RelStorage

        if ('cache_servers' not in kw
                and 'cache_module_name' not in kw
                and kw.get('share_local_cache', True)):
            if util.CACHE_SERVERS and util.CACHE_MODULE_NAME:
                kw['cache_servers'] = util.CACHE_SERVERS
                kw['cache_module_name'] = util.CACHE_MODULE_NAME
        if 'cache_prefix' not in kw:
            kw['cache_prefix'] = type(self).__name__ + self._testMethodName
        if 'cache_local_dir' not in kw:
            # Always use a persistent cache. This helps discover errors in
            # the persistent cache.
            # These tests run in a temporary directory that gets cleaned up, so the CWD is
            # appropriate. BUT: it should be an abspath just in case we change directories
            kw['cache_local_dir'] = os.path.abspath('.')
        if 'commit_lock_timeout' not in kw:
            # Cut this way down so we get better feedback.
            kw['commit_lock_timeout'] = 10

        assert self.driver_name
        options = Options(keep_history=self.keep_history, driver=self.driver_name, **kw)
        adapter = self.make_adapter(options)
        storage = RelStorage(adapter, options=options)
        if zap:
            # XXX: Some ZODB tests, possibly check4ExtStorageThread
            # and check7StorageThreads don't close storages when done
            # with them? This leads to connections remaining open with
            # locks on PyPy, so on PostgreSQL we can't TRUNCATE tables
            # and have to go the slow route.
            #
            # As of 2019-06-20 with PyPy 7.1.1, I'm no longer able to replicate
            # a problem like that locally, so we go back to the fast way.
            storage.zap_all()
        return self._wrap_storage(storage)

class MockConnection(object):
    rolled_back = False
    closed = False
    replica = None
    committed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True

    def cursor(self):
        return MockCursor()

class MockCursor(object):
    closed = False

    def __init__(self):
        self.executed = []
        self.inputsizes = {}
        self.results = []
        self.many_results = None

    def setinputsizes(self, **kw):
        self.inputsizes.update(kw)

    def execute(self, stmt, params=None):
        params = tuple(params) if isinstance(params, list) else params
        self.executed.append((stmt, params))

    def fetchone(self):
        return self.results.pop(0)

    def fetchall(self):
        if self.many_results:
            return self.many_results.pop(0)
        r = self.results
        self.results = None
        return r

    def close(self):
        self.closed = True

    def __iter__(self):
        for row in self.results:
            yield row

class MockOptions(Options):
    cache_module_name = '' # disable
    cache_servers = ''
    cache_local_mb = 1
    cache_local_dir_count = 1 # shrink

    @classmethod
    def from_args(cls, **kwargs):
        inst = cls()
        for k, v in kwargs.items():
            setattr(inst, k, v)
        return inst

    def __setattr__(self, name, value):
        if name not in Options.valid_option_names():
            raise AttributeError("Invalid option", name) # pragma: no cover
        object.__setattr__(self, name, value)

class MockConnectionManager(object):

    def __init__(self, driver=None):
        if driver is None:
            self.driver = MockDriver()

    def rollback_quietly(self, conn, cursor): # pylint:disable=unused-argument
        if hasattr(conn, 'rollback'):
            conn.rollback()

    def rollback_and_close(self, conn, cursor):
        self.rollback_quietly(conn, cursor)
        if conn:
            conn.close()
        if cursor:
            cursor.close()

    def open_for_load(self):
        conn = MockConnection()
        return conn, conn.cursor()

    open_for_store = open_for_load

    def restart_load(self, conn, cursor):
        pass

    restart_store = restart_load

class MockPackUndo(object):
    pass

class MockOIDAllocator(object):
    pass

class MockQuery(object):

    def __init__(self, raw):
        self.raw = raw

    def execute(self, cursor, params=None):
        cursor.execute(self.raw, params)

class MockPoller(object):

    poll_query = MockQuery('SELECT MAX(tid) FROM object_state')

    def __init__(self, driver=None):
        self.driver = driver or MockDriver()

class DisconnectedException(Exception):
    pass

class CloseException(Exception):
    pass

class LockException(Exception):
    pass

class MockDriver(object):

    disconnected_exceptions = (DisconnectedException,)
    close_exceptions = (CloseException,)
    lock_exceptions = (LockException,)
    illegal_operation_exceptions = ()

    dialect = DefaultDialect()


class MockAdapter(object):

    def __init__(self):
        self.driver = MockDriver()
        self.connmanager = MockConnectionManager()
        self.packundo = MockPackUndo()
        self.oidallocator = MockOIDAllocator()
        self.poller = MockPoller(self.driver)
