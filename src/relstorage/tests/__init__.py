"""relstorage.tests package"""

import abc
import contextlib
import os
import unittest
import functools

import transaction

from ZODB.Connection import Connection
from ZODB.tests.util import clear_transaction_syncs

from relstorage._compat import ABC
from relstorage.options import Options
from relstorage.adapters.sql import DefaultDialect
from relstorage.adapters.interfaces import ReplicaClosedException

try:
    from unittest import mock as _mock
except ImportError: # Python 2
    import mock as _mock

mock = _mock

@contextlib.contextmanager
def _fakeSubTest(*_args, **_kwargs):
    yield

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

    # 2.7 doesn't have subtest.
    subTest = getattr(
        unittest.TestCase,
        'subTest',
        None
    ) or _fakeSubTest

    none = unittest.TestCase.assertIsNone

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
        try:
            transaction.abort()
        finally:
            super(TestCase, self).tearDown()

    def assertIsEmpty(self, container, msg=None):
        self.assertLength(container, 0, msg)

    assertEmpty = assertIsEmpty

    def assertLength(self, container, length, msg=None):
        self.assertEqual(len(container), length,
                         '%s -- %s' % (msg, container) if msg else container)

    def assertExceptionNotCausedByDeadlock(self, exc, storage):
        cause = exc
        while cause is not None:
            if storage._adapter.driver.exception_is_deadlock(cause):
                raise AssertionError("Exception was a deadlock.", exc)
            cause = getattr(cause, '__relstorage_cause__', None)

def skipIfNoConcurrentWriters(func):
    # Skip the test if only one writer is allowed at a time.
    # e.g., sqlite
    @functools.wraps(func)
    def f(self):
        if self._storage._adapter.WRITING_REQUIRES_EXCLUSIVE_LOCK:
            self.skipTest("Storage doesn't support multiple writers")
        func(self)
    return f


class StorageCreatingMixin(ABC):

    keep_history = None # Override
    driver_name = None # Override.
    zap_slow = False # Override

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

    DEFAULT_COMMIT_LOCK_TIMEOUT = 10

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
            kw['commit_lock_timeout'] = self.DEFAULT_COMMIT_LOCK_TIMEOUT

        assert self.driver_name
        options = Options(keep_history=self.keep_history, driver=self.driver_name, **kw)
        adapter = self.make_adapter(options)
        storage = RelStorage(adapter, options=options)
        if zap:
            storage.zap_all(slow=self.zap_slow)
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
        return MockCursor(self)

class MockCursor(object):
    closed = False
    sort_sequence_params = False

    def __init__(self, conn=None):
        self.executed = []
        self.inputsizes = {}
        self.results = []
        self.many_results = None
        self.connection = conn

    def setinputsizes(self, **kw):
        self.inputsizes.update(kw)

    def execute(self, stmt, params=None):
        if self.sort_sequence_params and isinstance(params, (tuple, list)):
            params = sorted(sorted(p) if isinstance(p, (tuple, list)) else p
                            for p in params)

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

    isolation_load = 'SERIALIZABLE'
    clean_rollback = None
    _ignored_exceptions = (ReplicaClosedException,)

    def __init__(self, driver=None, clean_rollback=None):
        if driver is None:
            self.driver = MockDriver()
        if clean_rollback is not None:
            self.clean_rollback = clean_rollback
        self._ignored_exceptions = (ReplicaClosedException,) + self.driver.disconnected_exceptions

    def rollback_quietly(self, conn, cursor): # pylint:disable=unused-argument
        if hasattr(conn, 'rollback'):
            conn.rollback()
        return self.clean_rollback

    rollback_store_quietly = rollback_quietly

    def rollback_and_close(self, conn, cursor):
        self.rollback_quietly(conn, cursor)
        if conn:
            conn.close()
        if cursor:
            cursor.close()

    def open_for_load(self):
        conn = MockConnection()
        return conn, self.configure_cursor(conn.cursor())

    def configure_cursor(self, cur):
        return cur

    open_for_store = open_for_load

    def restart_load(self, conn, cursor, needs_rollback=True):
        pass

    restart_store = restart_load

    def cursor_for_connection(self, conn):
        return conn.cursor()

    def open_and_call(self, func):
        return func(*self.open_for_load())

    def describe_connection(self, _conn, _cursor):
        return "<MockConnectionManager>"

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
    last_requested_range = None
    def __init__(self, driver=None):
        self.driver = driver or MockDriver()
        self.changes = []  # [(oid, tid)]
        self.poll_tid = 1
        self.poll_changes = None

    def poll_invalidations(self, _conn, _cursor, _since):
        return self.poll_changes, self.poll_tid

    def get_current_tid(self, _cursor):
        return self.poll_tid


class DisconnectedException(Exception):
    pass

class CloseException(Exception):
    pass

class LockException(Exception):
    pass

class MockDriver(object):
    Binary = bytes
    supports_64bit_unsigned_id = True
    cursor_arraysize = 64
    disconnected_exceptions = (DisconnectedException,)
    close_exceptions = (CloseException,)
    lock_exceptions = (LockException,)
    illegal_operation_exceptions = ()

    dialect = DefaultDialect()

    isolation_load = 'SERIALIZABLE'
    def connection_may_need_rollback(self, conn): # pylint:disable=unused-argument
        return True
    connection_may_need_commit = connection_may_need_rollback

    def enter_critical_phase_until_transaction_end(self, connection, cursor):
        "Does nothing"

    is_in_critical_phase = enter_critical_phase_until_transaction_end
    exit_critical_phase = enter_critical_phase_until_transaction_end

    def commit(self, conn):
        conn.commit()

    def rollback(self, conn):
        conn.rollback()

    def synchronize_cursor_for_rollback(self, cursor):
        if cursor is not None:
            cursor.fetchall()

    def exception_is_deadlock(self, exc): # pylint:disable=unused-argument
        return None

class MockObjectMover(object):
    def __init__(self):
        self.data = {}  # {oid_int: (state, tid_int)}

    def load_current(self, _cursor, oid_int):
        return self.data.get(oid_int, (None, None))

    def current_object_tids(self, _cursor, oids, timeout=None):
        # pylint:disable=unused-argument
        return {
            oid: self.data[oid][1]
            for oid in oids
            if oid in self.data
        }

class MockLocker(object):

    def release_commit_lock(self, cursor):
        pass

class MockAdapter(object):

    def __init__(self):
        self.driver = MockDriver()
        self.connmanager = MockConnectionManager()
        self.packundo = MockPackUndo()
        self.oidallocator = MockOIDAllocator()
        self.poller = MockPoller(self.driver)
        self.mover = MockObjectMover()
        self.locker = MockLocker()
