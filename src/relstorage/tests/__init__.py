"""relstorage.tests package"""

import abc
import os
import unittest

import transaction

from relstorage._compat import ABC
from relstorage.options import Options

try:
    from unittest import mock
except ImportError: # Python 2
    import mock

mock = mock

class TestCase(unittest.TestCase):
    # Avoid deprecation warnings; 2.7 doesn't have
    # assertRaisesRegex
    assertRaisesRegex = getattr(
        unittest.TestCase,
        'assertRaisesRegex',
        None
    ) or getattr(unittest.TestCase, 'assertRaisesRegexp')

    __to_close = ()

    def setUp(self):
        # This sets up a temporary directory for each test and
        # changes to it.
        super(TestCase, self).setUp()
        self.__to_close = []

    def _closing(self, o):
        """
        Close the object before tearDown (opposite of addCleanup
        so that exceptions will propagate).

        Returns the given object.
        """
        self.__to_close.append(o)
        return o

    def tearDown(self):
        transaction.abort()
        for x in reversed(self.__to_close):
            x.close()
        self.__to_close = ()
        super(TestCase, self).tearDown()

    def assertIsEmpty(self, container):
        self.assertEqual(len(container), 0)

    assertEmpty = assertIsEmpty

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
            # appropriate.
            kw['cache_local_dir'] = '.'

        assert self.driver_name
        options = Options(keep_history=self.keep_history, driver=self.driver_name, **kw)
        adapter = self.make_adapter(options)
        storage = RelStorage(adapter, options=options)
        storage._batcher_row_limit = 1
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
