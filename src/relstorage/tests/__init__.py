"""relstorage.tests package"""

import unittest

from relstorage.options import Options

class TestCase(unittest.TestCase):
    # Avoid deprecation warnings; 2.7 doesn't have
    # assertRaisesRegex
    assertRaisesRegex = getattr(
        unittest.TestCase,
        'assertRaisesRegex',
        None
    ) or getattr(unittest.TestCase, 'assertRaisesRegexp')

    def assertIsEmpty(self, container):
        self.assertEqual(len(container), 0)

    assertEmpty = assertIsEmpty

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

    def setinputsizes(self, **kw):
        self.inputsizes.update(kw)

    def execute(self, stmt, params=None):
        params = tuple(params) if isinstance(params, list) else params
        self.executed.append((stmt, params))

    def fetchone(self):
        return self.results.pop(0)

    def fetchall(self):
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
