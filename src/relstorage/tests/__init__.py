"""relstorage.tests package"""

import unittest

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
