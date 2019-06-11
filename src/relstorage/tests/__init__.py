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
