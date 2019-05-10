
import unittest


class TestTreeMarker(unittest.TestCase):

    @property
    def _class(self):
        from ..treemark import TreeMarker
        return TreeMarker

    def _make(self):
        return self._class()

    def test_simple_tree(self):
        obj = self._make()
        obj.add_refs([
            (5, 7),
            (5, 9),
            (6, 1),
        ])
        obj.mark([5])
        self.assertEqual(set(obj.reachable), set([5, 7, 9]))

    def test_multiple_passes_required(self):
        obj = self._make()
        obj.add_refs([
            (3, 1),
            (5, 7),
            (5, 9),
            (6, 1),
            (9, 3),
            (9, 13),
        ])
        obj.mark([5])
        self.assertEqual(set(obj.reachable), set([1, 3, 5, 7, 9, 13]))

    def test_circular(self):
        obj = self._make()
        obj.add_refs([
            (5, 7),
            (5, 8),
            (7, 5),
            (8, 9),
            (9, 5),
            (10, 1),
        ])
        obj.mark([5])
        self.assertEqual(set(obj.reachable), set([5, 7, 8, 9]))

    def test_64bit(self):
        obj = self._make()
        obj.add_refs([
            (5, 7),
            (5, 9 << 32),
            (6 << 32, 1),
        ])
        obj.mark([5])
        self.assertEqual(set(obj.reachable), set([5, 7, 9 << 32]))

    def test_64bit_with_multiple_passes_required(self):
        obj = self._make()
        obj.add_refs([
            (3 << 32, 1),
            (5, 7),
            (5, 9 << 32),
            (6 << 32, 1),
            (9 << 32, 3 << 32),
            (9 << 32, 13),
        ])
        obj.mark([5])
        self.assertEqual(set(obj.reachable), set([
            1, 3 << 32, 5, 7, 9 << 32, 13]))

    def test_64bit_circular(self):
        obj = self._make()
        obj.add_refs([
            (5, 7),
            (5, 8 << 32),
            (7, 5),
            (8 << 32, 9 << 32),
            (9 << 32, 5),
            (10 << 32, 1),
        ])
        obj.mark([5])
        self.assertEqual(set(obj.reachable), set([5, 7, 8 << 32, 9 << 32]))

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestTreeMarker))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
