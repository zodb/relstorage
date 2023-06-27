# -*- coding: utf-8 -*-
"""
Tests for _inthashmap.pyx.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from relstorage import _inthashmap # pylint:disable=no-name-in-module
from relstorage.tests import TestCase

# pylint:disable=unnecessary-dunder-call,protected-access

class TestOidSet(TestCase):

    def _makeOne(self, *args):
        return _inthashmap.OidSet(*args)

    def test_empty(self):
        s = self._makeOne()
        self.assertIsEmpty(s)
        self.assertFalse(s)
        self.assertNotIn(42, s)
        self.assertEqual(list(s), [])
        self.assertEqual(s, s)
        self.assertEqual(s, self._makeOne())
        self.assertNotEqual(s, self)

    def test_add(self):
        s = self._makeOne()
        s.add(42)
        self.assertLength(s, 1)
        self.assertEqual(list(s), [42])
        self.assertTrue(s)
        self.assertIn(42, s)
        self.assertEqual(s, s)
        self.assertNotEqual(s, self)

        s2 = self._makeOne()
        self.assertNotEqual(s, s2)

        s2.add(42)
        self.assertEqual(s, s2)

    def _get_mock_data(self):
        return [1, 2, 3, 2, 1, 42, 42, 13]

    def _get_sorted_unique_mock_data(self):
        return sorted(set(self._get_mock_data()))

    def _check_created_or_updated(self, s):
        uniq_data = self._get_sorted_unique_mock_data()
        self.assertLength(s, len(uniq_data))
        self.assertEqual(sorted(s), uniq_data)
        self.assertTrue(s)
        for i in uniq_data:
            self.assertIn(i, s)

    def _check_update(self, data):
        s = self._makeOne()
        s.update(data)
        self._check_created_or_updated(s)

    def _check_create(self, data):
        s = self._makeOne(data)
        self._check_created_or_updated(s)

    def test_update_from_list(self):
        self._check_update(self._get_mock_data())

    def test_update_from_generator(self):
        self._check_update(x for x in self._get_mock_data())

    def test_update_from_set(self):
        self._check_update(set(self._get_mock_data()))

    def test_update_from_OidSet(self):
        s = self._makeOne(self._get_mock_data())
        self._check_update(s)

    def test_create_with_list(self):
        self._check_create(self._get_mock_data())

    def test_create_from_generator(self):
        self._check_create(x for x in self._get_mock_data())

    def test_create_from_empty_tuple(self):
        s = self._makeOne(())
        self.assertEqual(s, self._makeOne())

    def test_create_from_set(self):
        self._check_create(set(self._get_mock_data()))

    def test_create_from_OidSet(self):
        s = self._makeOne(self._get_mock_data())
        self._check_create(s)

    def test_not_hashable(self):
        s = self._makeOne()
        with self.assertRaises(TypeError):
            hash(s)

        s.add(42)
        with self.assertRaises(TypeError):
            hash(s)


class TestSet(TestOidSet):
    # We want to be sure to be API compatible with
    # standard sets.
    def _makeOne(self, *args):
        return set(*args)


class TestDict(TestCase):

    SUPPORTS_EQUALITY = True
    SUPPORTS_HASHING = False

    def _makeOne(self, *args):
        return dict(*args)

    def test_empty(self):
        s = self._makeOne()
        return self._check_empty(s)

    def _check_empty(self, s):
        self.assertIsEmpty(s)
        self.assertFalse(s)
        self.assertNotIn(42, s)
        self.assertEqual(list(s), [])
        self.assertEqual(s, s)
        if self.SUPPORTS_EQUALITY:
            self.assertEqual(s, self._makeOne())
        self.assertNotEqual(s, self)
        self.assertEqual(list(s.values()), [])
        self.assertEqual(list(s.items()), [])
        self.assertEqual(list(s.keys()), [])
        with self.assertRaises(KeyError):
            s.__getitem__(42)
        self.assertEqual(s.get(42, "Hi"), "Hi")
        self.assertIsNone(s.get(42))
        return s

    def test_not_hashable(self):
        if self.SUPPORTS_HASHING:
            self.skipTest("Supports hashing")
        s = self._makeOne()
        with self.assertRaises(TypeError):
            hash(s)

        s[42] = 42
        with self.assertRaises(TypeError):
            hash(s)

    def test__setitem__(self):
        s = self._makeOne()
        s[42] = 24
        return self._check_42_to_24(s)

    def _check_42_to_24(self, s):
        self.assertLength(s, 1)
        self.assertTrue(s)
        self.assertIn(42, s)
        self.assertEqual(s[42], 24)
        self.assertEqual(list(s), [42])
        self.assertEqual(s, s)
        self.assertEqual(list(s.keys()), [42])
        self.assertEqual(list(s.values()), [24])
        self.assertEqual(list(s.items()), [(42, 24)])
        self.assertEqual(s.get(42, "Hi"), 24)
        return s

    def test__delitem__(self):
        s = self._makeOne()
        with self.assertRaises(KeyError):
            s.__delitem__(42)

        s[42] = 24
        s[24] = 42
        self.assertIn(42, s)
        self.assertIn(24, s)
        self.assertLength(s, 2)
        del s[24]
        self.assertLength(s, 1)
        self.assertTrue(s)
        self.assertIn(42, s)
        self.assertEqual(list(s.keys()), [42])
        self.assertEqual(list(s.values()), [24])
        self.assertEqual(list(s.items()), [(42, 24)])
        self.assertEqual(s.get(42, "Hi"), 24)
        with self.assertRaises(KeyError):
            s.__delitem__(24)

        del s[42]
        self._check_empty(s)
        with self.assertRaises(KeyError):
            s.__delitem__(24)
        with self.assertRaises(KeyError):
            s.__delitem__(42)
        with self.assertRaises(KeyError):
            s.__delitem__(16)

    def test_update_from_dict(self):
        s = self._makeOne()
        s.update({42: 24})
        self._check_42_to_24(s)

    def test_update_from_generator(self):
        s = self._makeOne()
        s.update(i for i in ((42, 24),))
        self._check_42_to_24(s)

    def test_update_from_same_kind(self):
        s = self._makeOne()
        s2 = self._makeOne()
        s2[42] = 24
        s.update(s2)
        self._check_42_to_24(s2)

    def test_update_many_sequence(self):
        s = self._makeOne()
        s.update([(42, 13), (24, 42), (42, 24)])
        self.assertLength(s, 2)
        self.assertEqual(s[42], 24)
        self.assertEqual(s[24], 42)

    def test_update_many_dict(self):
        s = self._makeOne()
        s.update({42: 24, 24: 42})
        self.assertLength(s, 2)
        self.assertEqual(s[42], 24)
        self.assertEqual(s[24], 42)

    def test_views_can_iterate_many_times(self):
        s = self._makeOne()
        s.update({42: 24, 24: 42})

        keys = s.keys()
        self.assertEqual(sorted(keys), [24, 42])
        self.assertEqual(sorted(keys), [24, 42])
        self.assertEqual(sorted(keys), [24, 42])

        values = s.values()
        self.assertEqual(sorted(values), [24, 42])
        self.assertEqual(sorted(values), [24, 42])
        self.assertEqual(sorted(values), [24, 42])

        items = s.items()
        self.assertEqual(sorted(items), [(24, 42), (42, 24)])
        self.assertEqual(sorted(items), [(24, 42), (42, 24)])
        self.assertEqual(sorted(items), [(24, 42), (42, 24)])


class TestBTreeMap(TestDict):
    SUPPORTS_EQUALITY = False
    SUPPORTS_HASHING = True
    def _makeOne(self, *args):
        import BTrees
        return BTrees.family64.UU.BTree(*args)

    def test_update_from_generator(self):
        self.skipTest("BTrees cant use a generator")

class TestOidTidMap(TestDict):
    # We want to be sure to be API compatible
    # with standard maps, with the exception of not allowing
    # negative keys
    ALLOWS_NEGATIVE = False

    def _makeOne(self, *args):
        return _inthashmap.OidTidMap(*args)

    def test_empty(self):
        # pylint:disable=no-member
        s = super().test_empty()

        with self.assertRaises(ValueError):
            s.minValue()
        with self.assertRaises(ValueError):
            s.maxValue()

    def test__setitem__(self):
        # pylint:disable=no-member
        s = super().test__setitem__()
        self.assertEqual(24, s.minValue())
        self.assertEqual(24, s.maxValue())

    def test__setitem__negative_key(self):
        s = self._makeOne()
        with self.assertRaises(TypeError):
            s.__setitem__(-1, 2)

    def test__setitem__negative_value(self):
        s = self._makeOne()
        with self.assertRaises(TypeError):
            s.__setitem__(1, -2)

    def test_update_with_negatives(self):
        s = self._makeOne()
        with self.assertRaises(TypeError):
            s.update({1: -1})
        with self.assertRaises(TypeError):
            s.update({-1: 1})

    def test_update_with_non_ints(self):
        s = self._makeOne()
        with self.assertRaises(TypeError):
            s.update({'foo': 'bar'})

        with self.assertRaises(TypeError):
            s.update([('foo', 'bar')])

    def test_difference(self):
        s = self._makeOne({
            1: 1,
            2: 2,
            3: 3,
            4: 4,
            5: 5
        })

        # With itself
        result = s.difference(s)
        self.assertEmpty(result)
        self.assertIsInstance(result, _inthashmap.OidTidMap)
        self.assertIsNot(result, s)
        self.assertLength(s, 5)

        # With empty
        s2 = self._makeOne()
        result = s.difference(s2)
        self.assertEqual(result, s)
        self.assertIsNot(result, s)
        self.assertLength(s, 5)

        # Partial overlap
        s2.update({
            1: 0, 2: 0, 6: 0
        })
        result = s.difference(s2)
        self.assertLength(s, 5)
        self.assertLength(result, 3)
        self.assertEqual(dict(result), {3: 3, 4: 4, 5: 5})

    def test__multiunion(self):
        s = self._makeOne({1: 1, 2: 2})
        s2 = self._makeOne({2: 3, 3: 3, 4: 4})
        s3 = self._makeOne({4: 4, 5: 5, 1: 1})

        result = _inthashmap.OidTidMap._multiunion([s, s2, s3])
        self.assertEqual(result, [1, 2, 3, 4, 5])


        result = _inthashmap.OidTidMap._multiunion([s3, s2, s])
        self.assertEqual(result, [1, 2, 3, 4, 5])
