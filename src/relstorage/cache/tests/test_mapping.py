##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import partial

from relstorage.tests import TestCase

from . import MockOptions
from . import SizedLRUMapping
from . import list_lrukeys as list_lrukeys_

class SizedLRUMappingTests(TestCase):

    def assertNone(self, o):
        if o is not None:
            raise AssertionError("Expected None, not %r" % (o,))

    def assertNotNone(self, o):
        if o is None:
            raise AssertionError("Expected not None")

    def getClass(self):
        return SizedLRUMapping

    def test_get_empty(self):
        c = self.getClass()(100)
        with self.assertRaises(KeyError):
            c[(0, 0)] # pylint:disable=pointless-statement

    def test_age_empty(self):
        c = self.getClass()(100)
        c._age_factor = 0
        c._age()

    def test_set_bytes_value(self):
        b = self.getClass()(100)
        self.assertEqual(b.size, 0)
        b['abc'] = b'defghi'
        self.assertEqual(b.size, 9)
        self.assertEqual(b['abc'], b'defghi')
        # Other Mapping APIs
        self.assertIn('abc', b)
        self.assertEqual(list(b.keys()),
                         ['abc'])
        self.assertEqual(list(b.items()),
                         [('abc', b'defghi')])
        self.assertEqual(list(b.values()),
                         [b'defghi'])

        b['abc'] = b'123'
        self.assertEqual(b.size, 6)
        self.assertEqual(list(b.keys()),
                         ['abc'])
        self.assertEqual(list(b.items()),
                         [('abc', b'123')])
        self.assertEqual(list(b.values()),
                         [b'123'])

        b['abc'] = b''
        self.assertEqual(b.size, 3)
        b['abc'] = b'defghi'
        self.assertEqual(b.size, 9)

        del b['abc']
        self.assertEqual(b.size, 0)
        self.assertNotIn('abc', b)
        self.assertEqual(list(b.keys()),
                         [])
        self.assertEqual(list(b.items()),
                         [])
        self.assertEqual(list(b.values()),
                         [])


    def test_set_limit(self):
        b = self.getClass()(5)
        self.assertEqual(b.size, 0)
        b['abc'] = b'xy'
        self.assertEqual(b.size, 5)
        b['abc'] = b'z'
        self.assertEqual(b.size, 4)
        b['abcd'] = b'xyz'
        # In the past this was 7 and 'abc' was ejected. But the generational
        # system lets us go a bit over.
        self.assertEqual(b.size, 11)
        self.assertEqual(b.peek('abc'), b'z')
        self.assertEqual(b.peek("abcd"), b'xyz')

    def test_increasing_size_in_eden_w_empty_protected_bumps_to_protected(self):
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        # Get eden to exactly its size.
        b['a'] = b'x'
        self.assertEqual(b.size, 2)
        self.assertEqual(list_lrukeys('eden'), ['a'])

        b['b'] = b'y'
        self.assertEqual(b.size, 4)
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])

        # Now increase an existing key, thus making it in MRU,
        # and going over size of eden, and bumping down to protected.
        b['a'] = b'xyz'
        self.assertEqual(list_lrukeys('eden'), ['a'])
        self.assertEqual(list_lrukeys('protected'), ['b'])

        self.assertEqual(b.size, 6)

    def test_increasing_size_in_eden_w_partial_protected_bumps_to_protected(self):
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        # Fill up eden and begin spilling to protected
        b['a'] = b'x'
        self.assertEqual(b.size, 2)
        self.assertEqual(list_lrukeys('eden'), ['a'])

        b['b'] = b'y'
        self.assertEqual(b.size, 4)
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])

        b['c'] = b'z'
        self.assertEqual(b.size, 6)
        self.assertEqual(list_lrukeys('eden'), ['b', 'c'])
        self.assertEqual(list_lrukeys('protected'), ['a'])

        # Now increase an existing key, thus making it in MRU,
        # and going over size of eden, and bumping down to protected.
        b['b'] = b'xyz'
        self.assertEqual(list_lrukeys('eden'), ['b'])
        self.assertEqual(list_lrukeys('protected'), ['a', 'c'])
        self.assertEqual(b.size, 8)

    def test_increasing_size_in_eden_w_full_protected_bumps_to_probation(self):
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        # This actually stays in eden because it's the newest key,
        # even though it's too big
        b['a'] = b'x' * 31
        self.assertEqual(b.size, 32)
        self.assertEqual(list_lrukeys('eden'), ['a'])

        # But this will immediately force a into protected
        b['b'] = b'y'
        self.assertEqual(b.size, 34)
        self.assertEqual(list_lrukeys('eden'), ['b'])
        self.assertEqual(list_lrukeys('protected'), ['a'])
        self.assertEqual(list_lrukeys('probation'), [])

        # Ok, now fill up eden with another key
        b['c'] = b'z'
        self.assertEqual(b.size, 36)
        self.assertEqual(list_lrukeys('eden'), ['b', 'c'])
        self.assertEqual(list_lrukeys('protected'), ['a'])
        self.assertEqual(list_lrukeys('probation'), [])

        # Now increase an existing key, thus making it in MRU,
        # and going over size of eden. protected is full, so we go to probation.
        b['b'] = b'xyz'
        self.assertEqual(list_lrukeys('eden'), ['b'])
        self.assertEqual(list_lrukeys('protected'), ['a'])
        self.assertEqual(list_lrukeys('probation'), ['c'])
        self.assertEqual(b.size, 38)

        # Nothing was evicted
        self.assertEqual(b['a'], b'x' * 31)
        self.assertEqual(b['b'], b'xyz')
        self.assertEqual(b['c'], b'z')

    def test_increasing_size_in_full_protected_bumps_to_probation(self):
        # Fill up in the normal way
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        for k in range(10):
            # 10 4 byte entries
            b[str(k)] = 'abc'

        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['0', '1', '2', '3', '4', '5', '6', '7'])
        self.assertEqual(list_lrukeys('probation'), ['8'])
        self.assertEqual(b.size, 40)

        # Now bump protected over size, ejecting to probation.
        # Note that we drop an element to get us in size
        b['3'] = 'abcd'
        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['1', '2', '4', '5', '6', '7', '3'])
        self.assertEqual(list_lrukeys('probation'), ['0'])
        self.assertEqual(b.size, 37)

        # We can access only the ones that remain
        for k in range(8):
            self.assertNotNone(b.peek(str(k)))

        self.assertNone(b.peek('8'))
        self.assertNotNone(b.peek('9'))

    def test_increasing_size_in_full_probation_full_protection_bumps_to_probation(self):
        # Fill up in the normal way
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        for k in range(10):
            # 10 4 byte entries
            b[str(k)] = 'abc'

        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['0', '1', '2', '3', '4', '5', '6', '7'])
        self.assertEqual(list_lrukeys('probation'), ['8'])
        self.assertEqual(b.size, 40)

        # Now increase an entry in probation. This will move it to protected, which
        # will now be oversize.
        # Note that we drop an element to get us within size
        b['8'] = 'abcd'
        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['2', '3', '4', '5', '6', '7', '8'])
        self.assertEqual(list_lrukeys('probation'), ['1'])
        self.assertEqual(b.size, 37)

        # We can access only the ones that remain
        for k in range(1, 10):
            self.assertNotNone(b.peek(str(k)))

        self.assertNone(b.peek('0'))

    def _load(self, bio, bucket):
        bio.seek(0)
        return bucket.read_from_stream(bio)

    def _save(self, bio, bucket, options, byte_limit=None, pickle_fast=False):
        bio.seek(0)
        writer = bio
        count = bucket.write_to_stream(writer,
                                       byte_limit or options.cache_local_dir_write_max_size,
                                       pickle_fast=pickle_fast)
        writer.flush()
        bio.seek(0)
        return count

    def test_load_and_store(self, options=None, pickle_fast=False):
        # pylint:disable=too-many-statements
        from io import BytesIO
        if options is None:
            options = MockOptions()
        client1 = self.getClass()(100)
        client1['abc'] = b'xyz'

        bio = BytesIO()

        self._save(bio, client1, options, pickle_fast=pickle_fast)
        # Regardless of its read frequency, it's still written
        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, stored)
        self.assertEqual(count, 1)

        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, stored)
        self.assertEqual(count, 1)

        self.assertEqual(client1['abc'], client2['abc'])
        self.assertEqual(1, len(client2))
        self.assertEqual(client1.size, client2.size)

        client1.reset_stats()
        client1['def'] = b'123'
        _ = client1['def']
        self.assertEqual(2, len(client1))
        client1_max_size = client1.size
        bio = BytesIO()
        count_written = self._save(bio, client1, options)
        self.assertEqual(2, len(client1))
        self.assertEqual(count_written, len(client1))

        # This time there's too much data, so an arbitrary
        # entry gets dropped
        client2 = self.getClass()(7)
        count, stored = self._load(bio, client2)
        self.assertEqual(0, len(client2))
        self.assertEqual(count, 2)
        self.assertEqual(stored, 0)

        client2 = self.getClass()(8)
        count, stored = self._load(bio, client2)
        self.assertEqual(2, len(client2))
        self.assertEqual(count, 2)
        self.assertEqual(stored, 2)


        # Duplicate keys ignored.
        # Note that we do this in client1, because if we do it in client2,
        # the first key (abc) will push out the existing 'def' and get
        # inserted, and then 'def' will push out 'abc'
        count, stored = self._load(bio, client1)
        self.assertEqual(count, 2)
        self.assertEqual(stored, 0)
        self.assertEqual(2, len(client1))


        # Half duplicate keys
        self.assertEqual(2, len(client1))
        del client1['abc']
        self.assertEqual(1, len(client1))

        count, stored = self._load(bio, client1)
        self.assertEqual(client1['def'], b'123')
        self.assertEqual(client1['abc'], b'xyz')
        self.assertEqual(count, 2)
        self.assertEqual(stored, 1)
        self.assertEqual(client1.size, client1_max_size)

        # Even keys that have been aged down to 0 still get
        # written.
        # Force the conditions for it to actually do something.
        client1.limit = 0
        client1._age_factor = 0
        client1._age()
        client1._age()
        self.assertEqual(len(client1), 2)
        self.assertEqual(client1.size, client1_max_size)

        bio = BytesIO()
        self._save(bio, client1, options, client1_max_size, pickle_fast=pickle_fast)


        client1 = self.getClass()(100)
        count, stored = self._load(bio, client1)
        self.assertEqual(count, 2)
        self.assertEqual(stored, 2)
        self.assertEqual(client1.size, client1_max_size)

        # Because the keys had equal frequencies, then when
        # they were sorted by frequency, it came down to the original
        # iteration order. In the past, we iterated through the cffi rings,
        # but that's relatively slow. Now we iterate across our internal
        # dictionary, which can have arbitrary orders,
        # so we can't actually predict which key wound up where.

        list_lrukeys = partial(list_lrukeys_, client1)
        self.assertEqual(list_lrukeys('probation'), [])

        self.assertEqual(len(list_lrukeys('eden')), 1)
        self.assertEqual(len(list_lrukeys('protected')), 1)

        # Don't write anything if the limit is too small, but
        # we can still read it.
        bio = BytesIO()
        self._save(bio, client1, options, 1)

        client2 = self.getClass()(3)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, 0)
        self.assertEqual(stored, 0)

        # If the limit is smaller than the size, write the most frequently used
        # items
        client1 = self.getClass()(100)
        list_lrukeys = partial(list_lrukeys_, client1)
        client1['a'] = b'1'
        client1['b'] = b'2'
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])
        client1.get_and_bubble_all(('a',))
        client1.get_and_bubble_all(('a',))
        client1.get_and_bubble_all(('a',))
        self.assertEqual(list_lrukeys('eden'), ['b', 'a'])
        client1.get_and_bubble_all(('b',))
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])
        client1.get_and_bubble_all(('a',))
        self.assertEqual(list_lrukeys('eden'), ['b', 'a'])

        # A is much more popular than b

        bio = BytesIO()
        self._save(bio, client1, options, 2, pickle_fast=pickle_fast)

        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, 1)
        self.assertEqual(stored, 1)
        self.assertEqual(list_lrukeys_(client2, 'eden'), ['a'])


    def test_load_and_store_fast(self):
        self.test_load_and_store(pickle_fast=True)

    def test_get_backup_not_found(self):
        c = self.getClass()(100)
        c.get_from_key_or_backup_key(None, None)
        self.assertEqual(c._misses, 1)

    def test_get_backup_at_pref(self):
        c = self.getClass()(100)
        c['a'] = b'1'
        c['b'] = b'2'

        result = c.get_from_key_or_backup_key('a', 'b')
        self.assertEqual(result, b'1')
        self.assertEqual(c._hits, 1)

    def test_get_backup_at_backup(self):
        c = self.getClass()(100)
        c['b'] = b'1'

        result = c.get_from_key_or_backup_key('a', 'b')
        self.assertEqual(result, b'1')
        self.assertEqual(c._hits, 1)
        self.assertEqual(c._sets, 1)
        self.assertEqual(len(c), 1)
        self.assertNotIn('b', c)
