# -*- coding: utf-8 -*-
"""
Compatibility shims.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import array
import functools
import os
import platform
import sys

import BTrees
# XXX: This is a private module in ZODB, but it has a lot
# of knowledge about how to choose the right implementation
# based on Python version and implementation. We at least
# centralize the import from here.
from ZODB._compat import HIGHEST_PROTOCOL
from ZODB._compat import Pickler
from ZODB._compat import Unpickler
from ZODB._compat import dump
from ZODB._compat import dumps
from ZODB._compat import loads

__all__ = [
    # ZODB exports
    'HIGHEST_PROTOCOL',
    'Pickler',
    'Unpickler',
    'dump',
    'dumps',
    'loads',

    # Constants
    'PY3',
    'PY2',
    'PY36',
    'PYPY',
    'WIN',
    'MAC',
    'IN_TESTRUNNER',

    # dicts
    'list_values',
    'iteritems',
    'iterkeys',
    'itervalues',

    # OID and TID datastructures and algorithms
    "OID_TID_MAP_TYPE",
    'OID_OBJECT_MAP_TYPE',
    'OID_SET_TYPE',
    'OidTMap_difference',
    'OidTMap_multiunion',
    'OidTMap_intersection',
    'OidList',

    'MAX_TID',
    'iteroiditems',
    'string_types',
    'NStringIO',
    'wraps',
    'ABC',
    'base64_encodebytes',
    'base64_decodebytes',
    'update_wrapper',

    # Clocks
    'perf_counter',
]

PY3 = sys.version_info[0] == 3
PY36 = sys.version_info[:2] >= (3, 6)
PY2 = not PY3
PYPY = platform.python_implementation() == 'PyPy'
WIN = sys.platform.startswith('win')
MAC = sys.platform.startswith('darwin')

try:
    # Python 3.3+ (PEP 418)

    # The value (in fractional seconds) of a performance counter, i.e.
    # a clock with the highest available resolution to measure a short
    # duration. It does include time elapsed during sleep and is
    # system-wide. The reference point of the returned value is
    # undefined, so that only the difference between the results of
    # consecutive calls is valid.
    from time import perf_counter
except ImportError:
    import time

    if sys.platform == "win32":
        perf_counter = time.clock # pylint: disable=no-member
    else:
        perf_counter = time.time

    del time

# Dict support

if PY3:
    def list_values(d):
        return list(d.values())
    iteritems = dict.items
    iterkeys = dict.keys
    itervalues = dict.values
else:
    list_values = dict.values
    iteritems = dict.iteritems  # pylint:disable=no-member
    iterkeys = dict.iterkeys  # pylint:disable=no-member
    itervalues = dict.itervalues  # pylint:disable=no-member

# OID and TID data structures.
#
# The cache MVCC implementation depends on the map types being atomic
# for primitive operations, so don't accept Python BTree
# implementations. (Also, on PyPy, the Python BTree implementation
# uses more memory than a dict.)
#
# The map types are rarely large, but they could sometimes be.
# The set types are more likely to be large (e.g., a set is used to hold
# all the OIDs we need to poll after reading from a persistent cache).

if BTrees.LLBTree.LLBTree is not BTrees.LLBTree.LLBTreePy: # pylint:disable=no-member
    # For BTree and Tree set objects, if you subclass you can define two attributes
    # to customize their allocation (simplified):
    #
    # - ``max_internal_size`` is the number of buckets the root BTree
    # is allowed to hold before it splits. (Actually, it goes to twice
    # that size before it splits.) Each time it needs to resize its
    # internal array, it doubles. In the C code, the default for this
    # is DEFAULT_MAX_BTREE_SIZE == 500.
    #
    # - ``max_leaf_size`` is how big (strictly) a bucket is allowed to
    # be before *it* splits. Buckets start out by allocating an array
    # of size 16 and they double their allocation each time they have
    # to grow. Unfortunately there's no way to give them a suggested
    # size and have them allocate that. In the C code, the default for
    # this is DEFAULT_MAX_BUCKET_SIZE == 120
    #
    # Since these are only in-memory, there's no real downside to
    # increasing them all substantially; it will result in larger
    # memory allocations and better memory locality. The algorithms
    # are still logarithmic (and actually creation and lookups
    # benchmark slightly faster for larger values). The only time that
    # could be a problem is if they're *very* large, then the amount
    # of waste could be substantial. Choose values one less than a
    # power of two to avoid that last huge allocation.
    #
    # Using these values and an OidSet of about 800,000 objects (from
    # a real cache file) results in 12,000 fewer LLSet objects being
    # created, and 2MB less memory being used.
    class OID_TID_MAP_TYPE(BTrees.family64.UU.BTree):
        __slots__ = ()
        # Since these are contiguous arrays, there *might* be some
        # benefit in keeping them within a virtual memory page
        # (commonly 4K). The BTree doesn't actually hold an array of
        # Bucket, it holds an array of BTreeItem, which is a key and a
        # pointer (to bucket or tree). Assuming 64-bit pointers, the size of an array is
        # (8 * 2 * len) and we'd have to go down to 255, which is actually a
        # reduction from the default of 500 (8000 bytes, less than two pages).
        # Until we have evidence that keeping in a page matters, we'll just choose
        # some walues that we observed to result in an overall memory use reduction.
        max_internal_size = 1023 # just under 16K, four pages.
        # buckets have an array of keys and (for trees but not sets) an array of values.
        # This is definitely 64 bits
        max_leaf_size = 4095 # under 32K, or 8 pages.

    class OID_OBJECT_MAP_TYPE(BTrees.family64.UO.BTree):
        __slots__ = ()
        max_internal_size = OID_TID_MAP_TYPE.max_internal_size
        max_leaf_size = OID_TID_MAP_TYPE.max_leaf_size

    class OID_SET_TYPE(BTrees.family64.UU.TreeSet):
        max_internal_size = OID_TID_MAP_TYPE.max_internal_size
        max_leaf_size = OID_TID_MAP_TYPE.max_leaf_size

    OidTMap_difference = BTrees.family64.UU.difference  # pylint:disable=no-member
    OidTMap_multiunion = BTrees.family64.UU.multiunion  # pylint:disable=no-member
    OidTMap_intersection = BTrees.family64.UU.intersection  # pylint:disable=no-member
    OidSet_difference = OidTMap_difference
    def OidSet_discard(s, val):
        try:
            s.remove(val)
        except KeyError:
            pass

    def OidObjectMap_max_key(bt):
        if not bt:
            return 0
        return bt.maxKey()
else:
    OID_TID_MAP_TYPE = dict
    OID_OBJECT_MAP_TYPE = dict
    OID_SET_TYPE = set
    def OidTMap_difference(c1, c2):
        # Must prevent iterating while being changed
        c1 = dict(c1)
        return {k: c1[k] for k in set(c1) - set(c2)}

    def OidTMap_multiunion(seq):
        return set().union(*seq)

    def OidTMap_intersection(c1, c2):
        return set(c1).intersection(set(c2))

    def OidSet_difference(c1, c2):
        return set(c1) - set(c2)

    OidSet_discard = set.discard

    def OidObjectMap_max_key(mapping):
        if not mapping:
            return 0
        return max(iterkeys(mapping))

# Lists of OIDs or TIDs. These could be simple list() objects, or we
# can treat them as numbers and store them in array.array objects, if
# we have an unsigned 64-bit element type. array.array, just like the
# C version of BTrees, uses less memory on CPython, but has a cost
# converting back and forth between objects and native values. What's
# the cost? Let's measure.
#
# Test: list(xrange(30000000)) vs array.array('L', xrange(30000000))
#  on Python 2, with minor modifications (range and 'Q') on Python 3.
#
#              list mem  | array mem | list time | array time
# CPython 2:      861MB  |     228MB |    596ms  |     2390ms
# PyPy2 7.1:      229MB  |     227MB |    178ms  |     1830ms
# CPython 3.7:   2117MB  |     232MB |   3680ms  |     3150ms
#
# Test: Same as above, but using 300 instead of 30000000
#               list time | array time
# CPython 2:       6.28ms |     6.3ms
# PyPy2 7.1:       1.34ms |     1.43ms
# CPython 3.7:     3.69ms |     3.74ms
#
# Slicing x(30000000)[30000:30200]
#               list time | array time
# CPython 2:       427ns  |      148ns
# PyPy2 7.1*:      138ns  |     8950ns
# CPython 3.7:     671ns  |      411ns
#
# iterate x(30000000): for _ in x: pass
#               list time | array time  | small list time | small array time
# CPython 2:       357ms  |      604ms  |    2640ns       |  6050ns
# PyPy2 7.1*:       51ms  |      592ms  |     601ns       |  5910ns
# CPython 3.7:     308ms  |     2240ms  |    2250ns       |  6170ns
# * On PyPy, the test was wrapped in a method for better JIT.
#
# Using BTrees.family64.II.TreeSet(range(30000000))
#
#                memory  | construction time | iteration time
# CPython 2:      564MB  |            2740ms |    520ms
# CPython 3.7:    573MB  |            5280ms |   2390ms
#
#
# Observations:
# - Large list() is faster to create on CPython 2, but uses 4x the memory.
# - Large list() is *slower* to create on CPython 3 and uses an incredible
#    9x the memory. Relative to Python 2, I suspect the differences have to do with
#    all Python 3 integers being variable-length long objects, unlike Python 2.
#    I suspect that accounts for much of the difference in general.
# - PyPy memory usage is comparable for both list and array (which makes sense, it has
#    a specialized strategy for lists of integers), but large lists are faster to
#    create for some reason.
# - Creation times for small sets is basically the same on all platforms.
# - Slicing time of arrays is faster on CPython 2 and 3 but much slower on PyPy.
# - Iterating arrays is substantially slower on all platforms and for all sizes.
# - However, creating arrays is faster than creating 64-bit TreeSets; iteration
#   is about the same.
#
# Conclusions:
# Except on PyPy, when working with a large list of OIDs, a 64-bit array.array
# will save a substantial amount of memory. On Python 3, it will probably be slightly
# faster to create too; on both Python 2 and 3 it will be faster and smaller than an equivalent
# TreeSet. Slicing is faster with arrays as well. Iteration is around 3x slower, but that's likely
# to be noise compared to the body of the loop.
# Thus, everywhere except PyPy, if we have an unsigned 64-bit array.array available, that should
# be our choice.
_64bit_array = None
try:
    # Find out if we have a native unsigned 64-bit type
    array.array('Q', [1])
    _64bit_array = functools.partial(array.array, 'Q')
except ValueError:
    # We don't. Either we're on Python 2 or the compiler doesn't support 'long long'.
    # What about a regular unsigned long? If we're on a 64-bit platform, that
    # might be enough.
    a = array.array('L', [1])
    if a.itemsize >= 8:
        _64bit_array = functools.partial(array.array, 'L')

if _64bit_array and not PYPY:
    OidList = _64bit_array
else:
    OidList = list
TidList = OidList
# The maximum theoretical TID (and OID, incidentally).
# This is b"\xff\xff\xff\xff\xff\xff\xff\xff"
# It requires an 8 byte unsigned integer to store.
MAX_TID = BTrees.family64.maxuint
# The maximum TID if only signed values are allowed.
MAX_S_TID = BTrees.family64.maxint

def iteroiditems(d):
    # Could be either a BTree, which always has 'iteritems',
    # or a plain dict, which may or may not have iteritems.
    return d.iteritems() if hasattr(d, 'iteritems') else d.items()

# Types

if PY3:
    string_types = (str,)
    number_types = (int, float)
    from io import StringIO as NStringIO
    from functools import wraps
else:
    string_types = (basestring,) # pylint:disable=undefined-variable
    number_types = (int, long, float) # pylint:disable=undefined-variable
    from io import BytesIO as NStringIO
    # On Python 2, functools.update_wrapper doesn't set the '__wrapped__'
    # attribute, and we need that.
    from functools import wraps as _wraps
    class wraps(object):
        def __init__(self, func):
            self._orig = func
            self._wrapper = _wraps(func)

        def __call__(self, replacement):
            replacement = self._wrapper(replacement)
            replacement.__wrapped__ = self._orig
            return replacement


IN_TESTRUNNER = (
    # zope-testrunner --test-path ...
    'zope-testrunner' in sys.argv[0]
    # python -m zope.testrunner --test-path ...
    or os.path.join('zope', 'testrunner') in sys.argv[0]
)


try:
    from abc import ABC
except ImportError:
    import abc
    ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})
    del abc

# Functions
if PY3:
    xrange = range
    intern = sys.intern
    from base64 import encodebytes as base64_encodebytes
    from base64 import decodebytes as base64_decodebytes
    casefold = str.casefold
    from traceback import clear_frames
    clear_frames = clear_frames # pylint:disable=self-assigning-variable
    from functools import update_wrapper
else:
    xrange = xrange # pylint:disable=self-assigning-variable
    intern = intern # pylint:disable=self-assigning-variable
    from base64 import encodestring as base64_encodebytes # pylint:disable=no-name-in-module
    from base64 import decodestring as base64_decodebytes # pylint:disable=no-name-in-module
    casefold = str.lower
    def clear_frames(tb): # pylint:disable=unused-argument
        "Does nothing on Py2."

    from functools import update_wrapper as _update_wrapper
    def update_wrapper(wrapper, wrapped, *args, **kwargs):
        wrapper = _update_wrapper(wrapper, wrapped, *args, **kwargs)
        wrapper.__wrapped__ = wrapped
        return wrapped
