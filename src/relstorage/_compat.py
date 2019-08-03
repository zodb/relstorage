# -*- coding: utf-8 -*-
"""
Compatibility shims.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint:disable=unused-import,invalid-name,no-member,undefined-variable
# pylint:disable=no-name-in-module

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


PY3 = sys.version_info[0] == 3
PY2 = not PY3
PYPY = platform.python_implementation() == 'PyPy'
WIN = sys.platform.startswith('win')
MAC = sys.platform.startswith('darwin')

# Dict support

if PY3:
    def list_values(d):
        return list(d.values())
    iteritems = dict.items
    iterkeys = dict.keys
    itervalues = dict.values
else:
    list_values = dict.values
    iteritems = dict.iteritems
    iterkeys = dict.iterkeys
    itervalues = dict.itervalues

if not PYPY:
    OID_TID_MAP_TYPE = BTrees.family64.II.BTree
    OID_OBJECT_MAP_TYPE = BTrees.family64.IO.BTree
    OID_SET_TYPE = BTrees.family64.II.TreeSet
else:
    OID_TID_MAP_TYPE = dict
    OID_OBJECT_MAP_TYPE = dict
    OID_SET_TYPE = set

MAX_TID = BTrees.family64.maxint

def iteroiditems(d):
    # Could be either a BTree, which always has 'iteritems',
    # or a plain dict, which may or may not have iteritems.
    return d.iteritems() if hasattr(d, 'iteritems') else d.items()

# Types

if PY3:
    string_types = (str,)
    unicode = str
    number_types = (int, float)
    from io import StringIO as NStringIO
else:
    string_types = (basestring,)
    unicode = unicode
    number_types = (int, long, float)
    from io import BytesIO as NStringIO

try:
    from abc import ABC
except ImportError:
    import abc
    ABC = abc.ABCMeta('ABC', (object,), {})
    del abc

# Functions
if PY3:
    xrange = range
    intern = sys.intern
    from base64 import encodebytes as base64_encodebytes
    from base64 import decodebytes as base64_decodebytes
    casefold = str.casefold
    from traceback import clear_frames
    clear_frames = clear_frames
else:
    xrange = xrange
    intern = intern
    from base64 import encodestring as base64_encodebytes
    from base64 import decodestring as base64_decodebytes
    casefold = str.lower
    def clear_frames(tb): # pylint:disable=unused-argument
        "Does nothing on Py2."
