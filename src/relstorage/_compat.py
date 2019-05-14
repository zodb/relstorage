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

# Types

if PY3:
    string_types = (str,)
    unicode = str
else:
    string_types = (basestring,)
    unicode = unicode

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
else:
    xrange = xrange
    intern = intern
    from base64 import encodestring as base64_encodebytes
    from base64 import decodestring as base64_decodebytes

## Database types

# Things that can be recognized as a pickled state,
# passed to an io.BytesIO reader, and unpickled.

# psycopg2 is smart enough to return memoryview or
# buffer on Py3/Py2, respectively, for bytea columns.
# memoryview can't be passed to bytes() on Py2 or Py3,
# but it can be passed to cStringIO.StringIO() or io.BytesIO().


# Py MySQL Connector/Python returns a bytearray, whereas
# C MySQL Connector/Python returns bytes.

# Keep these ordered with the most common at the front;
# Python does a linear traversal of type checks.
state_types = (bytes, bytearray, memoryview)

if PY2:
    state_types += (buffer,)

def binary_column_as_state_type(data):
    if isinstance(data, state_types) or data is None:
        return data
    # Nothing we know about. cx_Oracle likes to give us an object
    # with .read(), look for that.
    return binary_column_as_state_type(data.read())

def binary_column_as_bytes(data):
    # Take the same inputs as `as_state_type`, but turn them into
    # actual bytes. This includes None and empty bytes, which becomes
    # the literal b'';
    if data is None or not data:
        return b''
    if isinstance(data, bytes):
        return data
    if isinstance(data, memoryview):
        return data.tobytes()
    # Everything left we convert with the bytes() construtor.
    # That would be buffer and bytearray
    __traceback_info__ = data, type(data)
    return bytes(data)
