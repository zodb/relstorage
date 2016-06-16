# -*- coding: utf-8 -*-
"""
Compatibility shims.

"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

import sys
PY3 = sys.version_info[0] == 3

# Dict support

if PY3:
    def list_keys(d):
        return list(d.keys())
    def list_items(d):
        return list(d.items())
    def list_values(d):
        return list(d.values())
    iteritems = dict.items
    iterkeys = dict.keys
else:
    list_keys = dict.keys
    list_items = dict.items
    list_values = dict.values
    iteritems = dict.iteritems
    iterkeys = dict.iterkeys

# Types

if PY3:
    string_types = (str,)
    unicode = str
else:
    string_types = (basestring,)
    unicode = unicode


# Functions
if PY3:
    xrange = range
    intern = sys.intern
else:
    xrange = xrange
    intern = intern

# Database types

if PY3:
    # psycopg2 is smart enough to return memoryview or
    # buffer on Py3/Py2, respectively, for bytea columns
    _db_binary_types = (memoryview,)
    def bytes_to_pg_binary(data):
        # bytes under Py2 is perfectly acceptable
        return data
else:
    _db_binary_types = (memoryview, buffer)
    # bytes is str under py2, so must be memoryview
    # There is a psycopg2.Binary type that should do basically the same thing
    try:
        from psycopg2 import Binary as _psyBinary
    except ImportError:
        bytes_to_pg_binary = memoryview
    else:
        bytes_to_pg_binary = _psyBinary

def db_binary_to_bytes(data):
    if isinstance(data, _db_binary_types):
        data = bytes(data)
    return data


from ZODB._compat import BytesIO
StringIO = BytesIO

from ZODB._compat import dumps, loads
