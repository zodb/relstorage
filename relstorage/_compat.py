# -*- coding: utf-8 -*-
"""
Compatibility shims.

"""

from __future__ import print_function, absolute_import, division

# pylint:disable=unused-import,invalid-name,no-member,undefined-variable
# pylint:disable=no-name-in-module,redefined-variable-type

import sys
import platform

PY3 = sys.version_info[0] == 3
PYPY = platform.python_implementation() == 'PyPy'

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
    itervalues = dict.values
else:
    list_keys = dict.keys
    list_items = dict.items
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

# Database types

if PY3:
    # psycopg2 is smart enough to return memoryview or
    # buffer on Py3/Py2, respectively, for bytea columns
    _db_binary_types = (memoryview,)
else:
    _db_binary_types = (memoryview, buffer)

def db_binary_to_bytes(data):
    if isinstance(data, _db_binary_types):
        data = bytes(data)
    return data


# mysqlclient, a binary driver that works for Py2, Py3 and
# PyPy (claimed), uses a connection that is a weakref. MySQLdb
# and PyMySQL use a hard reference
from weakref import ref as _wref
def mysql_connection(cursor):
    conn = cursor.connection
    if isinstance(conn, _wref):
        conn = conn()
    return conn



from ZODB._compat import BytesIO
StringIO = BytesIO

# XXX: This is a private module in ZODB, but it has a lot
# of knowledge about how to choose the right implementation
# based on Python version and implementation. We at least
# centralize the import from here.
from ZODB._compat import dumps, loads
from ZODB._compat import dump
from ZODB._compat import HIGHEST_PROTOCOL
from ZODB._compat import Pickler, Unpickler

import transaction
# Keys/values in extended_info/_extension and user/description on transaction
# are *required* to be text (unicode) in transaction 2.0.
# See https://github.com/zopefoundation/transaction/pull/28
TRANSACTION_DATA_IS_TEXT = hasattr(transaction.TransactionManager().begin(), 'extended_info')
