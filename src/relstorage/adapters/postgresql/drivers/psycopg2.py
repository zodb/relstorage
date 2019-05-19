# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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
"""
psycopg2 IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage._compat import PY3
from ..._abstract_drivers import AbstractModuleDriver
from ...interfaces import IDBDriver


__all__ = [
    'Psycopg2Driver',
]

def _create_connection(mod):
    class Psycopg2Connection(mod.extensions.connection):
        # The replica attribute holds the name of the replica this
        # connection is bound to.
        __slots__ = ('replica',)

    return Psycopg2Connection


@implementer(IDBDriver)
class Psycopg2Driver(AbstractModuleDriver):
    __name__ = 'psycopg2'
    MODULE_NAME = __name__

    PRIORITY = 1
    PRIORITY_PYPY = 2

    def __init__(self):
        super(Psycopg2Driver, self).__init__()

        psycopg2 = self.get_driver_module()

        # pylint:disable=no-member

        self.Binary = psycopg2.Binary
        self.connect = _create_connection(psycopg2)

        # extensions
        self.ISOLATION_LEVEL_READ_COMMITTED = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
        self.ISOLATION_LEVEL_SERIALIZABLE = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    def connect_with_isolation(self, isolation, *args, **kwargs):
        conn = self.connect(*args, **kwargs)
        conn.set_isolation_level(isolation)
        return conn, conn.cursor()

    # psycopg2 is smart enough to return memoryview or buffer on
    # Py3/Py2, respectively, for bytea columns. memoryview can't be
    # passed to bytes() on Py2 or Py3, but it can be passed to
    # cStringIO.StringIO() or io.BytesIO() --- unfortunately,
    # memoryviews, at least, don't like going to io.BytesIO() on
    # Python 3, and that's how we unpickle states. So while ideally
    # we'd like to keep it that way, to save a copy, we are forced to
    # make the copy. Plus there are tests that like to directly
    # compare bytes.

    if PY3:
        def binary_column_as_state_type(self, data):
            if data:
                # Calling 'bytes()' on a memoryview in Python 3 does
                # nothing useful.
                data = data.tobytes()
            return data
    else:
        def binary_column_as_state_type(self, data):
            if data:
                data = bytes(data)
            return data
