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
MySQLdb IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage.adapters.interfaces import IDBDriver

from relstorage._compat import PY3
from . import AbstractMySQLDriver

__all__ = [
    'MySQLdbDriver',
    'GeventMySQLdbDriver'
]

@implementer(IDBDriver)
class MySQLdbDriver(AbstractMySQLDriver):
    __name__ = 'MySQLdb'

    MODULE_NAME = 'MySQLdb'
    PRIORITY = 1
    PRIORITY_PYPY = 3
    _GEVENT_CAPABLE = False

    if PY3:
        # Setting the character_set_client = binary results in
        # mysqlclient failing to decode column names. I haven't
        # seen any UTF related warnings from this driver for the state
        # values.
        MY_CHARSET_STMT = 'SET character_set_results = binary'


class GeventMySQLdbDriver(MySQLdbDriver):
    __name__ = 'gevent MySQLdb'

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = False

    _wait_read = None
    _wait_write = None

    def get_driver_module(self):
        __import__('gevent')
        return super(GeventMySQLdbDriver, self).get_driver_module()

    _Connection = None

    @classmethod
    def _get_connection_class(cls):
        if cls._Connection is None:
            # pylint:disable=import-error,no-name-in-module
            from MySQLdb.connections import Connection as Base

            from gevent import socket
            wait_read = socket.wait_read # pylint:disable=no-member
            wait_write = socket.wait_write # pylint:disable=no-member

            class Connection(Base):
                def query(self, query):
                    # From the mysqlclient implementation:
                    # "Since _mysql releases the GIL while querying, we need immutable buffer"
                    if isinstance(query, bytearray):
                        query = bytes(query)

                    fileno = self.fileno()
                    wait_write(fileno)
                    self.send_query(query)
                    wait_read(fileno)
                    self.read_query_result()

            cls._Connection = Connection
        return cls._Connection

    def connect(self, *args, **kwargs):
        # Prior to mysqlclient 1.4, there was a 'waiter' Connection
        # argument that could be used to do this, but it was removed.
        # So we implement it ourself.
        klass = self._get_connection_class()
        return klass(*args, **kwargs) # pylint:disable=not-callable
