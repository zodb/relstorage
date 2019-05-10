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

from __future__ import print_function, absolute_import

from zope.interface import implementer

from ...interfaces import IDBDriver
from ..._abstract_drivers import AbstractModuleDriver

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

    def get_driver_module(self):
        import psycopg2
        return psycopg2
