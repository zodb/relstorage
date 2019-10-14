# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

from zope.interface import implementer

from relstorage._compat import PY3

from ..drivers import implement_db_driver_options
from ..drivers import AbstractModuleDriver
from ..drivers import MemoryViewBlobDriverMixin
from ..interfaces import IDBDriver

from .dialect import Sqlite3Dialect

__all__ = [
    'Sqlite3Driver',
]

database_type = 'sqlite3'

@implementer(IDBDriver)
class Sqlite3Driver(MemoryViewBlobDriverMixin,
                    AbstractModuleDriver):
    dialect = Sqlite3Dialect()
    __name__ = 'sqlite3'
    MODULE_NAME = __name__

    def __init__(self):
        super(Sqlite3Driver, self).__init__()

        # Sadly, if a connection is closed out from under it,
        # sqlite3 throws ProgrammingError, which is not very helpful.
        # That should really only happen in tests, though, since
        # we're directly connected to the file on disk.
        self.disconnected_exceptions += (self.driver_module.ProgrammingError,)

    if PY3:
        # Py2 returns buffer for blobs, hence the Mixin.
        # But Py3 returns plain bytes.
        def binary_column_as_state_type(self, data):
            return data

implement_db_driver_options(
    __name__,
    '.drivers'
)
