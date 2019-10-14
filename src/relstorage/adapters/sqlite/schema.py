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

import os.path
import contextlib

from zope.interface import implementer

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller
from ..schema import Schema

@implementer(ISchemaInstaller)
class Sqlite3SchemaInstaller(AbstractSchemaInstaller):

    COLTYPE_OID_TID = 'INTEGER'
    COLTYPE_BINARY_STRING = 'BLOB'
    COLTYPE_STATE = 'BLOB'
    COLTYPE_BLOB_CHUNK_NUM = 'INTEGER'
    COLTYPE_BLOB_CHUNK = 'BLOB'
    COLTYPE_MD5 = 'BLOB'

    def __init__(self, driver, oid_connmanager=None, **kwargs):
        self.driver = driver
        self.oid_connmanager = oid_connmanager
        super(Sqlite3SchemaInstaller, self).__init__(**kwargs)

    def get_database_name(self, cursor):
        return os.path.splitext(
            os.path.basename(self.connmanager.path))[0]

    def list_tables(self, cursor):
        cursor.execute(
            'SELECT name FROM sqlite_master '
            'WHERE type = "table"'
        )
        return [x[0] for x in cursor.fetchall()]

    def list_procedures(self, cursor):
        return ()

    list_sequences = list_procedures

    _new_oid_query = Schema.new_oid.create(if_not_exists=True)

    def _create_new_oid(self, cursor):
        self._new_oid_query.execute(cursor)
        self.oid_connmanager.open_and_call(
            lambda conn, cursor: self._new_oid_query.execute(cursor)
        )

    def _create_pack_lock(self, cursor):
        """Does nothing."""
    _reset_oid = _create_new_oid

    def _reset_oid(self, cursor):
        from .oidallocator import Sqlite3OIDAllocator
        with contextlib.closing(Sqlite3OIDAllocator(self.driver, self.connmanager)) as oids:
            oids.reset_oid(cursor)

    def drop_all(self):
        super(Sqlite3SchemaInstaller, self).drop_all()
        self.oid_connmanager.open_and_call(self._drop_all)
