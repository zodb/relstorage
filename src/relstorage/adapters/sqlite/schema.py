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

from zope.interface import implementer

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller


@implementer(ISchemaInstaller)
class Sqlite3SchemaInstaller(AbstractSchemaInstaller):

    COLTYPE_OID_TID = 'INTEGER'
    COLTYPE_BINARY_STRING = 'BLOB'
    COLTYPE_STATE = 'BLOB'
    COLTYPE_BLOB_CHUNK_NUM = 'INTEGER'
    COLTYPE_BLOB_CHUNK = 'BLOB'
    COLTYPE_MD5 = 'BLOB'

    all_tables = tuple(
        t for t in AbstractSchemaInstaller.all_tables
        if t != 'new_oid'
    )

    def __init__(self, driver, oid_allocator, **kwargs):
        self.driver = driver
        self.oid_allocator = oid_allocator
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

    def list_views(self, cursor):
        cursor.execute(
            'SELECT name FROM sqlite_master '
            'WHERE type = "view"'
        )
        return [x[0] for x in cursor.fetchall()]

    def list_procedures(self, cursor):
        return ()

    list_sequences = list_procedures

    def _create_pack_lock(self, cursor):
        """Does nothing."""

    def _reset_oid(self, cursor):
        self.oid_allocator.reset_oid()

    def drop_all(self):
        super(Sqlite3SchemaInstaller, self).drop_all()
        self.oid_allocator.reset_oid()
