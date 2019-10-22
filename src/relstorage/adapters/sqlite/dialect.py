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
"""
sqlite3 dialect.

There are a number of variations, depending what is supported by the
version of sqlite.

Sqlite supports ordered parameters with ``?`` and named parameters with ``:name``.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from sqlite3 import sqlite_version_info as sq3_version

from ..sql import DefaultDialect
from ..sql import Compiler as DefaultCompiler

from ..sql import OID
from ..sql import TID
from ..sql import BinaryString
from ..sql import State
from ..sql import Boolean

# Can sqlite3 enforce foreign key constraints? Note that you must
# enable this with 'PRAGMA foreign_keys=on'
SQ3_CAN_ENFORCE_FOREIGN_KEYS = sq3_version >= (3, 6, 19) # 2009-10-14

SQ3_SUPPORTS_WAL = sq3_version >= (3, 7)
SQ3_SUPPORTS_LENGTH_ON_BLOB = sq3_version >= (3, 7, 6)
SQ3_SUPPORTS_MULTIPLE_VALUES_INSERTS = sq3_version >= (3, 7, 11)
# Can multiple different connections share an in-memory database?
SQ3_SHARED_CACHE_SUPPORTS_MEMORY = sq3_version >= (3, 7, 13) # 2012-06-11

SQ3_SUPPORTS_CTE = sq3_version >= (3, 8, 3) # 2014-02-03

SQ3_SUPPORTS_PAREN_UPDATE = sq3_version >= (3, 15) # 2016-10-14
# Will it parse TRUE and FALSE as synonyms for 1/0 in queries?
SQ3_SUPPORTS_BOOL_KW = sq3_version >= (3, 23) # 2018-04-02
SQ3_SUPPORTS_UPSERT = sq3_version >= (3, 24) # 2018-06-04
SQ3_SUPPORTS_WINDOW = sq3_version >= (3, 25) # 2018-09-15



class _Sqlite3UpsertCompiler(DefaultCompiler):
    # We inherit (most of) the default upsert syntax (sqlite matches postgres
    # in that area).

    def can_prepare(self):
        # sqlite3 has no notion of prepared statements.
        return False

    def emit_identifier(self, identifier, quoted=False):
        if identifier == "transaction":
            # Sigh. This has to be quoted for some reason.
            quoted = True
        super(_Sqlite3UpsertCompiler, self).emit_identifier(identifier, quoted)

    if not SQ3_SUPPORTS_BOOL_KW:
        def visit_boolean_literal_expression(self, value):
            assert isinstance(value, bool)
            self.emit('1' if value else '0')

    def _placeholder(self, key):
        if key == '?':
            return key
        return ':%s' % (key,)

    def visit_ordered_bind_param(self, bind_param):
        self.placeholders[bind_param] = '?'
        self.emit('?')

    def visit_no_values(self):
        # Called following a VALUES statement when we have nothing.
        # But sqlite doesn't like (), there must be at least one
        # column
        self.emit("(NULL)")

    def visit_upsert_after_select(self, select):
        if select.is_unconstrained():
            # "The parser might not be able to tell if the "ON" keyword is
            # introducing the UPSERT or if it is the ON clause of a join.
            # To work around this, the SELECT statement should always
            # include a WHERE clause, even if that WHERE clause is just
            # ``WHERE true``."
            self.emit(' WHERE true ')

class _Sqlite3NoUpsertCompiler(_Sqlite3UpsertCompiler):
    # we have to disable the upsert syntax, we're limited to the old
    # syntax. The old 'INSERT OR REPLACE' syntax is supported from
    # 3.0.0 forward. It's not as flexible as the true upsert: for
    # example, you can't specify the type of conflict, nor can you use
    # a WHERE clause or specify the values to use in the update. It
    # also doesn't increment the cursor's change counter for replaced
    # rows.

    def emit_keyword_upsert(self):
        self.emit_keyword("INSERT OR REPLACE INTO")

    def visit_upsert_conflict_column(self, column):
        pass

    def visit_upsert_conflict_update(self, update):
        pass

    def visit_upsert_excluded_column(self, column):
        pass

    def visit_upsert_after_select(self, select):
        "No need to do anything"

SqliteCompiler = _Sqlite3UpsertCompiler if SQ3_SUPPORTS_UPSERT else _Sqlite3NoUpsertCompiler

class Sqlite3Dialect(DefaultDialect):
    datatype_map = {
        OID: 'INTEGER',
        TID: 'INTEGER',
        BinaryString: 'BLOB',
        State: 'BLOB',
        Boolean: 'INTEGER',
    }

    STMT_TRUNCATE = 'DELETE FROM'
    STMT_IF_NOT_EXISTS = 'IF NOT EXISTS'

    CONSTRAINT_AUTO_INCREMENT = 'AUTOINCREMENT'

    def compiler_class(self):
        return SqliteCompiler
