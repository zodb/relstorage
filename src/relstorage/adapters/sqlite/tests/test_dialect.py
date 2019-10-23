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

from relstorage.adapters.sql.tests import test_sql

from .. import dialect

class TestSQLiteUpsertDialect(test_sql.TestUpsert):
    keep_history = False
    dialect = dialect.Sqlite3Dialect()
    dialect.compiler_class = lambda: dialect._Sqlite3UpsertCompiler

    def test_default(self):
        self.assertEqual(
            dialect.SQ3_SUPPORTS_UPSERT,
            dialect.SqliteCompiler is dialect._Sqlite3UpsertCompiler)

    insert_or_replace = test_sql.TestUpsert.insert_or_replace.replace(
        '%s', '?'
    )

    insert_or_replace_subquery = test_sql.TestUpsert.insert_or_replace_subquery.replace(
        '%s', '?'
    )

    upsert_unconstrained_subquery = test_sql.TestUpsert.upsert_unconstrained_subquery.replace(
        '%s', '?'
    ).replace(
        ' ON ', ' WHERE true ON '
    )


class TestSQLiteOldDialect(test_sql.TestUpsert):
    keep_history = False
    dialect = dialect.Sqlite3Dialect()
    dialect.compiler_class = lambda: dialect._Sqlite3NoUpsertCompiler

    insert_or_replace = (
        'INSERT OR REPLACE INTO object_state(zoid, state, tid, state_size) VALUES (?, ?, ?, ?)'
    )

    insert_or_replace_subquery = (
        'INSERT OR REPLACE INTO object_state(zoid, tid, state, state_size) '
        'SELECT zoid, ?, state, COALESCE(LENGTH(state), 0) FROM temp_store ORDER BY zoid'
    )

    upsert_unconstrained_subquery = (
        'INSERT OR REPLACE INTO object_state(zoid, tid, state, state_size) '
        'SELECT zoid, ?, state, COALESCE(LENGTH(state), 0) FROM temp_store'
    )
