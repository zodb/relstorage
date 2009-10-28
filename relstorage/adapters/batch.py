##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
"""Batch table row insert/delete support.
"""

import re

class RowBatcher(object):
    """Generic row batcher.

    Expects '%s' parameters and a tuple for each row.
    """

    row_limit = 100
    size_limit = 1<<20
    database_name = None
    support_batch_insert = True

    def __init__(self, cursor):
        self.cursor = cursor
        self.rows_added = 0
        self.size_added = 0
        self.deletes = {}  # {(table, varname): set([value])}
        self.inserts = {}  # {(command, header, row_schema): {rowkey: [row]}}

    def delete_from(self, table, varname, value):
        key = (table, varname)
        values = self.deletes.get(key)
        if values is None:
            self.deletes[key] = values = set()
        values.add(str(value))
        self.rows_added += 1
        if self.rows_added >= self.row_limit:
            self.flush()

    def insert_into(self, header, row_schema, row, rowkey, size,
            command='INSERT'):
        key = (command, header, row_schema)
        rows = self.inserts.get(key)
        if rows is None:
            self.inserts[key] = rows = {}
        rows[rowkey] = row  # note that this may replace a row
        self.rows_added += 1
        self.size_added += size
        if (self.rows_added >= self.row_limit
            or self.size_added >= self.size_limit):
            self.flush()

    def flush(self):
        if self.deletes:
            self._do_deletes()
            self.deletes.clear()
        if self.inserts:
            self._do_inserts()
            self.inserts.clear()
        self.rows_added = 0
        self.size_added = 0

    def _do_deletes(self):
        for (table, varname), values in sorted(self.deletes.items()):
            v = list(values)
            v.sort()
            value_str = ','.join(v)
            stmt = "DELETE FROM %s WHERE %s IN (%s)" % (
                table, varname, value_str)
            self.cursor.execute(stmt)

    def _do_inserts(self):
        items = sorted(self.inserts.items())
        for (command, header, row_schema), rows in items:
            if self.support_batch_insert:
                parts = []
                params = []
                s = "(%s)" % row_schema
                for row in rows.values():
                    parts.append(s)
                    params.extend(row)
                parts = ',\n'.join(parts)
                stmt = "%s INTO %s VALUES\n%s" % (command, header, parts)
                self.cursor.execute(stmt, tuple(params))
            else:
                for row in rows.values():
                    stmt = "%s INTO %s VALUES (%s)" % (
                        command, header, row_schema)
                    self.cursor.execute(stmt, tuple(row))


class PostgreSQLRowBatcher(RowBatcher):

    def __init__(self, cursor, version_detector):
        super(PostgreSQLRowBatcher, self).__init__(cursor)
        self.support_batch_insert = (
            version_detector.get_version(cursor) >= (8, 2))


class MySQLRowBatcher(RowBatcher):
    pass


oracle_rowvar_re = re.compile(":([a-zA-Z0-9_]+)")

class OracleRowBatcher(RowBatcher):
    """Oracle-specific row batcher.

    Expects :name parameters and a dictionary for each row.
    """

    def __init__(self, cursor, inputsizes):
        super(OracleRowBatcher, self).__init__(cursor)
        self.inputsizes = inputsizes
        self.array_ops = {}  # {(operation, row_schema): {rowkey: [row]}}

    def _do_inserts(self):

        def replace_var(match):
            name = match.group(1)
            new_name = '%s_%d' % (name, rownum)
            if name in self.inputsizes:
                stmt_inputsizes[new_name] = self.inputsizes[name]
            params[new_name] = row[name]
            return ':%s' % new_name

        items = sorted(self.inserts.items())
        for (command, header, row_schema), rows in items:
            stmt_inputsizes = {}

            if len(rows) == 1:
                # use the single insert syntax
                row = rows.values()[0]
                stmt = "INSERT INTO %s VALUES (%s)" % (header, row_schema)
                for name in self.inputsizes:
                    if name in row:
                        stmt_inputsizes[name] = self.inputsizes[name]
                if stmt_inputsizes:
                    self.cursor.setinputsizes(**stmt_inputsizes)
                self.cursor.execute(stmt, row)

            else:
                # use the multi-insert syntax
                parts = []
                params = {}
                for rownum, row in enumerate(rows.values()):
                    mod_row = oracle_rowvar_re.sub(replace_var, row_schema)
                    parts.append("INTO %s VALUES (%s)" % (header, mod_row))

                parts = '\n'.join(parts)
                stmt = "INSERT ALL\n%s\nSELECT * FROM DUAL" % parts
                if stmt_inputsizes:
                    self.cursor.setinputsizes(**stmt_inputsizes)
                self.cursor.execute(stmt, params)

    def add_array_op(self, operation, row_schema, row, rowkey, size):
        key = (operation, row_schema)
        rows = self.array_ops.get(key)
        if rows is None:
            self.array_ops[key] = rows = {}
        rows[rowkey] = row  # note that this may replace a row
        self.rows_added += 1
        self.size_added += size
        if (self.rows_added >= self.row_limit
            or self.size_added >= self.size_limit):
            self.flush()

    def flush(self):
        if self.deletes:
            self._do_deletes()
            self.deletes.clear()
        if self.inserts:
            self._do_inserts()
            self.inserts.clear()
        if self.array_ops:
            self._do_array_ops()
            self.array_ops.clear()
        self.rows_added = 0
        self.size_added = 0

    def _do_array_ops(self):
        items = sorted(self.array_ops.items())
        for (operation, row_schema), rows in items:
            r = rows.values()
            params = []
            datatypes = [self.inputsizes[name] for name in row_schema.split()]
            for i, column in enumerate(zip(*r)):
                params.append(self.cursor.arrayvar(datatypes[i], list(column)))
            self.cursor.execute(operation, tuple(params))
