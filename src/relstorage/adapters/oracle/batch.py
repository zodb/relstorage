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
"""
Batch table row insert/delete support.
"""

from __future__ import absolute_import

import re

from relstorage._compat import list_values

from ..batch import RowBatcher

oracle_rowvar_re = re.compile(r":([a-zA-Z0-9_]+)")

class OracleRowBatcher(RowBatcher):
    """
    Oracle-specific row batcher.

    Expects :name parameters and a dictionary for each row.
    """

    insert_placeholder = delete_placeholder = ':1'

    # ORA-01795: maximum number of expressions in a list is 1000
    row_limit = 999

    def __init__(self, cursor, inputsizes, row_limit=None):
        super(OracleRowBatcher, self).__init__(cursor, row_limit)
        self.inputsizes = inputsizes
        self.array_ops = {}  # {(operation, row_schema): {rowkey: [row]}}

    def _do_inserts(self):
        # pylint:disable=too-many-locals
        def replace_var(match):
            name = match.group(1)
            new_name = '%s_%d' % (name, rownum) # pylint:disable=undefined-loop-variable
            if name in self.inputsizes:
                stmt_inputsizes[new_name] = self.inputsizes[name]
            params[new_name] = row[name]
            return ':%s' % new_name

        items = sorted(self.inserts.items())
        for (_command, header, row_schema, _), rows in items:
            stmt_inputsizes = {}

            if len(rows) == 1:
                # use the single insert syntax
                row = list_values(rows)[0]
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


                stmt = "INSERT ALL\n%s\nSELECT * FROM DUAL" % '\n'.join(parts)
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
            r = list_values(rows)
            params = []
            datatypes = [self.inputsizes[name] for name in row_schema.split()]
            for i, column in enumerate(zip(*r)):
                params.append(self.cursor.arrayvar(datatypes[i], list(column)))
            self.cursor.execute(operation, tuple(params))

    # TODO: Can we override _make_single_column_query() to pass
    # an array to the database for use in a sql statement?
    # We'd need the TABLE operator, probably. How far back is that supported?

    def _make_placeholder_list_of_length(self, count):
        return ','.join((':%d' % i for i in range(count)))
