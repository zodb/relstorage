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
from collections import defaultdict
import itertools

from relstorage._compat import iteritems


class RowBatcher(object):
    """
    Generic row batcher.

    Inserting can use whatever paramater placeholder format
    and parameter format the cursor supports.

    Deleting needs to use ordered parameters. The placeholder
    can be set in the ``delete_placeholder`` attribute.
    """

    row_limit = 100
    size_limit = 1 << 20
    delete_placeholder = '%s'
    # For testing, force the delete order to be deterministic
    # when multiple columns are involved
    sorted_deletes = False

    def __init__(self, cursor, row_limit=None, delete_placeholder="%s"):
        self.cursor = cursor
        self.delete_placeholder = delete_placeholder
        if row_limit is not None:
            self.row_limit = row_limit

        # These are cumulative
        self.total_rows_inserted = 0
        self.total_size_inserted = 0
        self.total_rows_deleted = 0

        # These all get reset at each flush()
        self.rows_added = 0
        self.size_added = 0

        self.deletes = defaultdict(set)   # {(table, columns_tuple): set([(column_value,)])}
        self.inserts = defaultdict(dict)  # {(command, header, row_schema, suffix): {rowkey: [row]}}

    def delete_from(self, table, **kw):
        if not kw:
            raise AssertionError("Need at least one column value")
        columns = tuple(sorted(kw))
        key = (table, columns)
        rows = self.deletes[key]
        row = tuple(kw[column] for column in columns)
        rows.add(row)
        self.rows_added += 1
        if self.rows_added >= self.row_limit:
            self.flush()

    def insert_into(self, header, row_schema, row, rowkey, size,
                    command='INSERT', suffix=''):
        key = (command, header, row_schema, suffix)
        rows = self.inserts[key]
        rows[rowkey] = row  # note that this may replace a row
        self.rows_added += 1
        self.size_added += size

        if (self.rows_added >= self.row_limit
                or self.size_added >= self.size_limit):
            self.flush()

    def flush(self):
        if self.deletes:
            self.total_rows_deleted += self._do_deletes()
            self.deletes.clear()
        if self.inserts:
            self.total_rows_inserted += self._do_inserts()
            self.inserts.clear()
        self.total_size_inserted += self.size_added
        self.rows_added = 0
        self.size_added = 0

    def _do_deletes(self):
        count = 0
        for (table, columns), rows in sorted(iteritems(self.deletes)):
            count += len(rows)
            # Be careful not to use % formatting on a string already
            # containing `delete_placeholder`
            if len(columns) == 1:
                # TODO: Some databases, like Postgres, natively support
                # array types, which can be much faster.
                # We should do that.
                placeholder_str = ','.join([self.delete_placeholder] * len(rows))
                stmt = "DELETE FROM %s WHERE %s IN (%s)" % (
                    table, columns[0], placeholder_str)
            else:
                row_template = " AND ".join(
                    ("%s=" % (column,)) + self.delete_placeholder
                    for column in columns
                )
                row_template = "(%s)" % (row_template,)
                rows_template = [row_template] * len(rows)
                stmt = "DELETE FROM %s WHERE %s" % (
                    table, " OR ".join(rows_template))

            rows = sorted(rows) if self.sorted_deletes else rows
            rows = list(itertools.chain.from_iterable(rows))
            self.cursor.execute(stmt, rows)
        return count

    def _do_inserts(self):
        count = 0
        # In the case that we have only a single table we're inserting into,
        # and thus common key values, we don't need to sort or iterate.
        # If we have multiple tables, we never want to sort by the rows too,
        # that doesn't make any sense.
        if len(self.inserts) == 1:
            items = [self.inserts.popitem()]
        else:
            items = sorted(iteritems(self.inserts), key=lambda i: i[0])
        for (command, header, row_schema, suffix), rows in items:
            # Batched inserts
            rows = list(rows.values())
            count += len(rows)
            value_template = "(%s)" % row_schema
            values_template = [value_template] * len(rows)
            params = list(itertools.chain.from_iterable(rows))

            stmt = "%s INTO %s VALUES\n%s\n%s" % (
                command, header, ',\n'.join(values_template), suffix)
            # e.g.,
            # INSERT INTO table(c1, c2)
            # VALUES (%s, %s), (%s, %s), (%s, %s)
            # <suffix>
            self.cursor.execute(stmt, params)
        return count
