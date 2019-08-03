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

    def __repr__(self):
        return "<%s at %x tins=%d tdel=%d prows=%d>" % (
            self.__class__.__name__,
            id(self),
            self.total_rows_inserted,
            self.total_rows_deleted,
            self.rows_added,
        )

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

    def select_from(self, columns, table, suffix='', **kw):
        """
        Handles a query of the ``WHERE col IN (?, ?,)`` type.

        The keyword arguments should be of length 1, containing
        an iterable of the values to check.

        Returns a iterator of matching rows.
        """
        assert len(kw) == 1
        filter_column, filter_values = kw.popitem()
        filter_values = list(filter_values)
        command = 'SELECT %s' % (','.join(columns),)
        while filter_values:
            filter_subset = filter_values[:self.row_limit]
            del filter_values[:self.row_limit]
            descriptor = [[(table, (filter_column,)), filter_subset]]
            self._do_batch(command, descriptor, rows_need_flattened=False, suffix=suffix)
            for row in self.cursor.fetchall():
                yield row

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
        return self._do_batch('DELETE', sorted(iteritems(self.deletes)))

    def _do_batch(self, command, descriptors, rows_need_flattened=True, suffix=''):
        count = 0
        for (table, columns), rows in descriptors:
            count += len(rows)
            # Be careful not to use % formatting on a string already
            # containing `delete_placeholder`
            these_params_need_flattened = rows_need_flattened
            if len(columns) == 1:
                stmt, params, these_params_need_flattened = self._make_single_column_query(
                    command, table,
                    columns[0], rows, rows_need_flattened
                )
            else:
                row_template = " AND ".join(
                    ("%s=" % (column,)) + self.delete_placeholder
                    for column in columns
                )
                row_template = "(%s)" % (row_template,)
                rows_template = [row_template] * len(rows)
                stmt = "%s FROM %s WHERE %s" % (
                    command,
                    table, " OR ".join(rows_template))
                params = rows

            if these_params_need_flattened:
                params = self._flatten_params(params)
            stmt += suffix
            __traceback_info__ = stmt, params
            self.cursor.execute(stmt, params)

        return count

    def _flatten_params(self, params):
        params = sorted(params) if self.sorted_deletes else params
        params = list(itertools.chain.from_iterable(params))
        return params

    def _make_single_column_query(self, command, table,
                                  filter_column, filter_value,
                                  rows_need_flattened):
        placeholder_str = ','.join([self.delete_placeholder] * len(filter_value))
        stmt = "%s FROM %s WHERE %s IN (%s)" % (
            command, table, filter_column, placeholder_str
        )
        return stmt, filter_value, rows_need_flattened

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
