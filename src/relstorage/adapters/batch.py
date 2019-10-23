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
from relstorage._util import parse_byte_size

# A cache
# {(placeholder, count): "string"}
_row_schemas = {}

class RowBatcher(object):
    """
    Generic row batcher.

    Inserting can use whatever paramater placeholder format
    and parameter format the cursor supports.

    Deleting needs to use ordered parameters. The placeholder
    can be set in the ``delete_placeholder`` attribute.
    """

    # How many total rows can be sent at once. Also used as
    # ``bind_limit`` if that is 0 or None.
    row_limit = 1024
    # The total number of available bind variables a single statement
    # can use.
    bind_limit = None
    # The default max_allowed_packet in MySQL is 4MB,
    # so the data, including encoding and the rest of the query structure,
    # must be less than that (unless we dynamically query to find out
    # what value we have)
    size_limit = parse_byte_size('2 MB')
    delete_placeholder = '%s'
    insert_placeholder = '%s'
    # For testing, force the delete order to be deterministic
    # when multiple columns are involved
    sorted_deletes = False

    def __init__(self, cursor, row_limit=None,
                 delete_placeholder=None,
                 insert_placeholder=None,
                 bind_limit=None):
        self.cursor = cursor
        if delete_placeholder is not None:
            self.delete_placeholder = delete_placeholder
        if insert_placeholder is not None:
            self.insert_placeholder = insert_placeholder
        if row_limit is not None:
            self.row_limit = row_limit
        if bind_limit is not None:
            self.bind_limit = bind_limit

        # These are cumulative
        self.total_rows_inserted = 0
        self.total_size_inserted = 0
        self.total_rows_deleted = 0

        # These all get reset at each flush()
        self.rows_added = 0
        self.size_added = 0
        self.bind_params_added = 0

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

    def _flush_if_needed(self):
        if self.rows_added >= self.row_limit:
            return self.flush()
        if self.bind_limit and self.bind_params_added >= self.bind_limit:
            return self.flush()
        if self.size_added >= self.size_limit:
            return self.flush()

    def _flush_if_would_exceed_bind(self, addition):
        # The bind limit is a hard limit we cannot exceed.
        # If adding *addition* params would cause us to exceed,
        # flush now.
        if self.bind_limit and self.bind_params_added + addition >= self.bind_limit:
            self.flush()
            return True

    def delete_from(self, table, **kw):
        if not kw:
            raise AssertionError("Need at least one column value")
        columns = tuple(sorted(kw))
        key = (table, columns)
        row = tuple(kw[column] for column in columns)
        bind_params_added = len(row) if key not in self.deletes[key] else 0
        self._flush_if_would_exceed_bind(bind_params_added)

        self.deletes[key].add(row)
        self.rows_added += 1
        self.bind_params_added += bind_params_added
        self._flush_if_needed()

    def insert_into(self, header, row_schema, row, rowkey, size,
                    command='INSERT', suffix=''):
        key = (command, header, row_schema, suffix)

        bind_params_added = len(row) if rowkey not in self.inserts[key] else 0
        self._flush_if_would_exceed_bind(bind_params_added)

        # If we flushed, self.inserts has started all over.
        self.inserts[key][rowkey] = row  # note that this may replace a row

        self.rows_added += 1
        self.bind_params_added += bind_params_added
        self.size_added += size
        self._flush_if_needed()

    def row_schema_of_length(self, param_count):
        # Use as the *row_schema* parameter to insert_into
        key = (self.insert_placeholder, param_count)
        try:
            return _row_schemas[key]
        except KeyError:
            p = [self.insert_placeholder] * param_count
            p = ', '.join(p)
            _row_schemas[key] = p
            return p

    def select_from(self, columns, table, suffix='', **kw):
        """
        Handles a query of the ``WHERE col IN (?, ?,)`` type::

        ``SELECT columns FROM table WHERE col IN (?, ...)``

        The keyword arguments should be of length 1, containing an
        iterable of the values to check: ``col=(1, 2)`` or in the
        dynamic case ``**{indirect_var: [1, 2]}``.

        The number of batches needed is determined by the length of
        the iterator divided by this object's ``bind_limit``,
        or, if that's not set, by the ``row_limit``.

        Returns a iterator of matching rows.
        """
        assert len(kw) == 1
        filter_column, filter_values = kw.popitem()
        filter_values = iter(filter_values)

        command = 'SELECT %s' % (','.join(columns),)

        chunk_size = self.bind_limit or self.row_limit
        chunk_size -= 1

        for head in filter_values:
            filter_subset = list(itertools.islice(filter_values, chunk_size))
            filter_subset.append(head)

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
        self.bind_params_added = 0

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
            self.cursor.execute(stmt, params)

        return count

    def _flatten_params(self, params):
        params = sorted(params) if self.sorted_deletes else params
        params = list(itertools.chain.from_iterable(params))
        return params

    def _make_placeholder_list_of_length(self, count):
        return ','.join([self.delete_placeholder] * count)

    def _make_single_column_query(self, command, table,
                                  filter_column, filter_value,
                                  rows_need_flattened):
        placeholder_str = self._make_placeholder_list_of_length(len(filter_value))
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
            __traceback_info__ = stmt
            self.cursor.execute(stmt, params)
        return count
