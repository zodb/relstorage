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
from relstorage._compat import perf_counter
from relstorage._util import parse_byte_size

from .interfaces import AggregateOperationTimeoutError

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

    perf_counter = perf_counter

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
        """
        Return the number of rows updated.
        """
        if self.rows_added >= self.row_limit:
            return self.flush()
        if self.bind_limit and self.bind_params_added >= self.bind_limit:
            return self.flush()
        if self.size_added >= self.size_limit:
            return self.flush()
        return 0

    def _flush_if_would_exceed_bind(self, addition):
        # The bind limit is a hard limit we cannot exceed.
        # If adding *addition* params would cause us to exceed,
        # flush now.
        if self.bind_limit and self.bind_params_added + addition >= self.bind_limit:
            return self.flush() or True # This should always be at least one, right?
        return 0

    def delete_from(self, table, **kw):
        """
        Returns the number of rows flushed as a result of this operation.
        That can include inserts.
        """
        # XXX: When deleting a lot from a single table, a bulk function
        # might be a lot faster.
        if not kw:
            raise AssertionError("Need at least one column value")
        columns = tuple(sorted(kw))
        key = (table, columns)
        row = tuple(kw[column] for column in columns)
        bind_params_added = len(row) if key not in self.deletes[key] else 0
        count = self._flush_if_would_exceed_bind(bind_params_added)

        self.deletes[key].add(row)
        self.rows_added += 1
        self.bind_params_added += bind_params_added
        count += self._flush_if_needed()
        return count

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

    def select_from(self, columns, table, suffix='', timeout=None, **kw):
        """
        Handles a query of the ``WHERE col IN (?, ?,)`` type::

        ``SELECT columns FROM table WHERE col IN (?, ...)``

        The keyword arguments should be of length 1, containing an
        iterable of the values to check: ``col=(1, 2)`` or in the
        dynamic case ``**{indirect_var: [1, 2]}``::

            batcher.select_from(('zoid', 'tid',), 'object_state',
                                zoid=oids)

        The number of batches needed is determined by the length of
        the iterator divided by this object's ``bind_limit``,
        or, if that's not set, by the ``row_limit``.

        Returns a iterator of matching rows. Matching rows are delivered
        incrementally, so some number of rows may be delivered and then
        an exception is raised.

        :keyword float timeout: If given, provides a number of seconds
           that is the approximate maximum amount of time this method will
           be allowed to take.
        :raises AggregateOperationTimeoutError: If *timeout* is given,
           and the cumulative time taken to query and process
           some subset of batches exceeds *timeout*. This is checked
           after each individual batch.
        """
        command = 'SELECT %s' % (','.join(columns),)

        for cursor, _count in self.__select_like(
                command,
                table,
                suffix,
                timeout,
                kw
        ):
            for row in cursor.fetchall():
                yield row

    def update_set_static(self, update_set, timeout=None,
                          batch_done_callback=lambda total_count: None,
                          **kw):
        """
        As for :meth:`select_from`, but the first parameter is
        the complete static UPDATE statement. It must be uppercase,
        and startwith "UPDATE".

        Rows are net expected to be returned, so the cursor is completely consumed between
        batches.
        """
        # We actually just consume the iteration of the cursor itself;
        # we can't consume the cursor. Some drivers (pg8000) throw ProgrammingError
        # if we try to iterate the cursor that has no rows.
        total_count = 0
        for _cursor, total_count in self.__select_like(
                update_set,
                None, # table not used
                '', # suffix not used,
                timeout,
                kw
        ):
            batch_done_callback(total_count)

        return total_count

    def __select_like(self, command, table, suffix, timeout, kw):
        assert len(kw) == 1, kw
        # filter_values may be a generic iterable or even a generator;
        # be sure not to materialize the whole thing at any point. Never
        # more than a chunk_size at a time.
        filter_column, filter_values = kw.popitem()
        filter_values = iter(filter_values)

        chunk_size = self.bind_limit or self.row_limit
        chunk_size -= 1

        begin = self.perf_counter() if timeout else None

        count = 0
        for head in filter_values:
            filter_subset = list(itertools.islice(filter_values, chunk_size))
            filter_subset.append(head)

            descriptor = [[(table, (filter_column,)), filter_subset]]

            count += self._do_batch(command, descriptor, rows_need_flattened=False, suffix=suffix)

            yield self.cursor, count

            if timeout and self.perf_counter() - begin >= timeout:
                # TODO: It'd be nice not to do this if we had no more
                # batches to do.
                raise AggregateOperationTimeoutError


    def flush(self):
        """
        Return the tetal number of rows deleted or inserted in this operation.
        (This is the number requested, in the case of deletes, not the number
        that actually matched.)

        This can be treated as a boolean to discover if anything was flushed.
        """
        count = 0
        if self.deletes:
            count += self._do_deletes()
            self.total_rows_deleted += count
            self.deletes.clear()
        if self.inserts:
            count += self._do_inserts()
            self.total_rows_inserted += count
            self.inserts.clear()
        self.total_size_inserted += self.size_added

        self.rows_added = 0
        self.size_added = 0
        self.bind_params_added = 0
        return count

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
        if not command.startswith('UPDATE'):
            stmt = "%s FROM %s WHERE %s IN (%s)" % (
                command, table, filter_column, placeholder_str
            )
        else:
            stmt = "%s WHERE %s IN (%s)" % (
                command, filter_column, placeholder_str
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
                command, header, ', '.join(values_template), suffix)
            # e.g.,
            # INSERT INTO table(c1, c2)
            # VALUES (%s, %s), (%s, %s), (%s, %s)
            # <suffix>
            __traceback_info__ = stmt
            self.cursor.execute(stmt, params)
        return count
