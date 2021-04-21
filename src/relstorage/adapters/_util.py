##############################################################################
#
# Copyright (c) 2017 Zope Foundation and Contributors.
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
"Internal helper utilities."
from __future__ import print_function
from __future__ import absolute_import

from collections import namedtuple
from functools import partial
from functools import update_wrapper
from functools import wraps

from .._compat import intern
from .._compat import MAX_TID

from .._util import Lazy

def query_property(base_name,
                   extension='',
                   formatted=False,
                   property_suffix='_queries',
                   lazy_suffix='_query'):
    """
    Defines a property that adapts to preserving or dropping history.

    To use, define a property ending in ``_queries``
    *property_suffix*) that is a two-tuple, where the preserving query
    comes first and the dropping query comes second. This indirection
    lets subclasses override these queries.

    Then define a property, passing the base name (without
    ``_queries``) to this function.

    The correct query will be lazily picked at runtime. The instance
    must have the ``keep_history`` attribute.

    If the chosen query is an exception instance or class, it will be
    raised instead of returned. This allows defining a query that is
    only supported in one of the two modes. Usually, this exception
    should be :class:`ZODB.POSException.Unsupported`.

    :keyword str extension: This string will be appended to whatever
        query is chosen before it is formatted and before it is returned.
    :keyword bool formatted: If True (*not* the default), then the
        chosen query will be formatted using the
        ``self.runner.script_vars``. This should produce a new
        query that uses the correct parameter binding for the database.
    """

    def prop(inst):
        queries = getattr(inst, base_name + property_suffix)
        query = queries[0] if inst.keep_history else queries[1]
        if isinstance(query, Exception) or (
                isinstance(query, type)
                and issubclass(query, Exception)):
            raise query

        if extension:
            query = query + extension
        if formatted:
            query = intern(query % inst.runner.script_vars)

        return query

    prop.__doc__ = "Query for " + base_name

    return Lazy(prop, base_name + lazy_suffix)

formatted_query_property = partial(query_property, formatted=True)


def noop_when_history_free(meth):
    """
    Decorator for *meth* that causes it to do nothing when
    ``self.keep_history`` is False.

    *meth* must have no return value (returns None) when it is
    history free. When history is preserved it can return anything.

    This requires a bit more memory to use the instance dict, but at
    runtime it has minimal time overhead (after the first call).
    """

    # Python 3.4 (via timeit)
    # calling a trivial method ('def t(self, arg): return arg') takes 118ns
    # calling a method that does 'if not self.keep_history: return; return arg'
    #   takes 142 ns
    # calling a functools.partial bound to self wrapped around t
    #   takes 298ns
    # calling a generic python function
    #     def wrap(self, *args, **kwargs):
    #       if not self.keep_history: return
    #       return self.t(*args, **kwargs)
    #   takes 429ns
    # So a partial function set into the __dict__ is the fastest way to
    # do this.

    meth_name = meth.__name__

    @wraps(meth)
    def no_op(*_args, **_kwargs):
        return

    @wraps(meth)
    def swizzler(self, *args, **kwargs):
        if not self.keep_history:
            setattr(self, meth_name, no_op)
        else:
            # NOTE: This creates a reference cycle
            bound = partial(meth, self)
            update_wrapper(bound, meth)
            if not hasattr(bound, '__wrapped__'):
                bound.__wrapped__ = meth
            setattr(self, meth_name, bound)

        return getattr(self, meth_name)(*args, **kwargs)

    if not hasattr(swizzler, '__wrapped__'):
        # Py2: this was added in 3.2
        swizzler.__wrapped__ = meth
        no_op.__wrapped__ = meth

    return swizzler


ResultDescription = namedtuple(
    'ResultDescription',
    # First two are mandatory, remaining five may be None
    # Example:
    # ('Name', 253, 17, 192, 192, 0, 0),
    ('name', 'type_code', 'display_size',
     'internal_size', 'precision', 'scale', 'null_ok'))


class DatabaseHelpersMixin(object):

    MAX_TID = MAX_TID

    def _metadata_to_native_str(self, value):
        # Some drivers, in some configurations, notably older versions
        # of MySQLdb (mysqlclient) on Python 3 in 'NAMES binary' mode,
        # can return column names and the like as bytes when we want native str.
        # pg8000 on Python2 does the reverse and returns unicode when we want
        # native str.
        # Sometime between version 8.0.21 and 8.0.24, mysql-connector-python
        # started returning `bytearray`
        if value is not None and not isinstance(value, str):
            # Checking for bytes tells us that it's a unicode object on Python 2;
            # we won't get here if it's bytes (because bytes is str)
            value = (value.decode('ascii')
                     if isinstance(value, (bytes, bytearray))
                     else value.encode('ascii"'))
        return value

    def _column_descriptions(self, cursor):
        __traceback_info__ = cursor.description
        return [ResultDescription(self._metadata_to_native_str(r[0]),
                                  # Not all drivers return lists or tuples
                                  # or things that can be sliced; psycopg2/cffi returns
                                  # an arbitrary sequence.
                                  # MySqlConnector-Python has been observed to provide
                                  # extra attributes.
                                  *list(r)[1:7])
                for r in cursor.description]

    def _rows_as_dicts(self, cursor):
        """
        An iterator of the rows as dictionaries, named by the
        lower-case column name.

        Some drivers offer the ability to do this directly when
        the statement is executed or the cursor is created;
        this is a lowest-common denominator way to do it utilizing
        DB-API 2.0 attributes.
        """
        column_descrs = self._column_descriptions(cursor)
        for row in cursor:
            result = {
                column_descr.name.lower(): column_value
                for column_descr, column_value in zip(column_descrs, row)
            }
            yield result

    def _rows_as_pretty_string(self, cursor):
        """
        Return the rows formatted in a way for easy human consumption.
        """
        from .._compat import NStringIO
        out = NStringIO()
        names = [d.name for d in self._column_descriptions(cursor)]
        kwargs = {'sep': '\t', 'file': out}
        print(*names, **kwargs)
        for row in cursor:
            print(*row, **kwargs)
        return out.getvalue()
