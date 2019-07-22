# -*- coding: utf-8 -*-
"""
Concepts that exist in SQL, such as columns and tables.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from ._util import Resolvable
from ._util import Columns

from .types import Type
from .types import Unknown

from .expressions import ExpressionOperatorMixin
from .expressions import ParamMixin

from .dialect import DialectAware

from .select import Selectable
from .insert import Insertable

class Column(ExpressionOperatorMixin,
             Resolvable):
    """
    Defines a column in a table.
    """

    def __init__(self, name, type_=Unknown, primary_key=False, nullable=True):
        self.name = name
        self.type_ = type_ if isinstance(type_, Type) else type_()
        self.primary_key = primary_key
        self.nullable = False if primary_key else nullable

    def is_type(self, type_):
        return isinstance(self.type_, type_)

    def __str__(self):
        return self.name


    def __compile_visit__(self, compiler):
        compiler.visit_column(self)


class _DeferredColumn(Column):

    def resolve_against(self, table):
        return getattr(table.c, self.name)

class _DeferredColumns(object):

    def __getattr__(self, name):
        return _DeferredColumn(name)

class ColumnResolvingProxy(ParamMixin):
    """
    A proxy that select can resolve to tables in the current table.
    """

    c = _DeferredColumns()


class SchemaItem(object):
    """
    A permanent item in a schema (aka the data dictionary).
    """


class Table(Selectable,
            Insertable,
            ParamMixin,
            SchemaItem):
    """
    A table relation.
    """

    def __init__(self, name, *columns):
        self.name = name
        self.columns = columns
        self.c = Columns(columns)

    def __str__(self):
        return self.name

    def __compile_visit__(self, compiler):
        compiler.emit_identifier(self.name)

    def natural_join(self, other_table):
        return NaturalJoinedTable(self, other_table)



class TemporaryTable(Table):
    """
    A temporary table.
    """


class _CompositeTableMixin(Selectable,
                           ParamMixin):

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

        common_columns = []
        for col in lhs.columns:
            if hasattr(rhs.c, col.name):
                common_columns.append(col)
        self.columns = common_columns
        self.c = Columns(common_columns)


class NaturalJoinedTable(DialectAware,
                         _CompositeTableMixin):

    def __init__(self, lhs, rhs):
        super(NaturalJoinedTable, self).__init__(lhs, rhs)
        assert self.c
        self._join_columns = self.c

        # TODO: Check for data type mismatches, etc?
        self.columns = list(self.lhs.columns)
        for col in self.rhs.columns:
            if not hasattr(self.lhs.c, col.name):
                self.columns.append(col)
        self.columns = tuple(self.columns)
        self.c = Columns(self.columns)

    def __compile_visit__(self, compiler):
        compiler.visit(self.lhs)
        compiler.emit_keyword('JOIN')
        compiler.visit(self.rhs)
        # careful with USING clause in a join: Oracle doesn't allow such
        # columns to have a prefix.
        compiler.emit_keyword('USING')
        compiler.visit_grouped(self._join_columns)


class HistoryVariantTable(DialectAware,
                          _CompositeTableMixin):
    """
    A table that can be one of two tables, depending on whether
    the instance is keeping history or not.
    """

    @property
    def history_preserving(self):
        return self.lhs

    @property
    def history_free(self):
        return self.rhs

    def __compile_visit__(self, compiler):
        keep_history = self.context.keep_history
        node = self.history_preserving if keep_history else self.history_free
        return compiler.visit(node)
