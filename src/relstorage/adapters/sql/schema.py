# -*- coding: utf-8 -*-
"""
Concepts that exist in SQL, such as columns and tables.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage._util import Lazy

from ._util import Columns

from .types import Type
from .types import Unknown

from .expressions import ExpressionOperatorMixin
from .expressions import ParamMixin

from .dialect import DialectAware

from .select import Selectable
from .insert import Insertable
from .insert import Deletable
from .insert import Updatable
from .query import Query

_marker = object()

class Column(ExpressionOperatorMixin):
    """
    Defines a column in a table.
    """
    # If present, the table name of the table we were defined in.
    table = None

    def __init__(self, name, type_=Unknown,
                 primary_key=False,
                 nullable=True,
                 auto_increment=False,
                 default=_marker):
        self.name = name
        self.type_ = type_ if isinstance(type_, Type) else type_()
        self.primary_key = primary_key
        self.nullable = False if primary_key else nullable
        self.auto_increment = auto_increment
        self.default = default

    def is_type(self, type_):
        return isinstance(self.type_, type_)

    def resolve_against(self, table):
        if table.c.is_ambiguous(self):
            return _QualifiedColumn(self)
        return self

    def __str__(self):
        return self.name

    def __repr__(self):
        result = '%s(%r,' % (
            type(self).__name__,
            self.name,
        )
        if self.table:
            result += 'table=%r' % (self.table,)
        result += ')'
        return result

    def __compile_visit__(self, compiler):
        compiler.visit_column(self)

    def __compile_visit_for_create__(self, compiler):
        compiler.visit_column(self)
        compiler.emit_keyword(
            compiler.dialect.datatype_for_column(self)
        )
        if self.primary_key:
            compiler.emit_column_constraint('PRIMARY KEY')
        # Order matters for some databases.
        # - In Oracle, the DEFAULT has to come before NOT NULL
        # - In sqlite, the auto_increment has to be before NOT NULL (?)
        if self.default is not _marker:
            compiler.emit_keyword('DEFAULT')
            compiler.visit_immediate_expression(self.default)

        if self.auto_increment:
            compiler.emit_column_autoincrement()
        if not self.nullable:
            compiler.emit_column_constraint('NOT NULL')
        compiler.emit(
            compiler.dialect.extra_constraints_for_column(self)
        )

class _QualifiedColumn(Column):

    def __init__(self, other):
        Column.__init__(self, other.name)
        vars(self).update(vars(other))
        assert self.table

    def __compile_visit__(self, compiler):
        compiler.visit_qualified_column(self)

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


class _CreateTable(Query):

    def __init__(self, table, if_not_exists=False):
        self.table = table
        self.if_not_exists = if_not_exists

    def __compile_visit__(self, compiler):
        compiler.create_table(self.table, self.if_not_exists)


class Table(Selectable,
            Insertable,
            Deletable,
            Updatable,
            ParamMixin,
            SchemaItem):
    """
    A table relation.
    """

    def __init__(self, name, *columns):
        self.name = name
        self.columns = columns
        for c in columns:
            c.table = name
        self.c = Columns(columns)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '%s(%r,%s)' % (
            type(self).__name__,
            self.name,
            ','.join('Column(%r)' % (c.name) for c in self.columns)
        )

    def __compile_visit__(self, compiler):
        compiler.emit_identifier(self.name)
        return self

    def _emit_create_prefix(self, compiler):
        compiler.emit_keyword('CREATE')
        compiler.emit_keyword('TABLE')

    def __compile_visit_for_create__(self, compiler, if_not_exists=False):
        self._emit_create_prefix(compiler)
        if if_not_exists:
            compiler.emit_if_not_exists()
        compiler.emit_identifier(self.name)
        compiler.visit_grouped(
            self.c
        )
        return self

    def natural_join(self, other_table):
        return NaturalJoinedTable(self, other_table)

    def inner_join(self, other_table):
        return _InnerJoinBuilder(self, other_table)

    def resolve_against(self, _):
        return self

    def create(self, if_not_exists=False):
        """
        WARNING: This currently doesn't support table constraints
        or foreign keys.
        """
        return _CreateTable(self, if_not_exists)


class TemporaryTable(Table):
    """
    A temporary table.
    """
    def _emit_create_prefix(self, compiler):
        compiler.emit_keyword('CREATE')
        compiler.emit_keyword('TEMPORARY')
        compiler.emit_keyword('TABLE')


class _CompositeTableMixin(Selectable,
                           ParamMixin):
    """
    A virtual table whose columns are the common set of columns
    found in two tables.
    """
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs
    # Use volatile properties so these get reset when an
    # instance is bound.

    @Lazy
    def _v_c(self):
        return self.lhs.c.intersection(self.rhs.c)

    @Lazy
    def _v_columns(self):
        return tuple(self._v_c)

    @property
    def c(self):
        return self._v_c

    @property
    def columns(self):
        return self._v_columns

    def resolve_against(self, _):
        return self

class _JoinedTable(DialectAware,
                   _CompositeTableMixin):

    @Lazy
    def _v_c(self):
        # Make the union of columns available,
        # being sure that if we're doing a USING join
        # we don't double the list of columns we're USING,
        # because we shouldn't use prefixes for those
        # (Oracle doesn't like it)
        join_columns = self._get_join_columns(
            self.lhs.resolve_against(self),
            self.rhs.resolve_against(self))
        union = self.lhs.c.dup_union(self.rhs.c, combining=join_columns.as_names())
        return union

    _JOIN_KW = 'JOIN'
    _JOIN_KIND_KW = 'USING'

    def _get_join_columns(self, lhs, rhs):
        return lhs.c.intersection(rhs.c)

    def __compile_visit__(self, compiler):
        # In case we're wrapped around HistoryVariantTable,
        # defer selection of join columns until now.
        # TODO: This probably only handles one extra level of joins?
        lhs = self.lhs.resolve_against(self)
        rhs = self.rhs.resolve_against(self)
        if isinstance(rhs, _JoinedTable):
            rhs = compiler.visit(rhs)
            compiler.emit_keyword(self._JOIN_KW)
            lhs = compiler.visit(lhs)
            compiler.emit_keyword(self._JOIN_KIND_KW)
            compiler.visit_grouped(self._get_join_columns(lhs, rhs))
        else:
            lhs = compiler.visit(self.lhs)
            compiler.emit_keyword(self._JOIN_KW)
            rhs = compiler.visit(self.rhs)
            compiler.emit_keyword(self._JOIN_KIND_KW)
            compiler.visit_grouped(self._get_join_columns(lhs, rhs))
        return self

class NaturalJoinedTable(_JoinedTable):
    pass

class _InnerJoinBuilder(object):

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def using(self, *columns):
        return UsingJoinedTable(self.lhs, self.rhs, columns)

class UsingJoinedTable(_JoinedTable):

    def __init__(self, lhs, rhs, columns):
        super(UsingJoinedTable, self).__init__(lhs, rhs)
        self._join_columns = Columns(columns)

    def _get_join_columns(self, lhs, rhs):
        c = self._join_columns
        assert self.lhs.c.intersection(c) == c
        assert self.rhs.c.intersection(c) == c
        return c


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

    @property
    def table_in_context(self):
        keep_history = self.context.keep_history
        node = self.history_preserving if keep_history else self.history_free
        return node

    def resolve_against(self, _):
        return self.table_in_context

    def __compile_visit__(self, compiler):
        return compiler.visit(self.table_in_context)
