# -*- coding: utf-8 -*-
"""
Concepts that exist in SQL, such as columns and tables.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from relstorage._util import Lazy

from ._util import Columns
from ._util import copy

from .types import Type
from .types import Unknown

from .expressions import ExpressionOperatorMixin
from .expressions import ParamMixin

from .dialect import DialectAware

from .select import Selectable
from .insert import Insertable
from .insert import Deletable
from .insert import Updatable
from .insert import Upsertable
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
        self.alias = name

    def is_type(self, type_):
        return isinstance(self.type_, type_)

    def resolve_against(self, table):
        if table.c.is_ambiguous(self):
            return _QualifiedColumn(self)
        return self

    def aliased(self, alias):
        return _AliasedColumn(self, alias)

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

class ColumnExpression(Column):

    def resolve_against(self, table):
        return self

    def __str__(self):
        return str(self.name)

    def aliased(self, alias):
        return _AliasedColumnExpression(self, alias)

class _AliasedColumn(Column):
    def __init__(self, other, alias):
        Column.__init__(self, other.name)
        vars(self).update(vars(other))
        self.alias = alias

    def __compile_visit__(self, compiler):
        compiler.visit_aliased_column(self)

class _AliasedColumnExpression(_AliasedColumn):

    def __str__(self):
        return str(self.name)

class _QualifiedColumn(DialectAware,
                       Column):

    def __init__(self, other):
        Column.__init__(self, other.name)
        vars(self).update(vars(other))
        assert self.table

    def __str__(self):
        return "%s.%s" % (self.table, self.name)

    def __compile_visit__(self, compiler):
        compiler.visit_qualified_column(self)

class _DeferredColumn(Column):

    def resolve_against(self, table):
        col = getattr(table.c, self.name)
        if col is not self:
            # We may not be able to resolve fully until the table
            # is bound.
            col = col.resolve_against(table)
        return col

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

    def __init__(self, table, **kw):
        self.table = table
        self.kw = kw

    def __compile_visit__(self, compiler):
        compiler.create_table(self.table, **self.kw)


class Table(Selectable,
            Insertable,
            Deletable,
            Updatable,
            Upsertable,
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

    def _emit_identifier_suffix(self, compiler):
        pass

    def __compile_visit_for_create__(self, compiler, if_not_exists=False):
        self._emit_create_prefix(compiler)
        if if_not_exists:
            compiler.emit_if_not_exists()
        compiler.emit_identifier(self.name)
        self._emit_identifier_suffix(compiler)
        self._visit_definition(compiler)
        return self

    def _visit_definition(self, compiler):
        compiler.visit_grouped(
            self.c
        )

    def natural_join(self, other_table):
        return NaturalJoinedTable(self, other_table)

    def inner_join(self, other_table):
        return _InnerJoinBuilder(self, other_table)

    def resolve_against(self, _table):
        return self

    def create(self, if_not_exists=False):
        """
        WARNING: This currently doesn't support table constraints
        or foreign keys.
        """
        return _CreateTable(self, if_not_exists=if_not_exists)


class TemporaryTable(Table):
    """
    A temporary table.
    """
    def _emit_create_prefix(self, compiler):
        compiler.emit_keyword('CREATE')
        compiler.emit_keyword('TEMPORARY')
        compiler.emit_keyword('TABLE')

class View(DialectAware,
           Table):
    """
    A table defined by a query.
    """

    def __init__(self, name, select):
        # The column names can't be qualified against the original table,
        # they disappear, so we need to rebind them against a new table.
        columns = [copy(c) for c in select.column_list]
        super(View, self).__init__(name, *columns)
        self.select = select

    def _emit_create_prefix(self, compiler):
        compiler.emit_keyword('CREATE')
        compiler.emit_keyword('VIEW')

    def _emit_identifier_suffix(self, compiler):
        compiler.emit_keyword('AS')

    def _visit_definition(self, compiler):
        with compiler.using_visit_name(''):
            compiler.visit(self.select)

    def __str__(self):
        # This is almost how you would inline it in a FROM clause
        return '(%s)' % (self.select,)

    def create(self): # pylint:disable=arguments-differ
        return _CreateTable(self, if_not_exists=False, use_trailer=False)


class _CompositeTableMixin(DialectAware,
                           Selectable,
                           ParamMixin):
    """
    A virtual table whose columns are the common set of columns
    found in two tables.
    """
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    _bind_vars_ignored = ('c', 'columns')

    def _bound_to(self, context, dialect):
        for v in self._bind_vars_ignored:
            self.__dict__.pop(v, None)
        return super(_CompositeTableMixin, self)._bound_to(context, dialect)

    @Lazy
    def c(self):
        return self.lhs.c.intersection(self.rhs.c)

    @Lazy
    def columns(self):
        return tuple(self.c)

    def resolve_against(self, _):
        return self

    def __repr__(self):
        return '<%s at 0x%x lhs=%r rhs=%r>' % (
            type(self).__name__,
            id(self),
            self.lhs,
            self.rhs,
        )

class _JoinedTable(_CompositeTableMixin):

    @Lazy
    def c(self):
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

    def join_kind(self, kind):
        s = copy(self)
        s._JOIN_KW = kind
        return s

    def __repr__(self):
        return '<%s at 0x%x %r %s %r (%r)>' % (
            type(self).__name__,
            id(self),
            self.lhs,
            self._JOIN_KW,
            self.rhs,
            self.c
        )

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


class HistoryVariantTable(_CompositeTableMixin):
    """
    A table that can be one of two tables, depending on whether
    the instance is keeping history or not.
    """

    @Lazy
    def c(self):
        # Lose the owning table of the columns so we can write the correct
        # one when bound
        union = _CompositeTableMixin.c.data[0](self) # pylint:disable=no-member
        return Columns([
            _DeferredColumn(c.name) for c in union])


    def _bound_to(self, context, dialect):
        t = self.history_preserving if context.keep_history else self.history_free
        return t.bind(context, dialect) if hasattr(t, 'bind') else t

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
