# -*- coding: utf-8 -*-
"""
The ``INSERT`` statement.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from zope.interface import implementer

from .query import Query
from .query import Clause
from ._util import copy
from .query import ColumnList
from .ast import resolved_against

from .interfaces import ITypedParams
from .interfaces import IOrderedBindParam
from .query import WhereMixin
from .expressions import AssignmentExpression
from .expressions import EmptyExpression
from .expressions import Expression
from .expressions import ParamMixin
from .dialect import DialectAware
from .dialect import NoDialectFoundError

# pylint objects to __compile_visit.*__
# pylint:disable=bad-dunder-name

logger = logging.getLogger(__name__)

class _ValuesPlaceholderList(ColumnList):
    pass

class _InsertValuesClause(ParamMixin, Clause):

    def __init__(self, column_list):
        self.column_list = column_list

    def __compile_visit__(self, compiler):
        compiler.emit_keyword('VALUES')
        values = ()
        if self.column_list:
            values = _ValuesPlaceholderList([self.orderedbindparam()
                                             for _ in self.column_list])
            compiler.visit_grouped(values)
        else:
            compiler.visit_no_values()

    def __compile_visit_for_upsert__(self, compiler):
        self.__compile_visit__(compiler)
        compiler.visit_upsert_values(self)


@implementer(ITypedParams)
class Insert(Query):

    column_list = None
    select = None
    epilogue = ''
#    values = None

    def __init__(self, table, *columns):
        super().__init__()
        self.table = table

        if columns:
            self.column_list = ColumnList(resolved_against(columns, table))

    def from_select(self, names, select):
        i = copy(self)
        i.column_list = ColumnList(resolved_against(names, select))
        i.select = select
        return i

    def _visit_command(self, compiler):
        compiler.emit_keyword_insert_into()

    def _visit_select(self, compiler):
        compiler.visit(self.select)

    def __compile_visit__(self, compiler):
        self._visit_command(compiler)
        compiler.visit(self.table)
        if self.column_list:
            compiler.visit_grouped(self.column_list)
        if self.select:
            self._visit_select(compiler)
        else:
            values = _InsertValuesClause(self.column_list)
            try:
                values = values.bind(self.context)
            except NoDialectFoundError:
                # Should only happen in testing.
                logger.debug('Unable to find dialect', exc_info=True)
            compiler.visit(values)
        compiler.emit(self.epilogue)

    def datatypes_for_parameters(self):
        dialect = self.dialect
        if self.column_list and not self.select:
            # If we're sending in a list of values, those have to
            # exactly match the columns, so we can easily get a list
            # of datatypes.
            column_list = self.column_list
            return dialect.datatypes_for_columns(column_list)

        if self.select and self.select.column_list.has_bind_param():
            targets = self.column_list
            sources = self.select.column_list
            # TODO: This doesn't support bind params anywhere except the
            # select list!
            # TODO: This doesn't support named bind params.
            columns_with_params = [
                target
                for target, source in zip(targets, sources)
                if IOrderedBindParam.providedBy(source) # pylint:disable=no-value-for-parameter
            ]
            return dialect.datatypes_for_columns(columns_with_params)
        return None


class _ExcludedColumn(Expression):

    def __init__(self, name):
        self.name = name

    def __compile_visit__(self, compiler): # pragma: no cover
        raise AssertionError("Should only be used in upsert")

    def __compile_visit_for_upsert__(self, compiler):
        compiler.visit_upsert_excluded_column(self)


class _UpsertAssignmentExpression(AssignmentExpression):
    __slots__ = ()

    def _get_vars_to_consider_binding(self):
        # Our super class doesn't do any binding of its
        # left or right hand sides, but we need to bind our
        # RHS so it has a context and can access version information.
        return (('rhs', self.rhs,),)


class _BindableList(DialectAware, list):
    def bind(self, context, dialect=None):
        return type(self)(
            i.bind(context, dialect)
            for i
            in self
        )


class Upsert(Insert):
    """
    Perform an insert-or-update operation.

    All supported databases have some version of this,
    but not all of them have the same expressive power. This
    interface is limited to the lowest common denominator.

    You must call ``on_conflict()`` and ``do_update()``.
    The on_conflict parameter must be a single column of the
    primary key, though this isn't verified. ``do_update``
    should be called to specify a subset of the columns in the
    insert list to be updated; they will have the same values as the insert
    list. Note that some databases may update all columns.
    """

    conflict_column = None
    update_columns = None

    def on_conflict(self, exp):
        i = copy(self)
        i.conflict_column = exp
        return i

    def do_update(self, *columns):
        update_columns = ColumnList(resolved_against(columns, self.table))
        assert update_columns.is_subset(self.column_list)
        i = copy(self)
        i.update_columns = update_columns
        return i

    @property
    def update_clause(self):
        update = Update(EmptyExpression(), _BindableList(
            _UpsertAssignmentExpression(col, _ExcludedColumn(col.name))
            for col
            in self.update_columns # pylint:disable=not-an-iterable
        ))
        try:
            return update.bind(self.context)
        except NoDialectFoundError:
            # Should only happen in testing
            return update

    def _visit_command(self, compiler):
        compiler.emit_keyword_upsert()

    def _visit_select(self, compiler):
        compiler.visit_upsert_before_select(self.select)
        super()._visit_select(compiler)
        compiler.visit_upsert_after_select(self.select)

    def __compile_visit__(self, compiler):
        assert self.conflict_column, "Didn't call on_conflict"
        assert self.update_columns, "Didn't call do_update"
        with compiler.visiting_upsert(self) as upsert_compiler:
            upsert_compiler.visit_upsert(self)

    def __compile_visit_for_upsert__(self, compiler):
        super().__compile_visit__(compiler)
        compiler.visit_upsert_conflict_column(self.conflict_column)
        compiler.visit_upsert_conflict_update(self.update_clause)

class Insertable(object):

    def insert(self, *columns):
        return Insert(self, *columns)


class Delete(Query, WhereMixin):

    _limit = None

    def __init__(self, table):
        super().__init__()
        self.table = table
        self._where = None

    def limit(self, literal):
        s = copy(self)
        # pylint:disable=protected-access
        s._limit = literal
        return s

    def __compile_visit__(self, compiler):
        compiler.emit_keyword('DELETE FROM')
        compiler.visit(self.table)
        if self._where:
            compiler.visit(self._where)
        compiler.visit_limit(self._limit)

class Truncate(Query):

    def __init__(self, table):
        super().__init__()
        self.table = table

    def __compile_visit__(self, compiler):
        compiler.emit_keyword_truncate_table()
        compiler.visit(self.table)


class Deletable(object):
    def delete(self):
        return Delete(self)

    def truncate(self):
        return Truncate(self)

class Updatable(object):
    c = None

    def update(self, **kwargs):
        """
        Update the table. The kwargs must name columns that are members of this table.
        """
        col_expressions = []
        for k, v in kwargs.items():
            col_expressions.append(AssignmentExpression(
                getattr(self.c, k),
                v
            ))

        return Update(self, col_expressions)

class Update(Query, WhereMixin):

    def __init__(self, table, col_expressions):
        self.table = table
        self.col_expressions = col_expressions

    def __compile_visit__(self, compiler):
        compiler.emit_keyword('UPDATE')
        compiler.visit(self.table)
        compiler.emit_keyword('SET')
        compiler.visit_csv(self.col_expressions)
        compiler.visit(self._where)


class Upsertable(object):

    def upsert(self, *columns):
        return Upsert(self, *columns)
