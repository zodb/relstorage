# -*- coding: utf-8 -*-
"""
Elements of select queries.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .query import Query
from .query import ColumnList
from .query import OrderBy
from .query import WhereMixin
from ._util import copy
from ._util import Resolvable

from .ast import TextNode
from .ast import resolved_against
from .expressions import EmptyExpression


class _SelectColumns(ColumnList):

    def __compile_visit__(self, compiler):
        compiler.visit_select_list_csv(self._columns)

    def as_select_list(self):
        return self


class Select(Query,
             Resolvable,
             WhereMixin):
    """
    A Select query.

    When instances of this class are stored in a class dictionary,
    they function as non-data descriptors: The first time they are
    accessed, they *bind* themselves to the instance and select the
    appropriate SQL syntax and compile themselves into a string.
    """

    _distinct = EmptyExpression()

    _order_by = EmptyExpression()
    _limit = None
    _for_update = None
    _nowait = None

    def __init__(self, table, *columns):
        self.table = table
        self.columns = columns
        if columns:
            self.column_list = _SelectColumns(resolved_against(columns, table))
        else:
            self.column_list = table
        self.c = self.column_list

    _bind_vars_ignored = ('column_list',)

    def _bound_to(self, context, dialect):
        super(Select, self)._bound_to(context, dialect)
        if self.columns:
            # Need to re-resolve, it's possible our table changed.
            self.column_list = _SelectColumns(
                resolved_against(self.columns, self.table)
            )
        return self

    def order_by(self, expression, dir=None):
        expression = expression.resolve_against(self.table)
        s = copy(self)
        s._order_by = OrderBy(expression, dir)
        return s

    def unordered(self):
        if not self._order_by:
            return self
        s = copy(self)
        del s._order_by
        return s

    def limit(self, literal):
        s = copy(self)
        s._limit = literal
        return s

    def for_update(self):
        s = copy(self)
        s._for_update = 'FOR UPDATE'
        return s

    def nowait(self):
        s = copy(self)
        s._nowait = 'NOWAIT'
        return s

    def distinct(self):
        s = copy(self)
        s._distinct = TextNode('DISTINCT')
        return s

    def join_kind(self, kind):
        s = copy(self)
        s.table = s.table.join_kind(kind)
        return s

    def is_unconstrained(self):
        "Does this query have nothing after the basic table?"
        return not self._order_by and not self._where

    def __compile_visit__(self, compiler):
        with compiler.visit_limited_select(self, self._limit):
            compiler.emit_keyword('SELECT')
            compiler.visit(self._distinct)
            compiler.visit_select_list(self.column_list)
            compiler.visit_from(self.table)
            compiler.visit_clause(self._where)
            compiler.visit_clause(self._order_by)
            compiler.visit_limit(self._limit)
            if self._for_update:
                compiler.emit_keyword(self._for_update)
            if self._nowait:
                compiler.emit_keyword(self._nowait)


class Selectable(object):
    """
    Mixin for something that can form the root of a selet query.
    """

    def select(self, *args, **kwargs):
        return Select(self, *args, **kwargs)
