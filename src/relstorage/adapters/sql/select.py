# -*- coding: utf-8 -*-
"""
Elements of select queries.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .query import Query
from .query import ColumnList
from .query import where as _where
from .query import OrderBy
from ._util import copy

from .ast import TextNode
from .ast import resolved_against

from .expressions import EmptyExpression

class _SelectColumns(ColumnList):

    def __compile_visit__(self, compiler):
        compiler.visit_select_list_csv(self._columns)

    def as_select_list(self):
        return self


class Select(Query):
    """
    A Select query.

    When instances of this class are stored in a class dictionary,
    they function as non-data descriptors: The first time they are
    accessed, they *bind* themselves to the instance and select the
    appropriate SQL syntax and compile themselves into a string.
    """

    _distinct = EmptyExpression()
    _where = EmptyExpression()
    _order_by = EmptyExpression()
    _limit = None
    _for_update = None
    _nowait = None

    def __init__(self, table, *columns):
        self.table = table
        if columns:
            self.column_list = _SelectColumns(resolved_against(columns, table))
        else:
            self.column_list = table

    def where(self, expression):
        expression = expression.resolve_against(self.table)
        s = copy(self)
        s._where = _where(expression)
        return s

    def and_(self, expression):
        expression = expression.resolve_against(self.table)
        s = copy(self)
        s._where = self._where.and_(expression)
        return s

    def order_by(self, expression, dir=None):
        expression = expression.resolve_against(self.table)
        s = copy(self)
        s._order_by = OrderBy(expression, dir)
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

    def __compile_visit__(self, compiler):
        compiler.emit_keyword('SELECT')
        compiler.visit(self._distinct)
        compiler.visit_select_list(self.column_list)
        compiler.visit_from(self.table)
        compiler.visit_clause(self._where)
        compiler.visit_clause(self._order_by)
        if self._limit:
            compiler.emit_keyword('LIMIT')
            compiler.emit(str(self._limit))
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
