# -*- coding: utf-8 -*-
"""
Expressions in the AST.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import implementer

from ._util import Resolvable
from ._util import copy

from .dialect import DialectAware
from .interfaces import INamedBindParam
from .interfaces import IOrderedBindParam

class Expression(DialectAware,
                 Resolvable):
    """
    A SQL expression.
    """

    __slots__ = ()

@implementer(INamedBindParam)
class BindParam(Expression):

    __slots__ = (
        'key',
    )

    def __init__(self, key):
        self.key = key

    def __compile_visit__(self, compiler):
        compiler.visit_bind_param(self)

@implementer(INamedBindParam)
def bindparam(key):
    return BindParam(key)


class LiteralExpression(Expression):

    __slots__ = (
        'value',
    )

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __compile_visit__(self, compiler):
        compiler.visit_literal_expression(self.value)

class BooleanLiteralExpression(LiteralExpression):

    __slots__ = ()

    def __compile_visit__(self, compiler):
        compiler.visit_boolean_literal_expression(self.value)

@implementer(IOrderedBindParam)
class OrderedBindParam(Expression):

    __slots__ = ()

    name = '%s'

    def __compile_visit__(self, compiler):
        compiler.visit_ordered_bind_param(self)


@implementer(IOrderedBindParam)
def orderedbindparam():
    return OrderedBindParam()


def as_expression(stmt):
    if hasattr(stmt, '__compile_visit__'):
        return stmt

    if isinstance(stmt, bool):
        # The values True and False are handled
        # specially because their representation varies
        # among databases (Oracle)
        stmt = BooleanLiteralExpression(stmt)
    else:
        stmt = LiteralExpression(stmt)
    return stmt

class BinaryExpression(Expression):
    """
    Expresses a comparison.
    """

    __slots__ = (
        'op',
        'lhs',
        'rhs',
    )

    def __init__(self, op, lhs, rhs):
        self.op = op
        self.lhs = lhs # type: Column
        # rhs is either a literal or a column;
        # certain literals are handled specially.
        rhs = as_expression(rhs)
        self.rhs = rhs

    def __str__(self):
        return '%s %s %s' % (
            self.lhs,
            self.op,
            self.rhs
        )

    def _visit_lhs(self, compiler):
        compiler.visit(self.lhs)

    def _visit_rhs(self, compiler):
        compiler.visit(self.rhs)

    def __compile_visit__(self, compiler):
        self._visit_lhs(compiler)
        compiler.visit_op(self.op)
        self._visit_rhs(compiler)

    def resolve_against(self, table):
        lhs = self.lhs.resolve_against(table)
        rhs = self.rhs.resolve_against(table)
        new = copy(self)
        new.rhs = rhs
        new.lhs = lhs
        return new

class EmptyExpression(Expression):
    """
    No expression at all.
    """

    __slots__ = ()

    def __bool__(self):
        return False

    __nonzero__ = __bool__

    def __str__(self):
        return ''

    def and_(self, expression):
        return expression

    def __compile_visit__(self, compiler):
        "Does nothing"

class EqualExpression(BinaryExpression):

    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, '=', lhs, rhs)

AssignmentExpression = EqualExpression

class NotEqualExpression(BinaryExpression):

    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, '<>', lhs, rhs)


class GreaterExpression(BinaryExpression):

    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, '>', lhs, rhs)

class GreaterEqualExpression(BinaryExpression):

    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, '>=', lhs, rhs)

class LessEqualExpression(BinaryExpression):

    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, '<=', lhs, rhs)

class LessExpression(BinaryExpression):

    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, '<', lhs, rhs)

class InExpression(BinaryExpression):
    __slots__ = ()

    def __init__(self, lhs, rhs):
        BinaryExpression.__init__(self, 'IN', lhs, rhs)

    def _visit_rhs(self, compiler):
        compiler.visit_grouped(self.rhs)


class And(Expression):

    __slots__ = (
        'lhs',
        'rhs',
    )

    def __init__(self, lhs, rhs):
        self.lhs = as_expression(lhs)
        self.rhs = as_expression(rhs)

    def __compile_visit__(self, compiler):
        compiler.visit_grouped(BinaryExpression('AND', self.lhs, self.rhs))

    def resolve_against(self, table):
        lhs = self.lhs.resolve_against(table)
        rhs = self.rhs.resolve_against(table)
        new = copy(self)
        self.lhs = lhs
        self.rhs = rhs
        return new


class ParamMixin(object):
    def bindparam(self, key):
        return bindparam(key)

    def orderedbindparam(self):
        return orderedbindparam()

class ExpressionOperatorMixin(object):
    def __eq__(self, other):
        return EqualExpression(self, other)

    def __gt__(self, other):
        return GreaterExpression(self, other)

    def __ge__(self, other):
        return GreaterEqualExpression(self, other)

    def __ne__(self, other):
        return NotEqualExpression(self, other)

    def __le__(self, other):
        return LessEqualExpression(self, other)

    def __lt__(self, other):
        return LessExpression(self, other)

    def in_(self, other):
        return InExpression(self, other)
