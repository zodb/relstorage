# -*- coding: utf-8 -*-
"""
Function expressions.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .expressions import Expression
from .schema import Column

class _Functions(object):

    def max(self, column):
        return _Function('max', column)

    def count(self, column=Column('*')):
        return _Function('COUNT', column)

class _Function(Expression):

    def __init__(self, name, expression):
        self.name = name
        self.expression = expression

    def __compile_visit__(self, compiler):
        compiler.emit_identifier(self.name)
        compiler.visit_grouped(self.expression)

func = _Functions()
