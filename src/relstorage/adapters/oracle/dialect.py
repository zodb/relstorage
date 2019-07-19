# -*- coding: utf-8 -*-
"""
The Oracle dialect

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ..sql import DefaultDialect
from ..sql import Compiler
from ..sql import Boolean
from ..sql import Column

class OracleCompiler(Compiler):

    def visit_boolean_literal_expression(self, value):
        sql = "'Y'" if value else "'N'"
        self.emit(sql)

    def can_prepare(self):
        # We haven't investigated preparing statements manually
        # with cx_Oracle. There's a chance that `cx_Oracle.Connection.stmtcachesize`
        # will accomplish all we need.
        return False

    def _placeholder(self, key):
        # XXX: What's that block in the parent about key == '%s'?
        # Only during prepare() I think.
        return ':' + key

    def visit_ordered_bind_param(self, bind_param):
        ph = self.placeholders[bind_param] = ':%d' % (len(self.placeholders) + 1,)
        self.emit(ph)

    def visit_select_expression(self, column_node):
        if isinstance(column_node, Column) and column_node.is_type(Boolean):
            # Fancy CASE statement to get 1 or 0 into Python
            self.emit_keyword('CASE WHEN')
            self.emit_identifier(column_node.name)
            self.emit(" = 'Y' THEN 1 ELSE 0 END")
        else:
            super(OracleCompiler, self).visit_select_expression(column_node)


class OracleDialect(DefaultDialect):

    def compiler_class(self):
        return OracleCompiler
