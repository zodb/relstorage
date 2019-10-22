# -*- coding: utf-8 -*-
"""
The Oracle dialect

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from contextlib import contextmanager

from ..sql import DefaultDialect
from ..sql import Compiler
from ..sql import Boolean
from ..sql import Column
from ..sql import OID
from ..sql import TID

from ..sql._util import Columns
from ..sql.insert import _ExcludedColumn

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

    @contextmanager
    def visit_limited_select(self, select, limit):
        if limit:
            self.visit_limit = self._visit_limit_while_limited
            self.emit('SELECT * FROM (')
            try:
                yield self
            finally:
                del self.visit_limit
            self.emit(') WHERE ROWNUM <=')
            self.visit_immediate_expression(limit)
        else:
            yield self

    def visit_limit(self, limit_literal): # pylint:disable=method-hidden
        if limit_literal is not None:
            raise Exception("Forgot to visit in a visit_limited_select")

    def _visit_limit_while_limited(self, limit):
        assert limit is not None

    def visit_upsert(self, upsert):
        self.emit_keyword('MERGE INTO')
        self.visit(upsert.table)
        self.emit_keyword('USING')
        if upsert.select:
            self.visit_grouped(upsert.select)
            self.emit_keyword('D')
        else:
            raise TypeError
        self.emit_keyword('ON')
        self.emit(
            '(',
            upsert.table.name + '.' + upsert.conflict_column.name,
            ' = '
            'D.' + upsert.conflict_column.name,
            ')'
        )
        self.emit_keyword('WHEN MATCHED THEN')
        self.visit(upsert.update_clause)

        self.emit_keyword('WHEN NOT MATCHED THEN INSERT')
        self.visit_grouped(upsert.column_list)
        self.emit_keyword('VALUES')
        excluded_columns = Columns(_ExcludedColumn(c.name) for c in upsert.column_list)
        self.visit_grouped(excluded_columns)

    def visit_upsert_excluded_column(self, column):
        self.emit('D.' + column.name)

class OracleDialect(DefaultDialect):

    datatype_map = DefaultDialect.datatype_map.copy()
    datatype_map.update({
        OID: 'NUMBER(20)',
        TID: 'NUMBER(20)',
        Boolean: "CHAR",
    })

    def extra_constraints_for_column(self, column):
        if column.is_type(Boolean):
            return "CHECK ({col_name} IN ('N', 'Y'))".format(
                col_name=column.name
            )
        return ''

    def compiler_class(self):
        return OracleCompiler
