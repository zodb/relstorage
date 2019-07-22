# -*- coding: utf-8 -*-
"""
The ``INSERT`` statement.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import implementer

from .query import Query
from ._util import copy
from .query import ColumnList
from .ast import resolved_against

from .interfaces import ITypedParams
from .interfaces import IOrderedBindParam

@implementer(ITypedParams)
class Insert(Query):

    column_list = None
    select = None
    epilogue = ''
    values = None

    def __init__(self, table, *columns):
        super(Insert, self).__init__()
        self.table = table
        if columns:
            self.column_list = ColumnList(resolved_against(columns, table))
            # TODO: Probably want a different type, like a ValuesList
            self.values = ColumnList([self.orderedbindparam() for _ in columns])

    def from_select(self, names, select):
        i = copy(self)
        i.column_list = ColumnList(names)
        i.select = select
        return i

    def __compile_visit__(self, compiler):
        compiler.emit_keyword('INSERT INTO')
        compiler.visit(self.table)
        compiler.visit_grouped(self.column_list)
        if self.select:
            compiler.visit(self.select)
        else:
            compiler.emit_keyword('VALUES')
            compiler.visit_grouped(self.values)
        compiler.emit(self.epilogue)

    def __add__(self, extension):
        # This appends a textual epilogue. It's a temporary
        # measure until we have more nodes and can model what
        # we're trying to accomplish.
        assert isinstance(extension, str)
        i = copy(self)
        i.epilogue += extension
        return i

    def datatypes_for_parameters(self):
        dialect = self.dialect
        if self.values and self.column_list:
            # If we're sending in a list of values, those have to
            # exactly match the columns, so we can easily get a list
            # of datatypes.
            column_list = self.column_list
            datatypes = dialect.datatypes_for_columns(column_list)
        elif self.select and self.select.column_list.has_bind_param():
            targets = self.column_list
            sources = self.select.column_list
            # TODO: This doesn't support bind params anywhere except the
            # select list!
            # TODO: This doesn't support named bind params.
            columns_with_params = [
                target
                for target, source in zip(targets, sources)
                if IOrderedBindParam.providedBy(source)
            ]
            datatypes = dialect.datatypes_for_columns(columns_with_params)
        return datatypes



class Insertable(object):

    def insert(self, *columns):
        return Insert(self, *columns)
