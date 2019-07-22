# -*- coding: utf-8 -*-
"""
Utility functions and base classes.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from copy import copy as stdlib_copy

from .interfaces import IBindParam

def copy(obj):
    new = stdlib_copy(obj)
    volatile = [k for k in vars(new) if k.startswith('_v')]
    for k in volatile:
        delattr(new, k)
    return new

class Resolvable(object):

    __slots__ = ()

    def resolve_against(self, table):
        # pylint:disable=unused-argument
        return self



class Columns(object):
    """
    Grab bag of columns.
    """

    def __init__(self, columns):
        cs = []
        for c in columns:
            setattr(self, c.name, c)
            cs.append(c)
        self._columns = tuple(cs)

    def __bool__(self):
        return bool(self._columns)

    __nonzero__ = __bool__

    def __getattr__(self, name):
        # Here only so that pylint knows this class has a set of
        # dynamic attributes.
        raise AttributeError("Column list %s does not include %s" % (
            self._col_list(),
            name
        ))

    def __getitem__(self, ix):
        return self._columns[ix]

    def _col_list(self):
        return ','.join(str(c) for c in self._columns)

    def __compile_visit__(self, compiler):
        compiler.visit_csv(self._columns)

    def has_bind_param(self):
        return any(
            IBindParam.providedBy(c)
            for c in self._columns
        )

    def as_select_list(self):
        from .select import _SelectColumns
        return _SelectColumns(self._columns)
