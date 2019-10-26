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
    """
    Make a shallow copy of the object, ignoring any volatile attributes.
    """
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
            try:
                col_name = c.alias or c.name
            except AttributeError:
                pass
            else:
                setattr(self, col_name, c)
            cs.append(c)
        self._columns = tuple(cs)

    def __bool__(self):
        return bool(self._columns)

    __nonzero__ = __bool__

    def __getattr__(self, name):
        # Here only so that pylint knows this class has a set of
        # dynamic attributes.
        raise AttributeError("Column list %s does not include %s" % (
            self._col_list() or "()",
            name
        ))

    def has_column(self, name):
        return hasattr(self, name)

    def __contains__(self, column):
        return column in self._columns

    def is_subset(self, other):
        return all(c in other for c in self)

    def __getitem__(self, ix):
        return self._columns[ix]

    def __iter__(self):
        return iter(self._columns)

    def _col_list(self):
        return ','.join(str(c) for c in self._columns)

    def __compile_visit__(self, compiler):
        compiler.visit_csv(self._columns)

    def has_bind_param(self):
        return any(
            IBindParam.providedBy(c)
            for c in self._columns
        )

    def is_ambiguous(self, column):
        """
        Does the *column* name appear more than once?

        If so it needs to be qualified.
        """
        count = 0
        for c in self._columns:
            if c.name == column.name:
                count += 1
        return count > 1

    def as_select_list(self):
        from .select import _SelectColumns
        return _SelectColumns(self._columns)

    def as_names(self):
        return {c.name for c in self}

    def as_dict(self):
        # This fails if there are duplicates.
        return {col.name: col for col in self._columns}

    def union(self, other):
        """
        Discards duplicates.
        """
        od = other.as_dict()
        od.update(self.as_dict())
        return type(self)(
            sorted(od.values(), key=lambda c: c.name))

    def dup_union(self, other, combining=()):
        """
        Preserves duplicates, with the exception of columns in *combining*.
        """
        seen_combining = set()
        columns = []

        for c in self._columns + other._columns:
            if c.name in combining:
                if c.name not in seen_combining:
                    columns.append(c)
                    seen_combining.add(c.name)
            else:
                columns.append(c)
        return type(self)(columns)

    def intersection(self, other):
        col_names = set(other.as_dict()).intersection(set(self.as_dict()))
        col_names = sorted(col_names)
        return type(self)(
            getattr(self, c)
            for c in col_names
        )

    def __repr__(self):
        return '<%s %s>' % (
            type(self).__name__,
            ', '.join(repr(c) for c in self)
        )

    def __eq__(self, other):
        try:
            return self.as_dict() == other.as_dict()
        except AttributeError:
            return NotImplemented
