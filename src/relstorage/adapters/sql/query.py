# -*- coding: utf-8 -*-
"""
Compiled queries ready for execution.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from weakref import WeakKeyDictionary

from relstorage._util import CachedIn

from ._util import copy
from ._util import Columns
from .dialect import DialectAware

from .expressions import ParamMixin


class Clause(DialectAware):
    """
    A portion of a SQL statement.
    """



class ColumnList(Columns):
    """
    List of columns used in a query.
    """

    # This class exists for semantics, it currently doesn't
    # do anything different than the super.


class Query(ParamMixin,
            Clause):
    __name__ = None
    prepare = False

    def __str__(self):
        return str(self.compiled())

    def __get__(self, inst, klass):
        if inst is None:
            return self
        # We need to set this into the instance's dictionary.
        # Otherwise we'll be rebinding and recompiling each time we're
        # accessed which is not good (in fact it's a step backwards
        # from query_property()). On Python 3.6+, there's the
        # `__set_name__(klass, name)` called on the descriptor which
        # does the job perfectly. In earlier versions, we're on our
        # own.
        #
        # TODO: In test cases, we spend a lot of time binding and compiling.
        # Can we find another layer of caching somewhere?
        result = self.bind(inst).compiled()
        if not self.__name__:
            # Go through the class hierarchy, find out what we're called.
            for base in klass.mro():
                for k, v in vars(base).items():
                    if v is self:
                        self.__name__ = k
                        break
        assert self.__name__

        vars(inst)[self.__name__] = result

        return result

    def __set_name__(self, owner, name):
        self.__name__ = name

    @CachedIn('_v_compiled')
    def compiled(self):
        return CompiledQuery(self)

    def prepared(self):
        """
        Note that it's good to prepare this query, if
        supported by the driver.
        """
        s = copy(self)
        s.prepare = True
        return s


class CompiledQuery(object):
    """
    Represents a completed query.
    """

    stmt = None
    params = None
    _raw_stmt = None
    _prepare_stmt = None
    _prepare_converter = None

    def __init__(self, root):
        self.root = root
        # We do not keep a reference to the context;
        # it's likely to be an instance object that's
        # going to have us stored in its dictionary.
        dialect = root.dialect

        compiler = dialect.compiler(root)
        self.stmt, self.params = compiler.compile()
        self._raw_stmt = self.stmt # for debugging
        if compiler.can_prepare():
            self._prepare_stmt, self.stmt, self._prepare_converter = compiler.prepare()

    def __repr__(self):
        if self._prepare_stmt:
            return "%s (%s)" % (
                self.stmt,
                self._prepare_stmt
            )
        return self.stmt

    def __str__(self):
        return self.stmt

    _cursor_cache = WeakKeyDictionary()

    def _stmt_cache_for_cursor(self, cursor):
        """Returns a dictionary."""
        # If we can't store it directly on the cursor, as happens for
        # types implemented in C, we use a weakkey dictionary.
        try:
            cursor_prep_stmts = cursor._rs_prepared_statements
        except AttributeError:
            try:
                cursor_prep_stmts = cursor._rs_prepared_statements = {}
            except AttributeError:
                cursor_prep_stmts = self._cursor_cache.get(cursor)
                if cursor_prep_stmts is None:
                    cursor_prep_stmts = self._cursor_cache[cursor] = {}
        return cursor_prep_stmts

    def execute(self, cursor, params=None):
        # (Any, dict) -> None
        # TODO: Include literals from self.params.
        # TODO: Syntax transformation if they don't support names.
        # TODO: Validate given params match expected ones, nothing missing?
        stmt = self.stmt
        if self._prepare_stmt:
            # Prepare on demand.

            # In all databases, prepared statements
            # persist past COMMIT/ROLLBACK (though in PostgreSQL
            # preparing them takes locks that only go away at
            # COMMIT/ROLLBACK). But they don't persist past a session
            # restart (new connection) (obviously).
            #
            # Thus we keep a cache of statements we have prepared for
            # this particular connection/cursor.
            # TODO: This should probably really be on the connection,
            # not the cursor. connection is session.
            cursor_prep_stmts = self._stmt_cache_for_cursor(cursor)
            try:
                stmt = cursor_prep_stmts[self._prepare_stmt]
            except KeyError:
                stmt = cursor_prep_stmts[self._prepare_stmt] = self.stmt
                __traceback_info__ = self._prepare_stmt, self, self.root.dialect.compiler(self.root)
                cursor.execute(self._prepare_stmt)
            params = self._prepare_converter(params)

        __traceback_info__ = stmt, params
        if params:
            cursor.execute(stmt, params)
        elif self.params:
            # XXX: This isn't really good.
            # If there are both literals in the SQL and params,
            # we don't handle that.
            cursor.execute(stmt, self.params)
        else:
            cursor.execute(stmt)
