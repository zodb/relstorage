# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

"""
A small abstraction layer for SQL queries.

Features:

    - Simple, readable syntax for writing queries.

    - Automatic switching between history-free and history-preserving
      schemas.

    - Support for automatically preparing statements (depending on the
      driver; some allow parameters, some do not, for example)

    - Always use bind parameters

    - Take care of minor database syntax issues.

This is inspired by the SQLAlchemy Core, but we don't use it because
we don't want to take a dependency that can conflict with applications
using RelStorage.
"""
# pylint:disable=too-many-lines

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from copy import copy as stdlib_copy
from operator import attrgetter
from weakref import WeakKeyDictionary

from zope.interface import implementer

from relstorage._compat import NStringIO
from relstorage._compat import intern
from relstorage._util import CachedIn
from .interfaces import IDBDialect

def copy(obj):
    new = stdlib_copy(obj)
    volatile = [k for k in vars(new) if k.startswith('_v')]
    for k in volatile:
        delattr(new, k)
    return new

class Type(object):
    """
    A database type.
    """

class _Unknown(Type):
    "Unspecified."

class Integer64(Type):
    """
    A 64-bit integer.
    """

class OID(Integer64):
    """
    Type of an OID.
    """

class TID(Integer64):
    """
    Type of a TID.
    """

class BinaryString(Type):
    """
    Arbitrary sized binary string.
    """

class State(Type):
    """
    Used for storing object state.
    """

class Boolean(Type):
    """
    A two-value column.
    """

class _Resolvable(object):

    def resolve_against(self, table):
        # pylint:disable=unused-argument
        return self

class Column(_Resolvable):
    """
    Defines a column in a table.
    """

    def __init__(self, name, type_=_Unknown, primary_key=False, nullable=True):
        self.name = name
        self.type_ = type_ if isinstance(type_, Type) else type_()
        self.primary_key = primary_key
        self.nullable = False if primary_key else nullable

    def is_type(self, type_):
        return isinstance(self.type_, type_)

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return _EqualExpression(self, other)

    def __gt__(self, other):
        return _GreaterExpression(self, other)

    def __ge__(self, other):
        return _GreaterEqualExpression(self, other)

    def __ne__(self, other):
        return _NotEqualExpression(self, other)

    def __le__(self, other):
        return _LessEqualExpression(self, other)

    def __compile_visit__(self, compiler):
        compiler.visit_column(self)


class _LiteralNode(_Resolvable):
    def __init__(self, raw):
        self.raw = raw
        self.name = 'anon_%x' % (id(self),)

    def __compile_visit__(self, compiler):
        compiler.emit(str(self.raw))

    def resolve_against(self, table):
        return self

class _TextNode(_LiteralNode):
    pass

class _Columns(object):
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
            isinstance(c, (_BindParam, _OrderedBindParam))
            for c in self._columns
        )

    def as_select_list(self):
        return _SelectColumns(self._columns)

_ColumnList = _Columns

class _SelectColumns(_Columns):

    def __compile_visit__(self, compiler):
        compiler.visit_select_list_csv(self._columns)

class Table(object):
    """
    A table relation.
    """

    def __init__(self, name, *columns):
        self.name = name
        self.columns = columns
        self.c = _Columns(columns)

    def __str__(self):
        return self.name

    def select(self, *args, **kwargs):
        return Select(self, *args, **kwargs)

    def __compile_visit__(self, compiler):
        compiler.emit_identifier(self.name)

    def bindparam(self, key):
        return bindparam(key)

    def orderedbindparam(self):
        return orderedbindparam()

    def natural_join(self, other_table):
        return NaturalJoinedTable(self, other_table)

    def insert(self, *args, **kwargs):
        return Insert(self, *args, **kwargs)

class TemporaryTable(Table):
    """
    A temporary table.
    """

class _CompositeTableMixin(object):

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

        common_columns = []
        for col in lhs.columns:
            if hasattr(rhs.c, col.name):
                common_columns.append(col)
        self.columns = common_columns
        self.c = _Columns(common_columns)

    def select(self, *args, **kwargs):
        return Select(self, *args, **kwargs)

    def bindparam(self, key):
        return bindparam(key)

    def orderedbindparam(self):
        return orderedbindparam()

@implementer(IDBDialect)
class DefaultDialect(object):

    keep_history = True

    datatype_map = {
        OID: 'BIGINT',
        TID: 'BIGINT',
        BinaryString: 'BYTEA',
        State: 'BYTEA',
        Boolean: 'BOOLEAN',
    }

    def bind(self, context):
        # The context will reference us most likely
        # (compiled statement in instance dictionary)
        # so try to avoid reference cycles.
        keep_history = context.keep_history
        new = copy(self)
        new.keep_history = keep_history
        return new

    def compiler_class(self):
        return Compiler

    def compiler(self, root):
        return self.compiler_class()(root)

    def datatypes_for_columns(self, column_list):
        columns = list(column_list)
        datatypes = []
        for column in columns:
            datatype = self.datatype_map[type(column.type_)]
            datatypes.append(datatype)
        return datatypes

    def __eq__(self, other):
        if isinstance(other, DefaultDialect):
            return other.keep_history == self.keep_history
        return NotImplemented


class _MissingDialect(DefaultDialect):
    def __bool__(self):
        return False

    __nonzero__ = __bool__


class _Bindable(object):

    context = _MissingDialect()

    _dialect_locations = (
        attrgetter('dialect'),
        attrgetter('driver.dialect'),
        attrgetter('poller.driver.dialect'),
        attrgetter('connmanager.driver.dialect'),
        attrgetter('adapter.driver.dialect')
    )

    def _find_dialect(self, context):
        # Find the dialect to use for the context. If it specifies
        # one, then use it. Otherwise go hunting for the database
        # driver and use *it*. Preferably the driver is attached to
        # the object we're looking at, but if not that, we'll look at
        # some common attributes for adapter objects for it.
        if isinstance(context, DefaultDialect):
            return context

        for getter in self._dialect_locations:
            try:
                dialect = getter(context)
            except AttributeError:
                pass
            else:
                return dialect.bind(context)
        __traceback_info__ = vars(context)
        raise TypeError("Unable to bind to %s; no dialect found" % (context,))

    def bind(self, context):
        context = self._find_dialect(context)
        if context is None:
            return self

        new = copy(self)
        new.context = context
        bound_replacements = {
            k: v.bind(context)
            for k, v
            in vars(new).items()
            if isinstance(v, _Bindable)
        }
        for k, v in bound_replacements.items():
            setattr(new, k, v)
        return new

class NaturalJoinedTable(_Bindable,
                         _CompositeTableMixin):

    def __init__(self, lhs, rhs):
        super(NaturalJoinedTable, self).__init__(lhs, rhs)
        assert self.c
        self._join_columns = self.c

        # TODO: Check for data type mismatches, etc?
        self.columns = list(self.lhs.columns)
        for col in self.rhs.columns:
            if not hasattr(self.lhs.c, col.name):
                self.columns.append(col)
        self.columns = tuple(self.columns)
        self.c = _Columns(self.columns)

    def __compile_visit__(self, compiler):
        compiler.visit(self.lhs)
        compiler.emit_keyword('JOIN')
        compiler.visit(self.rhs)
        # careful with USING clause in a join: Oracle doesn't allow such
        # columns to have a prefix.
        compiler.emit_keyword('USING')
        compiler.visit_grouped(self._join_columns)


class HistoryVariantTable(_Bindable,
                          _CompositeTableMixin):
    """
    A table that can be one of two tables, depending on whether
    the instance is keeping history or not.
    """

    @property
    def history_preserving(self):
        return self.lhs

    @property
    def history_free(self):
        return self.rhs

    def __compile_visit__(self, compiler):
        keep_history = self.context.keep_history
        node = self.history_preserving if keep_history else self.history_free
        return compiler.visit(node)


class _CompiledQuery(object):
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
        context = root.context

        compiler = context.compiler(root)
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
            #
            cursor_prep_stmts = self._stmt_cache_for_cursor(cursor)
            try:
                stmt = cursor_prep_stmts[self._prepare_stmt]
            except KeyError:
                stmt = cursor_prep_stmts[self._prepare_stmt] = self.stmt
                __traceback_info__ = self._prepare_stmt, self, self.root.context.compiler(self.root)
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

class Compiler(object):

    def __init__(self, root):
        self.buf = NStringIO()
        self.placeholders = {}
        self.root = root


    def __repr__(self):
        return "<%s %s %r>" % (
            type(self).__name__,
            self.buf.getvalue(),
            self.placeholders
        )

    def compile(self):
        self.visit(self.root)
        return self.finalize()

    def can_prepare(self):
        # Obviously this needs to be more general.
        # Some drivers, for example, can't deal with parameters
        # in a prepared statement.
        return self.root.prepare and isinstance(self.root, _Query)

    _prepared_stmt_counter = 0

    @classmethod
    def _next_prepared_stmt_name(cls):
        cls._prepared_stmt_counter += 1
        return 'rs_prep_stmt_%d' % (cls._prepared_stmt_counter,)

    def _prepared_param(self, number):
        return '$' + str(number)

    _PREPARED_CONJUNCTION = 'AS'

    def _quote_query_for_prepare(self, query):
        return query

    def _find_datatypes_for_prepared_query(self):
        # Deduce the datatypes based on the types of the columns
        # we're sending as params.
        if isinstance(self.root, Insert):
            root = self.root
            dialect = root.context
            # TODO: Should probably delegate this to the node.
            if root.values and root.column_list:
                # If we're sending in a list of values, those have to
                # exactly match the columns, so we can easily get a list
                # of datatypes.
                column_list = root.column_list
                datatypes = dialect.datatypes_for_columns(column_list)
            elif root.select and root.select.column_list.has_bind_param():
                targets = root.column_list
                sources = root.select.column_list
                # TODO: This doesn't support bind params anywhere except the
                # select list!
                columns_with_params = [
                    target
                    for target, source in zip(targets, sources)
                    if isinstance(source, _OrderedBindParam)
                ]
                assert len(self.placeholders) == len(columns_with_params)
                datatypes = dialect.datatypes_for_columns(columns_with_params)
            return datatypes
        return ()

    def prepare(self):
        # This is correct for PostgreSQL. This needs moved to a dialect specific
        # spot.

        datatypes = self._find_datatypes_for_prepared_query()
        query = self.buf.getvalue()
        name = self._next_prepared_stmt_name()

        if datatypes:
            assert isinstance(datatypes, (list, tuple))
            datatypes = ', '.join(datatypes)
            datatypes = ' (%s)' % (datatypes,)
        else:
            datatypes = ''

        q = query.strip()

        # PREPARE needs the query string to use $1, $2, $3, etc,
        # as placeholders.
        # In MySQL, it's a plain question mark.
        placeholder_to_number = {}
        counter = 0
        for placeholder_name in self.placeholders.values():
            counter += 1
            placeholder = self._placeholder(placeholder_name)
            placeholder_to_number[placeholder_name] = counter
            param = self._prepared_param(counter)
            q = q.replace(placeholder, param, 1)

        q = self._quote_query_for_prepare(q)

        stmt = 'PREPARE {name}{datatypes} {conjunction} {query}'.format(
            name=name, datatypes=datatypes,
            query=q,
            conjunction=self._PREPARED_CONJUNCTION,
        )


        if placeholder_to_number:
            execute = 'EXECUTE {name}({params})'.format(
                name=name,
                params=','.join(['%s'] * len(self.placeholders)),
            )
        else:
            # Neither MySQL nor PostgreSQL like a set of empty parens: ()
            execute = 'EXECUTE {name}'.format(name=name)

        if '%s' in placeholder_to_number:
            # There was an ordered param. If there was one,
            # they must all be ordered, so there's no need to convert anything.
            assert len(placeholder_to_number) == 1
            def convert(p):
                return p
        else:
            def convert(d):
                # TODO: This may not actually be needed, since we issue a regular
                # cursor.execute(), it may be able to handle named?
                params = [None] * len(placeholder_to_number)
                for placeholder_name, ix in placeholder_to_number.items():
                    params[ix - 1] = d[placeholder_name]
                return params

        return intern(stmt), intern(execute), convert

    def finalize(self):
        return intern(self.buf.getvalue().strip()), {v: k for k, v in self.placeholders.items()}

    def visit(self, node):
        node.__compile_visit__(self)

    visit_clause = visit

    def emit(self, *contents):
        for content in contents:
            self.buf.write(content)

    def emit_w_padding_space(self, value):
        ended_in_space = self.buf.getvalue().endswith(' ')
        value = value.strip()
        if not ended_in_space:
            self.buf.write(' ')
        self.emit(value, ' ')

    emit_keyword = emit_w_padding_space

    def emit_identifier(self, identifier):
        last_char = self.buf.getvalue()[-1]
        if last_char not in ('(', ' '):
            self.emit(' ', identifier)
        else:
            self.emit(identifier)

    def visit_column_list(self, column_list):
        clist = column_list.c if hasattr(column_list, 'c') else column_list
        self.visit(clist)

    def visit_select_list(self, column_list):
        clist = column_list.c if hasattr(column_list, 'c') else column_list
        self.visit(clist.as_select_list())

    def visit_csv(self, nodes):
        self.visit(nodes[0])
        for node in nodes[1:]:
            self.emit(', ')
            self.visit(node)

    visit_select_expression = visit

    def visit_select_list_csv(self, nodes):
        self.visit_select_expression(nodes[0])
        for node in nodes[1:]:
            self.emit(', ')
            self.visit_select_expression(node)

    def visit_column(self, column_node):
        self.emit_identifier(column_node.name)

    def visit_from(self, from_):
        self.emit_keyword('FROM')
        self.visit(from_)

    def visit_grouped(self, clause):
        self.emit('(')
        self.visit(clause)
        self.emit(')')

    def visit_op(self, op):
        self.emit(' ' + op + ' ')

    def _next_placeholder_name(self, prefix='param'):
        return '%s_%d' % (prefix, len(self.placeholders),)

    def _placeholder(self, key):
        # Write things in `pyformat` style by default, assuming a
        # dictionary of params; this is supported by most drivers.
        if key == '%s':
            return key
        return '%%(%s)s' % (key,)

    def _placeholder_for_literal_param_value(self, value):
        placeholder = self.placeholders.get(value)
        if not placeholder:
            placeholder_name = self._next_placeholder_name(prefix='literal')
            placeholder = self._placeholder(placeholder_name)
            self.placeholders[value] = placeholder_name
        return placeholder

    def visit_literal_expression(self, value):
        placeholder = self._placeholder_for_literal_param_value(value)
        self.emit(placeholder)

    def visit_boolean_literal_expression(self, value):
        # In the oracle dialect, this needs to be
        # either "'Y'" or "'N'"
        assert isinstance(value, bool)
        self.emit(str(value).upper())

    def visit_bind_param(self, bind_param):
        self.placeholders[bind_param] = bind_param.key
        self.emit(self._placeholder(bind_param.key))

    def visit_ordered_bind_param(self, bind_param):
        self.placeholders[bind_param] = '%s'
        self.emit('%s')

_Compiler = Compiler # BWC. Remove

class _Expression(_Bindable,
                  _Resolvable):
    """
    A SQL expression.
    """

class _BindParam(_Expression):

    def __init__(self, key):
        self.key = key

    def __compile_visit__(self, compiler):
        compiler.visit_bind_param(self)


def bindparam(key):
    return _BindParam(key)

class _LiteralExpression(_Expression):

    def __init__(self, value):
        self.value = value

    def __compile_visit__(self, compiler):
        compiler.visit_literal_expression(self.value)

class _BooleanLiteralExpression(_LiteralExpression):

    def __compile_visit__(self, compiler):
        compiler.visit_boolean_literal_expression(self.value)

class _OrderedBindParam(_Expression):

    name = '%s'

    def __compile_visit__(self, compiler):
        compiler.visit_ordered_bind_param(self)

def orderedbindparam():
    return _OrderedBindParam()

def _as_node(c):
    if isinstance(c, int):
        return _LiteralNode(c)
    if isinstance(c, str):
        return _TextNode(c)
    return c

def _as_expression(stmt):
    if hasattr(stmt, '__compile_visit__'):
        return stmt

    if isinstance(stmt, bool):
        # The values True and False are handled
        # specially because their representation varies
        # among databases (Oracle)
        stmt = _BooleanLiteralExpression(stmt)
    else:
        stmt = _LiteralExpression(stmt)
    return stmt

class _BinaryExpression(_Expression):
    """
    Expresses a comparison.
    """

    def __init__(self, op, lhs, rhs):
        self.op = op
        self.lhs = lhs # type: Column
        # rhs is either a literal or a column;
        # certain literals are handled specially.
        rhs = _as_expression(rhs)
        self.rhs = rhs

    def __str__(self):
        return '%s %s %s' % (
            self.lhs,
            self.op,
            self.rhs
        )

    def __compile_visit__(self, compiler):
        compiler.visit(self.lhs)
        compiler.visit_op(self.op)
        compiler.visit(self.rhs)

    def resolve_against(self, table):
        lhs = self.lhs.resolve_against(table)
        rhs = self.rhs.resolve_against(table)
        new = copy(self)
        new.rhs = rhs
        new.lhs = lhs
        return new

class _EmptyExpression(_Expression):
    """
    No comparison at all.
    """

    def __bool__(self):
        return False

    __nonzero__ = __bool__

    def __str__(self):
        return ''

    def and_(self, expression):
        return expression

    def __compile_visit__(self, compiler):
        "Does nothing"

class _EqualExpression(_BinaryExpression):

    def __init__(self, lhs, rhs):
        _BinaryExpression.__init__(self, '=', lhs, rhs)

class _NotEqualExpression(_BinaryExpression):

    def __init__(self, lhs, rhs):
        _BinaryExpression.__init__(self, '<>', lhs, rhs)


class _GreaterExpression(_BinaryExpression):

    def __init__(self, lhs, rhs):
        _BinaryExpression.__init__(self, '>', lhs, rhs)

class _GreaterEqualExpression(_BinaryExpression):

    def __init__(self, lhs, rhs):
        _BinaryExpression.__init__(self, '>=', lhs, rhs)

class _LessEqualExpression(_BinaryExpression):

    def __init__(self, lhs, rhs):
        _BinaryExpression.__init__(self, '<=', lhs, rhs)


class _Clause(_Bindable):
    """
    A portion of a SQL statement.
    """

class _And(_Expression):

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def __compile_visit__(self, compiler):
        compiler.visit_grouped(_BinaryExpression('AND', self.lhs, self.rhs))

    def resolve_against(self, table):
        return type(self)(self.lhs.resolve_against(table),
                          self.rhs.resolve_against(table))

class _WhereClause(_Clause):

    def __init__(self, expression):
        self.expression = expression

    def and_(self, expression):
        expression = _And(self.expression, expression)
        new = copy(self)
        new.expression = expression
        return new

    def __compile_visit__(self, compiler):
        compiler.emit_keyword(' WHERE')
        compiler.visit_grouped(self.expression)

class _OrderBy(_Clause):

    def __init__(self, expression, dir):
        self.expression = expression
        self.dir = dir

    def __compile_visit__(self, compiler):
        compiler.emit(' ORDER BY ')
        compiler.visit(self.expression)
        if self.dir:
            compiler.emit(' ' + self.dir)


def _where(expression):
    if expression:
        return _WhereClause(expression)

class _Query(_Bindable):
    __name__ = None
    prepare = False

    def __str__(self):
        return str(self.compiled())

    def __get__(self, inst, klass):
        if inst is None:
            return self
        # We need to set this into the instance's dictionary.
        # Otherwise we'll be rebinding and recompiling each time we're
        # accessed which is not good. On Python 3.6+, there's the
        # `__set_name__(klass, name)` called on the descriptor which
        # does the job perfectly. In earlier versions, we're on our
        # own.
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
        return _CompiledQuery(self)

    def prepared(self):
        """
        Note that it's good to prepare this query, if
        supported by the driver.
        """
        s = copy(self)
        s.prepare = True
        return s

class _DeferredColumn(Column):

    def resolve_against(self, table):
        return getattr(table.c, self.name)

class _DeferredColumns(object):

    def __getattr__(self, name):
        return _DeferredColumn(name)

class _It(object):
    """
    A proxy that select can resolve to tables in the current table.
    """

    c = _DeferredColumns()

    def bindparam(self, name):
        return bindparam(name)

it = _It()

def _resolved_against(columns, table):
    resolved = [
        _as_node(c).resolve_against(table)
        for c
        in columns
    ]
    return resolved

class Select(_Query):
    """
    A Select query.

    When instances of this class are stored in a class dictionary,
    they function as non-data descriptors: The first time they are
    accessed, they *bind* themselves to the instance and select the
    appropriate SQL syntax and compile themselves into a string.
    """

    _distinct = _EmptyExpression()
    _where = _EmptyExpression()
    _order_by = _EmptyExpression()
    _limit = None
    _for_update = None
    _nowait = None

    def __init__(self, table, *columns):
        self.table = table
        if columns:
            self.column_list = _ColumnList(_resolved_against(columns, table))
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
        s._order_by = _OrderBy(expression, dir)
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
        s._distinct = _TextNode('DISTINCT')
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

class _Functions(object):

    def max(self, column):
        return _Function('max', column)

class _Function(_Expression):

    def __init__(self, name, expression):
        self.name = name
        self.expression = expression

    def __compile_visit__(self, compiler):
        compiler.emit_identifier(self.name)
        compiler.visit_grouped(self.expression)

func = _Functions()

class Insert(_Query):

    column_list = None
    select = None
    epilogue = ''
    values = None

    def __init__(self, table, *columns):
        self.table = table
        if columns:
            self.column_list = _Columns(_resolved_against(columns, table))
            # TODO: Probably want a different type, like a ValuesList
            self.values = _Columns([orderedbindparam() for _ in columns])

    def from_select(self, names, select):
        i = copy(self)
        i.column_list = _Columns(names)
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
