# -*- coding: utf-8 -*-
"""
RDBMS-specific SQL.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from operator import attrgetter

from zope.interface import implementer

from relstorage._compat import NStringIO
from relstorage._compat import intern
from ..interfaces import IDBDialect

from .types import OID
from .types import TID
from .types import BinaryString
from .types import State
from .types import Boolean

from ._util import copy
from .interfaces import ITypedParams

# pylint:disable=too-many-function-args

@implementer(IDBDialect)
class DefaultDialect(object):

    keep_history = True
    _context_repr = None

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
        new._context_repr = repr(context)
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
        return NotImplemented # pragma: no cover

    def __repr__(self):
        return "<%s at %x keep_history=%s context=%s>" % (
            type(self).__name__,
            id(self),
            self.keep_history,
            self._context_repr
        )


class _MissingDialect(DefaultDialect):
    def __bool__(self):
        return False

    __nonzero__ = __bool__


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
        # in a prepared statement; we currently handle that by overriding
        # this method.
        return self.root.prepare

    _prepared_stmt_counter = 0

    @classmethod
    def _next_prepared_stmt_name(cls, query):
        # Even with the GIL, this isn't fully safe to do; two threads
        # can still get the same value. We don't want to allocate a
        # lock because we might be patched by gevent later. So that's
        # where `query` comes in: we add the hash as a disambiguator.
        # Of course, for there to be a duplicate prepared statement
        # sent to the database, that would mean that we were somehow
        # using the same cursor or connection in multiple threads at
        # once (or perhaps we got more than one cursor from a
        # connection? We should only have one.)
        #
        # TODO: Sidestep this problem by allocating this earlier;
        # the SELECT or INSERT statement could pick it when it is created;
        # that happens at the class level at import time, when we should be
        # single-threaded.
        #
        # That may also help facilitate caching.
        cls._prepared_stmt_counter += 1
        return 'rs_prep_stmt_%d_%d' % (
            cls._prepared_stmt_counter,
            abs(hash(query)),
        )

    def _prepared_param(self, number):
        return '$' + str(number)

    _PREPARED_CONJUNCTION = 'AS'

    def _quote_query_for_prepare(self, query):
        return query

    def _find_datatypes_for_prepared_query(self):
        # Deduce the datatypes based on the types of the columns
        # we're sending as params.
        result = ()
        param_provider = ITypedParams(self.root, None)
        if param_provider is not None:
            result = param_provider.datatypes_for_parameters() # pylint:disable=assignment-from-no-return
        return result

    def prepare(self):
        # This is correct for PostgreSQL. This needs moved to a dialect specific
        # spot.

        datatypes = self._find_datatypes_for_prepared_query()
        query = self.buf.getvalue()
        name = self._next_prepared_stmt_name(query)

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

    def emit_null(self):
        self.emit('NULL')

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


class _DefaultContext(object):

    keep_history = True


class DialectAware(object):

    context = _DefaultContext()
    dialect = _MissingDialect()

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
        __traceback_info__ = getattr(context, '__dict__', ()) # vars() doesn't work on e.g., None
        raise TypeError("Unable to bind to %s; no dialect found" % (context,))

    def bind(self, context, dialect=None):
        if dialect is None:
            dialect = self._find_dialect(context)

        assert dialect is not None

        new = copy(self)
        if context is not None:
            new.context = context
        new.dialect = dialect
        bound_replacements = {
            k: v.bind(context, dialect)
            for k, v
            in vars(new).items()
            if isinstance(v, DialectAware)
        }
        for k, v in bound_replacements.items():
            setattr(new, k, v)
        return new
