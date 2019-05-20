# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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
umysqldb IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

import operator
import re
import sys

import six
from ZODB.POSException import TransactionTooLargeError

from relstorage._compat import intern

from .pymysql import PyMySQLDriver

__all__ = [
    'umysqldbDriver',
]

logger = __import__('logging').getLogger(__name__)

param_match = re.compile(r'%\(.*?\)s')

# {orig_sql: (new_sql, itemgetter)}
_dict_cache = {}

# The underlying umysql driver doesn't handle dicts as arguments
# to queries (as of 2.61). Until it does, we need to do that
# because RelStorage uses that in a few places

# Error handling:

# umysql contains its own mapping layer which first goes through
# pymysl.err.error_map, but that only handles a very small number
# of errors. First, umysql errors get mapped to a subclass of pymysql.err.Error,
# either an explicit one or OperationalError or InternalError.

# Next, RuntimeError subclasses get mapped to ProgrammingError
# or stay as is.

# The problem is that some errors are not mapped appropriately. In
# particular, IOError is prone to escaping as-is, which relstorage
# isn't expecting, thus defeating its try/except blocks.

# We must catch that here. There may be some other things we
# want to catch and map, but we'll do that on a case by case basis
# (previously, we mapped everything to Error, which may have been
# hiding some issues)

UConnection = None

def _make_connection_class():
    # pylint:disable=too-many-statements,import-error,no-name-in-module
    global UConnection
    if UConnection is not None:
        return UConnection

    # umysqldb piggybacks on much of the implementation of PyMySQL
    import umysqldb
    import umysql
    import umysqldb.connections

    from pymysql.err import InternalError, InterfaceError, ProgrammingError
    class Connection(umysqldb.connections.Connection): # pylint:disable=no-member
        # pylint:disable=abstract-method
        _umysql_conn = None

        def __debug_lock(self, sql, ex=False): # pragma: no cover
            if 'GET_LOCK' not in sql:
                return

            try:
                result = self._result
                if result is None:
                    logger.warn("No result from GET_LOCK query: %s",
                                result.__dict__, exc_info=ex)
                    return
                if not result.affected_rows:
                    logger.warn("Zero rowcount from GET_LOCK query: %s",
                                result.__dict__, exc_info=ex)
                if not result.rows:
                    # We see this a fair amount. The C code in umysql
                    # got a packet that its treating as an "OK"
                    # response, for which it just returns a tuple
                    # (affected_rows, rowid), but no actual rows. In
                    # all cases, it has been returning affected_rows
                    # of 2? We *could* patch the rows variable here to
                    # be [0], indicating the lock was not taken, but
                    # given that OK response I'm not sure that's right
                    # just yet
                    logger.warn("Empty rows from GET_LOCK query: %s",
                                result.__dict__, exc_info=ex)
            except Exception: # pylint: disable=broad-except
                logger.exception("Failed to debug lock problem")

        def __dict_to_tuple(self, sql, args):
            """
            Transform a dict-format expression into the equivalent
            tuple version.

            Caches statements. We know we only use a small number of small
            hard coded strings.
            """
            try:
                # This is racy, but it's idempotent
                tuple_sql, itemgetter = _dict_cache[sql]
            except KeyError:
                dict_exprs = param_match.findall(sql)
                if not dict_exprs:
                    tuple_sql = sql
                    itemgetter = lambda d: ()
                else:
                    itemgetter = operator.itemgetter(*[dict_expr[2:-2] for dict_expr in dict_exprs])
                    if len(dict_exprs) == 1:
                        _itemgetter = itemgetter
                        itemgetter = lambda d: (_itemgetter(d),)
                    tuple_sql = param_match.sub('%s', sql)
                    tuple_sql = intern(tuple_sql)
                _dict_cache[sql] = (tuple_sql, itemgetter)

            return tuple_sql, itemgetter(args)

        def query(self, sql, args=()):
            __traceback_info__ = args
            if isinstance(args, dict):
                sql, args = self.__dict_to_tuple(sql, args)

            try:
                return super(Connection, self).query(sql, args=args)
            except IOError: # pragma: no cover
                self.__debug_lock(sql, True)
                six.reraise(InterfaceError, None, sys.exc_info()[2])
            except ProgrammingError as e:
                if e.args[0] == 'cursor closed':
                    # This has only been seen during aborts and rollbacks; however, if it
                    # happened at some other time it might lead to inconsistency...
                    # ...and it turns out it did, once we started using prepared statements.
                    # So instead, translate to a disconnected exception that the higher levels
                    # know how to deal with.
                    six.reraise(IOError, e, sys.exc_info()[2])
                else: # pragma: no cover
                    raise
            except InternalError as e: # pragma: no cover
                # Rare.
                self.__debug_lock(sql, True)
                if e.args == (0, 'Socket receive buffer full'):
                    # This is a function of the server having a larger
                    # ``max_allowed_packet`` than ultramysql can
                    # handle. umysql up through at least 2.61
                    # hardcodes a receive (and send) buffer size of
                    # 16MB
                    # (https://github.com/esnme/ultramysql/issues/34).
                    # If the server has a larger value then that and generates
                    # a packet bigger than that, we get this error (after spending
                    # time reading 16Mb, of course). This can happen for a single row
                    # (if the blob chunk size was configured too high), or it can
                    # happen for aggregate queries (the dbiter.iter_objects query is
                    # particularly common cause of this.) Retrying won't help.
                    raise TransactionTooLargeError(
                        "umysql got results bigger than 16MB."
                        " Reduce the server's max_allowed_packet setting.")
                raise
            except Exception: # pragma: no cover
                self.__debug_lock(sql, True)
                raise

        def __reconnect(self):
            assert not self._umysql_conn.is_connected()
            self._umysql_conn.close()
            del self._umysql_conn
            self._umysql_conn = umysql.Connection() # pylint:disable=no-member
            self._connect()  # Potentially this could raise again?

        def connect(self, *_args, **_kwargs): # pragma: no cover pylint:disable=arguments-differ
            # Redirect the PyMySQL connect method to the umysqldb method, that's
            # already captured the host and port. (XXX: Why do we do this?)
            return self._connect()

    UConnection = Connection
    return UConnection


class umysqldbDriver(PyMySQLDriver):
    __name__ = 'umysqldb'

    AVAILABLE_ON_PYPY = False
    MODULE_NAME = __name__
    PRIORITY = PRIORITY_PYPY = 100 # Last of the list.

    def __init__(self):
        super(umysqldbDriver, self).__init__()
        self.connect = _make_connection_class()

        # umysql has a tendency to crash when given a bytearray (which
        # is what pymysql.Binary would produce), at least on OS X.
        self.Binary = bytes
