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
MySQL IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

from ...drivers import AbstractModuleDriver
from ...drivers import implement_db_driver_options
from ...sql import Compiler
from ...sql import DefaultDialect
from ...sql import OID
from ...sql import TID
from ...sql import BinaryString
from ...sql import State

database_type = 'mysql'

class MySQLCompiler(Compiler):

    def can_prepare(self):
        # If there are params, we can't prepare unless we're using
        # the binary protocol; otherwise we have to SET user variables
        # with extra round trips, which is worse.
        return not self.placeholders and super(MySQLCompiler, self).can_prepare()

    _PREPARED_CONJUNCTION = 'FROM'

    def _prepared_param(self, number):
        return '?'

    def _quote_query_for_prepare(self, query):
        return "'{query}'".format(query=query)

    # Upserts: It's a good idea to use `ON DUPLICATE KEY UPDATE`
    # instead of REPLACE because `ON DUPLICATE` updates rows in place
    # instead of performing a DELETE followed by an INSERT; that might
    # matter for row locking or MVCC, depending on isolation level and
    # locking strategies, and it's been said that it matters for
    # backend IO, especially on things like a large distributed SAN
    # (https://github.com/zodb/relstorage/issues/189). This is also
    # more similar to what PostgreSQL uses, possibly allowing more
    # query sharing (with a smarter query runner/interpreter).


    def visit_upsert_conflict_column(self, _column):
        self.emit_keyword('ON DUPLICATE KEY')

    def visit_upsert_conflict_update(self, update):
        self.emit_keyword('UPDATE')
        self.visit_csv(update.col_expressions)

    def visit_upsert_excluded_column(self, column):
        self.emit('VALUES(%s)' % (column.name,))


class MySQLDialect(DefaultDialect):

    datatype_map = dict(DefaultDialect.datatype_map)
    datatype_map.update({
        OID: 'BIGINT UNSIGNED',
        TID: 'BIGINT UNSIGNED',
        BinaryString: 'BLOB',
        #MD5: 'CHAR(32) CHARACTER SET ascii',
        State: 'LONGBLOB',

    })

    CONSTRAINT_AUTO_INCREMENT = 'AUTO_INCREMENT'
    STMT_TABLE_TRAILER = 'ENGINE = InnoDB'
    def compiler_class(self):
        return MySQLCompiler

class IterateFetchmanyMixin(object):
    """
    Mixin to cause us to fetch in batches using fetchmany().
    """
    sleep = None
    def __iter__(self):
        fetch = self.fetchmany
        sleep = self.sleep
        batch = fetch()
        while batch:
            for row in batch:
                yield row
            if sleep is not None:
                sleep() # pylint:disable=not-callable
            batch = fetch()

    next = __next__ = None

class AbstractMySQLDriver(AbstractModuleDriver):

    MY_SESSION_VARS = {
        # Don't try to decode pickle states as UTF-8 (or whatever the
        # environment is configured as); See
        # https://github.com/zodb/relstorage/issues/57. This varies
        # depending on Python 2/3 and which driver.
        #
        # In the past, we used 'SET names binary' for everything except
        # mysqlclient on Python 3, which set the character_set_results
        # only (because of an issue decoding column names (See
        # https://github.com/zodb/relstorage/issues/213)). But having
        # binary be the default encoding for string literals prevents
        # using the JSON type. So we took another look at connection
        # parameters and got things working with ``character_set_results``
        # everywhere.
        'character_set_results': 'binary',

        # Make the default timezone UTC. That way UTC_TIMESTAMP()
        # and UNIX_TIMESTAMP() and FROM_UNIXTIME are all self-consistent.
        # Subclasses can set to None if they don't need to do this.
        # Starting in 8.0.17 this is hintable using SET_VAR.
        "time_zone": "'+00:00'",

        # Certain identifiers are quoted in sql strings for compatibility
        # with other databases. This should go away as all plain strings
        # transition to modeled values. (Notable example: "transaction"
        # is a reserved word in sqlite.)
        'sql_mode': "CONCAT(@@sql_mode, ',ANSI_QUOTES')",
    }

    dialect = MySQLDialect()

    #: Holds the first statement that all connections need to run.
    #: Used as the value of the ``init_command`` argument to ``connect()`` for
    #: drivers that support it (mysqlclient, PyMySQL). This implementation fills
    #: in the session variables needed.
    _init_command = ''

    def __init__(self):
        super(AbstractMySQLDriver, self).__init__()

        kv = ["%s=%s" % (k, v) for k, v in self.MY_SESSION_VARS.items()]
        self._init_command = "SET " + ", ".join(kv)
        self.mysql_deadlock_exc = self.driver_module.OperationalError

    _server_side_cursor = None

    def cursor(self, conn, server_side=False):
        if server_side:
            assert self._server_side_cursor is not None
            cursor = conn.cursor(self._server_side_cursor)
            cursor.arraysize = self.cursor_arraysize
        else:
            cursor = super(AbstractMySQLDriver, self).cursor(conn, server_side=False)
        return cursor

    def connect(self, *args, **kwargs):
        if self._init_command:
            kwargs['init_command'] = self._init_command
        return super(AbstractMySQLDriver, self).connect(*args, **kwargs)

    def synchronize_cursor_for_rollback(self, cursor):
        """Does nothing."""

    def callproc_multi_result(self, cursor, proc,
                              args=(),
                              exit_critical_phase=False):
        """
        Some drivers need extra arguments to execute a statement that
        returns multiple results, and they don't all use the standard
        way to retrieve them, so use this.

        Returns a list of lists of rows, one list for each result set::

            [
              [(row in first),  ...],
              [(row in second), ...],
              ...
            ]

        The last item in the list is the result of the stored procedure
        itself.

        Note that, because 'CALL' potentially returns multiple result
        sets, there is potentially at least one extra database round
        trip involved when we call ``cursor.nextset()``. If the
        procedure being called is very short or returns only a single
        very small result, this may add substantial overhead.

        As of PyMySQL 0.9.3, mysql-connector-python 8.0.16 and MySQLdb
        (mysqlclient) 1.4.2 using libmysqlclient.21.so (from mysql8)
        or libmysqlclient.20 (mysql 5.7), all the drivers use the
        flags from the server to detect that there are no more results
        and turn ``nextset()`` into a simple flag check. So if the CALL
        only returns one result set (because the CALLed object doesn't
        return any of its own, i.e., it only has side-effects) there
        shouldn't be any penalty.

        :keyword bool exit_critical_phase: If true (*not* the default),
            then if the cursor's connection is in a critical phase,
            it will be exited *before* sending the ``CALL`` query.
            Do this when the called procedure will commit the transaction
            itself. There's no need to maintain elevated priority and block
            other greenlets while the query completes, we have nothing else
            that must be done to release locks and unblock other threads.

            Exiting before is fine; it's almost a certainty that we
            won't need to block to write to the socket.
        """
        if exit_critical_phase:
            self.exit_critical_phase(cursor.connection, cursor)

        cursor.execute('CALL ' + proc, args)

        multi_results = [cursor.fetchall()]
        while cursor.nextset():
            multi_results.append(cursor.fetchall())
        return multi_results

    def callproc_no_result(self, cursor, proc, args=()):
        """
        An optimization for calling stored procedures that don't
        produce any results, except for the null result of calling the
        procedure itself.
        """
        cursor.execute('CALL ' + proc, args)
        cursor.fetchall()

    def exit_critical_phase(self, connection, cursor):
        "Override if you implement critical phases."

    ERRCODE_DEADLOCK = 1213

    def exception_is_deadlock(self, exc):
        # OperationalError: (1213, 'Failed to get exclusive locks.')
        # This works for mysqldb and PyMySQL. Naturally it doesn't work for
        # mysqlconnector, where it is just a DatabaseError. Hence the indirection.
        return (
            isinstance(exc, self.mysql_deadlock_exc)
            and self.ERRCODE_DEADLOCK in exc.args
        )

implement_db_driver_options(
    __name__,
    'mysqlconnector', 'mysqldb', 'pymysql',
)
