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

from ..._abstract_drivers import AbstractModuleDriver
from ..._abstract_drivers import implement_db_driver_options
from ...sql import Compiler
from ...sql import DefaultDialect

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
        return '"{query}"'.format(query=query)

class MySQLDialect(DefaultDialect):

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
    MY_CHARSET_STMT = 'SET character_set_results = binary'

    # Make the default timezone UTC. That way UTC_TIMESTAMP()
    # and UNIX_TIMESTAMP() and FROM_UNIXTIME are all self-consistent.
    # Subclasses can set to None if they don't need to do this.
    # Starting in 8.0.17 this is hintable using SET_VAR.
    MY_TIMEZONE_STMT = "SET time_zone = '+00:00'"

    CURSOR_INIT_STMTS = (
        MY_CHARSET_STMT,
        MY_TIMEZONE_STMT,
    )

    # TODO: MySQLdb (mysqlclient) and PyMySQL support an
    # ``init_command`` argument to connect() that could be used to
    # automatically handle both these statements (``SET names binary,
    # time_zone = X``).

    _server_side_cursor = None
    _ignored_fetchall_on_set_exception = ()

    def _make_cursor(self, conn, server_side=False):
        if server_side:
            cursor = conn.cursor(self._server_side_cursor)
            cursor.arraysize = self.cursor_arraysize
        else:
            cursor = super(AbstractMySQLDriver, self).cursor(conn, server_side=False)
        return cursor

    def cursor(self, conn, server_side=False):
        cursor = self._make_cursor(conn, server_side=server_side)
        for stmt in self.CURSOR_INIT_STMTS:
            cursor.execute(stmt)
            try:
                cursor.fetchall()
            except self._ignored_fetchall_on_set_exception:
                pass
        return cursor

    def synchronize_cursor_for_rollback(self, cursor):
        """Does nothing."""

    def callproc_multi_result(self, cursor, proc, args=()):
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
        """
        cursor.execute('CALL ' + proc, args)

        multi_results = [cursor.fetchall()]
        while cursor.nextset():
            multi_results.append(cursor.fetchall())
        return multi_results


    dialect = MySQLDialect()


implement_db_driver_options(
    __name__,
    'mysqlconnector', 'mysqldb', 'pymysql',
)
