##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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

from __future__ import absolute_import

import logging
from relstorage._compat import iteritems

from ..scriptrunner import ScriptRunner

log = logging.getLogger(__name__)

_stmt_cache = {}

def _format_to_named(stmt):
    """
    Convert '%s' pyformat strings to :n numbered
    strings. Intended only for static strings.

    This is legacy. Replace strings that use this with SQL statements
    constructed from the schema.
    """
    import re
    from relstorage._compat import intern

    try:
        return _stmt_cache[stmt]
    except KeyError:
        matches = []

        def replace(_match):
            matches.append(None)
            return ':%d' % len(matches)
        new_stmt = intern(re.sub('%s', replace, stmt))
        _stmt_cache[stmt] = new_stmt

        return new_stmt

class OracleScriptRunner(ScriptRunner):

    script_vars = dict(ScriptRunner.script_vars)
    script_vars.update({
        'TRUE':         "'Y'",
        'FALSE':        "'N'",
        'oid':          ':oid',
        'tid':          ':tid',
        'pack_tid':     ':pack_tid',
        'undo_tid':     ':undo_tid',
        'self_tid':     ':self_tid',
        'min_tid':      ':min_tid',
        'max_tid':      ':max_tid',
        # Oracle won't accept ORDER BY clauses inside
        # the subquery of an IN
        'INNER_ORDER_BY': ''
    })

    def run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        params should be either an empty tuple (no parameters) or
        a map.
        """
        generic_stmt = generic_stmt.format(**self.format_vars)
        # We can't quote "transaction", but we have to for sqlite.
        generic_stmt = generic_stmt.replace(' "transaction"', ' transaction')

        if generic_params and isinstance(generic_params, tuple):
            generic_stmt = _format_to_named(generic_stmt) # Unnamed params become numbered.
        if generic_params and isinstance(generic_params, dict):
            # Oracle raises ORA-01036 if the parameter map contains extra keys,
            # so filter out any unused parameters.
            tracker = TrackingMap(self.script_vars)
            stmt = generic_stmt % tracker
            used = tracker.used
            params = {}
            for k, v in iteritems(generic_params):
                if k in used:
                    params[k] = v

        else:
            stmt = generic_stmt % self.script_vars
            params = ()

        if generic_params and isinstance(generic_params, tuple):
            params = generic_params
        __traceback_info__ = stmt

        try:
            cursor.execute(stmt, params)
        except:
            log.warning("script statement failed: %r; parameters: %r",
                        stmt, params)
            raise

    def run_many(self, cursor, stmt, items):
        """Execute a statement repeatedly.  Items should be a list of tuples.

        stmt should use '%s' parameter format.
        """
        cursor.executemany(_format_to_named(stmt), items)


class TrackingMap(object):
    """Provides values for keys while tracking which keys are accessed."""

    def __init__(self, source):
        self.source = source
        self.used = set()

    def __getitem__(self, key):
        self.used.add(key)
        return self.source[key]


class CXOracleScriptRunner(OracleScriptRunner):

    def __init__(self, driver):
        self.driver = driver

    def new_instance(self):
        return type(self)(self.driver)


    def _read_lob(self, value):
        """Handle an Oracle LOB by returning its byte stream.

        Returns other objects unchanged.
        """
        if isinstance(value, self.driver.LOB):
            return value.read()
        return value

    def run_lob_stmt(self, cursor, stmt, args=(), default=None):
        """
        Execute a statement and return one row with all LOBs
        inline.

        Returns the value of the default parameter if the result was
        empty.

        The statement can either be a string, or a CompiledQuery
        object.
        """

        try:
            if hasattr(stmt, 'execute'):
                stmt.execute(cursor, args)
            else:
                cursor.execute(stmt, args)
            rows = cursor.fetchall()
        except self.driver.DatabaseError as e:
            # ORA-01406: fetched column value was truncated
            error = e.args[0]

            if ((isinstance(error, str) and not error.endswith(' 1406'))
                    or error.code != 1406):
                raise
            # Execute the query, but alter it slightly without
            # changing its meaning, so that the query cache
            # will see it as a statement that has to be compiled
            # with different output type parameters.
            oth = cursor.connection.outputtypehandler
            cursor.connection.outputtypehandler = None
            try:
                cursor.execute(stmt + ' ', args)
                rows = [
                    tuple(self._read_lob(x) for x in row)
                    for row in cursor
                ]
            finally:
                cursor.connection.outputtypehandler = oth

        assert len(rows) in (0, 1)
        return rows[0] if rows else default
