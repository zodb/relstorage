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

from zope.interface import implementer

from .interfaces import IScriptRunner

log = logging.getLogger(__name__)


@implementer(IScriptRunner)
class ScriptRunner(object):

    # script_vars contains replacements for parts of scripts.
    # These are correct for PostgreSQL and MySQL but not for Oracle.
    # The primary purpose of this originally was to compensate for different
    # parameter passing styles in the DB-API. It has also been used for some
    # SQL syntax/datatype differences.
    #
    # TODO: Make a clean separation between syntax, and DB-API substitutions.
    # Use `script_vars` (or a better name) for parameters, and
    # `format_vars` (or a better name) for syntax? Or just wrap this up in a
    # query_property(..., formatted=True)?
    script_vars = {
        'TRUE':         'TRUE',
        'FALSE':        'FALSE',
        'TRUNCATE':     'TRUNCATE TABLE',
        'oid':          '%(oid)s',
        'tid':          '%(tid)s',
        'pack_tid':     '%(pack_tid)s',
        'undo_tid':     '%(undo_tid)s',
        'self_tid':     '%(self_tid)s',
        'min_tid':      '%(min_tid)s',
        'max_tid':      '%(max_tid)s',
        'INNER_ORDER_BY': 'ORDER BY zoid',
    }

    format_vars = {
    }

    def new_instance(self):
        return type(self)()

    def with_format_vars(self, **new_vars):
        inst = self.new_instance()
        inst.format_vars = dict(self.format_vars)
        inst.format_vars.update(new_vars)
        return inst

    def run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        params should be either an empty tuple (no parameters) or
        a map.

        The input statement is generic and needs to be transformed
        into a database-specific statement.
        """
        __traceback_info__ = generic_stmt, self.format_vars
        stmt = generic_stmt.format(**self.format_vars)
        if '%(' in stmt and ')s' in stmt:
            __traceback_info__ = stmt, self.script_vars
            stmt = stmt % self.script_vars
        __traceback_info__ = stmt
        try:
            cursor.execute(stmt, generic_params)
        except:
            log.warning("script statement failed: %r; parameters: %r",
                        stmt, generic_params)
            raise

    def run_script(self, cursor, script, params=()):
        """Execute a series of statements in the database.

        params should be either an empty tuple (no parameters) or
        a map.

        The statements are transformed by run_script_stmt
        before execution.
        """
        lines = []
        for line in script.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            if line.endswith(';'):
                line = line[:-1]
                lines.append(line)
                stmt = '\n'.join(lines)
                self.run_script_stmt(cursor, stmt, params)
                lines = []
            else:
                lines.append(line)
        if lines:
            stmt = '\n'.join(lines)
            self.run_script_stmt(cursor, stmt, params)

    def run_many(self, cursor, stmt, items):
        """Execute a statement repeatedly.  Items should be a list of tuples.

        stmt should use '%s' parameter format.
        """
        cursor.executemany(stmt, items)
