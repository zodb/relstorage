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
from relstorage.adapters.interfaces import IScriptRunner
from zope.interface import implementer
import logging
import re
from relstorage._compat import iteritems
from relstorage._compat import intern

log = logging.getLogger(__name__)


@implementer(IScriptRunner)
class ScriptRunner(object):

    # script_vars contains replacements for parts of scripts.
    # These are correct for PostgreSQL and MySQL but not for Oracle.
    script_vars = {
        'TRUE':         'TRUE',
        'FALSE':        'FALSE',
        'TRUNCATE':     'TRUNCATE',
        'oid':          '%(oid)s',
        'tid':          '%(tid)s',
        'pack_tid':     '%(pack_tid)s',
        'undo_tid':     '%(undo_tid)s',
        'self_tid':     '%(self_tid)s',
        'min_tid':      '%(min_tid)s',
        'max_tid':      '%(max_tid)s',
    }

    def run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        params should be either an empty tuple (no parameters) or
        a map.

        The input statement is generic and needs to be transformed
        into a database-specific statement.
        """
        stmt = generic_stmt % self.script_vars
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


class OracleScriptRunner(ScriptRunner):

    script_vars = {
        'TRUE':         "'Y'",
        'FALSE':        "'N'",
        'TRUNCATE':     'TRUNCATE TABLE',
        'oid':          ':oid',
        'tid':          ':tid',
        'pack_tid':     ':pack_tid',
        'undo_tid':     ':undo_tid',
        'self_tid':     ':self_tid',
        'min_tid':      ':min_tid',
        'max_tid':      ':max_tid',
    }

    def run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        params should be either an empty tuple (no parameters) or
        a map.
        """
        if generic_params:
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
        # replace '%s' with ':n'
        matches = []
        def replace(_match):
            matches.append(None)
            return ':%d' % len(matches)
        stmt = intern(re.sub('%s', replace, stmt))

        cursor.executemany(stmt, items)


class TrackingMap(object):
    """Provides values for keys while tracking which keys are accessed."""

    def __init__(self, source):
        self.source = source
        self.used = set()

    def __getitem__(self, key):
        self.used.add(key)
        return self.source[key]
