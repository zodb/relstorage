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
from ..scriptrunner import ScriptRunner

import logging
import re
from relstorage._compat import iteritems
from relstorage._compat import intern

log = logging.getLogger(__name__)

_stmt_cache = {}

def format_to_named(stmt):
    """
    Convert '%s' pyformat strings to :n numbered
    strings. Intended only for static strings.
    """
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
        cursor.executemany(format_to_named(stmt), items)


class TrackingMap(object):
    """Provides values for keys while tracking which keys are accessed."""

    def __init__(self, source):
        self.source = source
        self.used = set()

    def __getitem__(self, key):
        self.used.add(key)
        return self.source[key]
