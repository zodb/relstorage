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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ..scriptrunner import ScriptRunner

class Sqlite3ScriptRunner(ScriptRunner):
    script_vars = dict(ScriptRunner.script_vars)
    script_vars.update({
        'TRUNCATE': 'DELETE FROM',
        'FALSE': '0',
        'TRUE': '1'
    })

    def run_script_stmt(self, cursor, generic_stmt, generic_params=()):
        if generic_params and isinstance(generic_params, dict):
            for key in generic_params:
                generic_stmt = generic_stmt.replace('%%(%s)s' % (key,), ':%s' % (key,))

        return ScriptRunner.run_script_stmt(self, cursor, generic_stmt, generic_params)
