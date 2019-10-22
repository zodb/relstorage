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


from ...sql.tests import test_sql
from ..drivers import MySQLDialect

class TestMySQLDialect(test_sql.TestUpsert):
    keep_history = False
    dialect = MySQLDialect()

    insert_or_replace = (
        'INSERT INTO object_state(zoid, state, tid, state_size) '
        'VALUES (%s, %s, %s, %s) '
        'ON DUPLICATE KEY UPDATE '
        'state = VALUES(state), tid = VALUES(tid), '
        'state_size = VALUES(state_size)'
    )

    insert_or_replace_subquery = (
        'INSERT INTO object_state(zoid, tid, state, state_size) '
        'SELECT zoid, %s, state, COALESCE(LENGTH(state), 0) FROM temp_store '
        'ORDER BY zoid '
        'ON DUPLICATE KEY UPDATE '
        'state = VALUES(state), tid = VALUES(tid), '
        'state_size = VALUES(state_size)'
    )

    upsert_unconstrained_subquery = (
        'INSERT INTO object_state(zoid, tid, state, state_size) '
        'SELECT zoid, %s, state, COALESCE(LENGTH(state), 0) FROM temp_store '
        'ON DUPLICATE KEY UPDATE state = VALUES(state), '
        'tid = VALUES(tid), state_size = VALUES(state_size)'
    )
