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
"""Pack/Undo implementations.
"""
from __future__ import absolute_import

from ..packundo import HistoryFreePackUndo
from ..packundo import HistoryPreservingPackUndo
from .dialect import OracleDialect


def _oracle_fetchmany(self, cursor): # pylint:disable=unused-argument
    # We can't safely fetch many rows at once without
    # getting 'ProgrammingError: LOB variable no longer valid after subsequent fetch'
    # See https://github.com/zodb/relstorage/issues/30
    return cursor



# Oracle fails to notice that pack_object is now filled and chooses
# the wrong execution plan, completely killing this query on large
# RelStorage databases, unless these hints are included.
_oracle_traverse_graph_optimizer_hint = "/*+ FULL(object_ref) FULL(pack_object) */"


class OracleHistoryPreservingPackUndo(HistoryPreservingPackUndo):

    dialect = OracleDialect()

    _script_choose_pack_transaction = """
        SELECT MAX(tid)
        FROM transaction
        WHERE tid > 0
            AND tid <= %(tid)s
            AND packed = 'N'
        """

    _script_create_temp_pack_visit = None
    _script_create_temp_undo = None
    _script_reset_temp_undo = "DELETE FROM temp_undo"

    _script_find_pack_tid = """
        SELECT MAX(keep_tid)
        FROM pack_object
        """

    _script_transaction_has_data = """
        SELECT DISTINCT tid
        FROM object_state
        WHERE tid = %(tid)s
        """

    _script_delete_empty_transactions_batch = """
        DELETE FROM transaction
        WHERE packed = %(TRUE)s
          AND empty = %(TRUE)s
          AND rownum <= 1000
        """

    # XXX: This is necessary
    # (https://github.com/zodb/relstorage/issues/135), but the HP
    # tests don't fail without it.
    _fetchmany = _oracle_fetchmany

    _traverse_graph_optimizer_hint = _oracle_traverse_graph_optimizer_hint


class OracleHistoryFreePackUndo(HistoryFreePackUndo):

    dialect = OracleDialect()

    _script_choose_pack_transaction = """
        SELECT MAX(tid)
        FROM object_state
        WHERE tid > 0
            AND tid <= %(tid)s
        """

    _script_create_temp_pack_visit = None

    _fetchmany = _oracle_fetchmany
    _traverse_graph_optimizer_hint = _oracle_traverse_graph_optimizer_hint


def _check_limit(c):
    for b in c.mro():
        for k, v in vars(b).items():
            if hasattr(v, 'bind'):
                v = v.bind(c.dialect)
            if 'LIMIT ' in str(v):
                raise TypeError("Forgot to override %s in %s" % (k, c))

_check_limit(OracleHistoryPreservingPackUndo)
_check_limit(OracleHistoryFreePackUndo)
