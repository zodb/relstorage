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
from ..schema import Schema

class _LockStmt(object):
    # 8.0 supports 'FOR SHARE' but before that we have
    # this.
    _lock_for_share = 'LOCK IN SHARE MODE'

class MySQLHistoryPreservingPackUndo(_LockStmt, HistoryPreservingPackUndo):

    # Previously we needed to work around a MySQL performance bug by
    # avoiding an expensive subquery.
    #
    # See:
    #      http://mail.zope.org/pipermail/zodb-dev/2008-May/011880.html
    #      http://bugs.mysql.com/bug.php?id=28257
    #
    # However, this was fixed in 5.6.
    _script_create_temp_pack_visit = """
        CREATE TEMPORARY TABLE temp_pack_visit (
            zoid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            keep_tid BIGINT UNSIGNED NOT NULL
        );
        CREATE INDEX temp_pack_keep_tid ON temp_pack_visit (keep_tid);
        """

    # It was once purported here that "MySQL optimizes deletion far
    # better when using a (USING) join syntax", a so-called
    # "multi-table" delete; indeed, as of 5.7 and 8.0, the documentation still maintains
    # that certain optimizations aren't used in DELETE with subqueries and councils
    # USING instead. However: "You cannot use ORDER BY or LIMIT
    # in a multiple-table DELETE" and "If you use a multiple-table
    # DELETE statement involving InnoDB tables for which there are
    # foreign key constraints, the MySQL optimizer might process
    # tables in an order that differs from that of their parent/child
    # relationship. In this case, the statement fails and rolls back.
    # Instead, you should delete from a single table and rely on the
    # ON DELETE capabilities that InnoDB provides to cause the other
    # tables to be modified accordingly."
    # (https://dev.mysql.com/doc/refman/5.7/en/delete.html; The same
    # goes for 8.0). In 8.0 we could potentially use a WITH clause.
    #
    # ORDER BY is important to lock rows in the same order as
    # transaction commits do, so that we don't deadlock with
    # commits, which also lock rows.
    _script_pack_current_object = """
    DELETE FROM current_object
    WHERE zoid IN (
        SELECT zoid
        FROM pack_state
        WHERE pack_state.tid = %(tid)s
    )
    AND tid = %(tid)s
    ORDER BY zoid
    """

    _script_pack_object_state = """
    DELETE FROM object_state
    WHERE zoid IN (
        SELECT zoid
        FROM pack_state
        WHERE pack_state.tid = %(tid)s
    )
    AND tid = %(tid)s
    ORDER BY zoid
    """

    _script_pack_object_ref = """
        DELETE FROM object_refs_added
        USING object_refs_added
            JOIN transaction USING (tid)
        WHERE transaction.is_empty = true;

        DELETE FROM object_ref
        USING object_ref
            JOIN transaction USING (tid)
        WHERE transaction.is_empty = true
        """

    _script_create_temp_undo = """
        CREATE TEMPORARY TABLE temp_undo (
            zoid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            prev_tid BIGINT UNSIGNED NOT NULL
        );
    """

    # pylint:disable=singleton-comparison
    _delete_empty_transactions_batch_query = Schema.transaction.delete(
    ).where(
        Schema.transaction.c.packed == True
    ).and_(
        Schema.transaction.c.is_empty == True
    ).limit(1000)


class MySQLHistoryFreePackUndo(_LockStmt, HistoryFreePackUndo):
    pass
