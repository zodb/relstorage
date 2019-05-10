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

from ..packundo import HistoryPreservingPackUndo
from ..packundo import HistoryFreePackUndo

class MySQLHistoryPreservingPackUndo(HistoryPreservingPackUndo):

    # Work around a MySQL performance bug by avoiding an expensive subquery.
    # See: http://mail.zope.org/pipermail/zodb-dev/2008-May/011880.html
    #      http://bugs.mysql.com/bug.php?id=28257
    _script_create_temp_pack_visit = """
        CREATE TEMPORARY TABLE temp_pack_visit (
            zoid BIGINT UNSIGNED NOT NULL,
            keep_tid BIGINT UNSIGNED NOT NULL
        );
        CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid);
        CREATE INDEX temp_pack_keep_tid ON temp_pack_visit (keep_tid);
        """

    # MySQL optimizes deletion far better when using a join syntax.
    _script_pack_current_object = """
        DELETE FROM current_object
        USING current_object
            JOIN pack_state USING (zoid, tid)
        WHERE current_object.tid = %(tid)s
        """

    _script_pack_object_state = """
        DELETE FROM object_state
        USING object_state
            JOIN pack_state USING (zoid, tid)
        WHERE object_state.tid = %(tid)s
        """

    _script_pack_object_ref = """
        DELETE FROM object_refs_added
        USING object_refs_added
            JOIN transaction USING (tid)
        WHERE transaction.empty = true;

        DELETE FROM object_ref
        USING object_ref
            JOIN transaction USING (tid)
        WHERE transaction.empty = true
        """

    _script_create_temp_undo = """
        CREATE TEMPORARY TABLE temp_undo (
            zoid BIGINT UNSIGNED NOT NULL,
            prev_tid BIGINT UNSIGNED NOT NULL
        );
        CREATE UNIQUE INDEX temp_undo_zoid ON temp_undo (zoid)
        """

    _script_delete_empty_transactions_batch = """
        DELETE FROM transaction
        WHERE packed = %(TRUE)s
          AND empty = %(TRUE)s
        LIMIT 1000
        """

class MySQLHistoryFreePackUndo(HistoryFreePackUndo):

    _script_create_temp_pack_visit = """
        CREATE TEMPORARY TABLE temp_pack_visit (
            zoid BIGINT UNSIGNED NOT NULL,
            keep_tid BIGINT UNSIGNED NOT NULL
        );
        CREATE UNIQUE INDEX temp_pack_visit_zoid ON temp_pack_visit (zoid);
        """
