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
"""TransactionControl implementations"""

from __future__ import absolute_import

import logging

from ..txncontrol import GenericTransactionControl
from ..txncontrol import noop_when_history_free

log = logging.getLogger(__name__)


class OracleTransactionControl(GenericTransactionControl):

    def __init__(self, connmanager, poller, keep_history, Binary, twophase):
        GenericTransactionControl.__init__(self, connmanager, poller, keep_history, Binary)
        self.twophase = twophase

    def commit_phase1(self, store_connection, tid):
        """Begin a commit.  Returns the transaction name.

        The transaction name must not be None.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        if self.twophase:
            store_connection.connection.prepare()
        return '-'

    @noop_when_history_free
    def add_transaction(self, cursor, tid, username, description, extension,
                        packed=False):
        stmt = """
        INSERT INTO transaction
            (tid, packed, username, description, extension)
        VALUES (:1, :2, :3, :4, :5)
        """
        max_desc_len = 2000
        if len(description) > max_desc_len:
            log.warning('Trimming description of transaction %s '
                        'to %d characters', tid, max_desc_len)
            description = description[:max_desc_len]
        cursor.execute(stmt, (
            tid, packed and 'Y' or 'N', self.Binary(username),
            self.Binary(description), self.Binary(extension)))
