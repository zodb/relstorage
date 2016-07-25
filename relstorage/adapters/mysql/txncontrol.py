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

from zope.interface import implementer

from ..interfaces import ITransactionControl
from ..txncontrol import AbstractTransactionControl

@implementer(ITransactionControl)
class MySQLTransactionControl(AbstractTransactionControl):

    def __init__(self, keep_history, Binary):
        self.keep_history = keep_history
        self.Binary = Binary

    def get_tid(self, cursor):
        """Returns the most recent tid."""
        if self.keep_history:
            stmt = """
            SELECT tid
            FROM transaction
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
        else:
            stmt = """
            SELECT tid
            FROM object_state
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
            if not cursor.rowcount:
                # nothing has been stored yet
                return 0

        assert cursor.rowcount == 1
        return cursor.fetchone()[0]

    def add_transaction(self, cursor, tid, username, description, extension,
                        packed=False):
        """Add a transaction."""
        if self.keep_history:
            stmt = """
            INSERT INTO transaction
                (tid, packed, username, description, extension)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(stmt, (
                tid, packed, self.Binary(username),
                self.Binary(description), self.Binary(extension)))
