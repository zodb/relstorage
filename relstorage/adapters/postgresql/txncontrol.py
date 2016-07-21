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

from relstorage.adapters.interfaces import ITransactionControl
from zope.interface import implementer

from ..txncontrol import AbstractTransactionControl


@implementer(ITransactionControl)
class PostgreSQLTransactionControl(AbstractTransactionControl):

    def __init__(self, keep_history, driver):
        self.keep_history = keep_history
        self._Binary = driver.Binary

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
            VALUES (%s, %s,
                %s, %s,
                %s)
            """
            Binary = self._Binary
            cursor.execute(stmt, (tid, packed,
                                  Binary(username), Binary(description),
                                  Binary(extension)))
