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

import abc

from zope.interface import implementer

from .._compat import ABC
from ._util import noop_when_history_free

from .schema import Schema
from .interfaces import ITransactionControl


class AbstractTransactionControl(ABC):
    """Abstract base class"""

    # pylint:disable=unused-argument

    def __init__(self, connmanager):
        self.connmanager = connmanager

    def commit_phase1(self, conn, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        The transaction name must not be None.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        return '-'

    def commit_phase2(self, conn, cursor, txn):
        """Final transaction commit.

        txn is the name returned by commit_phase1.
        """
        conn.commit()

    def abort(self, conn, cursor, txn=None):
        """
        Abort the commit. If txn is not None, phase 1 is also aborted.

        The connection is rolled back quietly using :meth:`~IConnectionManager.rollback_quietly`
        and the boolean result of that function is returned.
        """
        return self.connmanager.rollback_quietly(conn, cursor)

    @abc.abstractmethod
    def get_tid(self, cursor):
        "Returns the most recent tid"
        raise NotImplementedError()

    @abc.abstractmethod
    def add_transaction(self, cursor, tid, username, description, extension,
                        packed=False):
        """Add a transaction"""
        raise NotImplementedError()

@implementer(ITransactionControl)
class GenericTransactionControl(AbstractTransactionControl):
    """
    A :class:`ITransactionControl` implementation that works for history-free
    and history-preserving storages that share a common syntax.
    """

    def __init__(self, connmanager, poller, keep_history, Binary):
        super(GenericTransactionControl, self).__init__(connmanager)
        self.poller = poller
        self.keep_history = keep_history
        self.Binary = Binary

    def get_tid(self, cursor):
        self.poller.poll_query.execute(cursor)
        row = cursor.fetchall()
        if not row:
            # nothing has been stored yet
            return 0

        tid = row[0][0]
        return tid if tid is not None else 0

    _add_transaction_query = Schema.transaction.insert(
        Schema.transaction.c.tid,
        Schema.transaction.c.packed,
        Schema.transaction.c.username,
        Schema.transaction.c.description,
        Schema.transaction.c.extension
    ).prepared()

    @noop_when_history_free
    def add_transaction(self, cursor, tid, username, description, extension,
                        packed=False):
        binary = self.Binary
        self._add_transaction_query.execute(
            cursor,
            (
                tid, packed, binary(username),
                binary(description), binary(extension)
            )
        )
