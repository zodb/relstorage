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
from __future__ import print_function

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

    def commit_phase1(self, store_connection, tid):
        return '-'

    def commit_phase2(self, store_connection, txn, load_connection):
        store_connection.commit()

    def abort(self, store_connection, txn=None):
        """
        Abort the commit. If txn is not None, phase 1 is also aborted.

        The connection is rolled back quietly using :meth:`~IConnectionManager.rollback_quietly`
        and the boolean result of that function is returned.
        """
        return store_connection.rollback_quietly()

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
        return self.poller.get_current_tid(cursor)

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

    _delete_transaction_query = Schema.transaction.delete(
    ).where(
        Schema.transaction.c.tid == Schema.transaction.orderedbindparam()
    )

    @noop_when_history_free
    def delete_transaction(self, cursor, tid):
        self._delete_transaction_query.execute(
            cursor,
            (tid,)
        )
