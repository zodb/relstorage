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

import six
import abc


from zope.interface import implementer

from .interfaces import ITransactionControl
from ._util import query_property
from ._util import noop_when_history_free

@six.add_metaclass(abc.ABCMeta)
class AbstractTransactionControl(object):
    """Abstract base class"""

    # pylint:disable=unused-argument

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
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        conn.rollback()

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

    _get_tid_queries = (
        "SELECT MAX(tid) FROM transaction",
        "SELECT MAX(tid) FROM object_state"
    )
    _get_tid_query = query_property('_get_tid')

    def __init__(self, keep_history, Binary): # noqa
        self.keep_history = keep_history
        self.Binary = Binary

    def get_tid(self, cursor):
        cursor.execute(self._get_tid_query)
        row = cursor.fetchone()
        if not row:
            # nothing has been stored yet
            return 0

        tid = row[0]
        return tid if tid is not None else 0

    @noop_when_history_free
    def add_transaction(self, cursor, tid, username, description, extension,
                        packed=False):
        stmt = """
        INSERT INTO transaction
            (tid, packed, username, description, extension)
        VALUES (%s, %s, %s, %s, %s)
        """
        binary = self.Binary
        cursor.execute(stmt, (
            tid, packed, binary(username),
            binary(description), binary(extension)))
