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

from ZODB.POSException import Unsupported
from ..txncontrol import GenericTransactionControl
from .._util import query_property
from .mover import to_prepared_queries

class _PostgreSQLTransactionControl(GenericTransactionControl):

    # See adapter.py for where this is prepared.
    # Either history preserving or not, it's the same.
    _get_tid_query = 'EXECUTE get_latest_tid'


    def __init__(self, connmanager, keep_history, driver):
        super(_PostgreSQLTransactionControl, self).__init__(
            connmanager,
            keep_history,
            driver.Binary
        )


class PostgreSQLTransactionControl(_PostgreSQLTransactionControl):

    # (tid, packed, username, description, extension)
    _add_transaction_query = 'EXECUTE add_transaction(%s, %s, %s, %s, %s)'

    _prepare_add_transaction_queries = to_prepared_queries(
        'add_transaction',
        [
            GenericTransactionControl._add_transaction_query,
            Unsupported("No transactions in HF mode"),
        ],
        ('BIGINT', 'BOOLEAN', 'BYTEA', 'BYTEA', 'BYTEA')
    )

    _prepare_add_transaction_query = query_property('_prepare_add_transaction')


class PG8000TransactionControl(_PostgreSQLTransactionControl):
    # We can't handle the parameterized prepared statements.
    pass
