# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ..txncontrol import GenericTransactionControl


class Sqlite3TransactionControl(GenericTransactionControl):

    def commit_phase2(self, store_connection, txn, load_connection):
        # When committing, terminate the load connection's transaction now.
        # This allows any actions taken on commit, such as SQLite's auto-checkpoint,
        # to see a state where this reader is not holding open old MVCC resources.
        # See https://github.com/zodb/relstorage/issues/401
        load_connection.rollback_quietly()
        GenericTransactionControl.commit_phase2(self, store_connection, txn, load_connection)
