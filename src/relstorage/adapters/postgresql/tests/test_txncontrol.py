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

from relstorage.adapters.tests import test_txncontrol


class TestPostgreSQLTransactionControl(test_txncontrol.TestTransactionControl):

    def _getClass(self):
        from ..txncontrol import PostgreSQLTransactionControl
        return PostgreSQLTransactionControl

    def _get_hf_tid_query(self):
        return 'EXECUTE get_latest_tid'

    _get_hp_tid_query = _get_hf_tid_query
