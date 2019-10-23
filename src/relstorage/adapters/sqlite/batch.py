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

from ..batch import RowBatcher

class Sqlite3RowBatcher(RowBatcher):
    # The batch size depends on how many params a statement can
    # have; if we go too big we get OperationalError: too many SQL
    # variables. The default allowed is 999.
    # Note that the multiple-value syntax was added in
    # 3.7.11, 2012-03-20.

    bind_limit = 998

    # sqlite only supports ? as a param.
    delete_placeholder = '?'
    insert_placeholder = '?'
