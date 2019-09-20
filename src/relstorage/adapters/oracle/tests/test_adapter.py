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

from zope.interface import alsoProvides

from relstorage.tests import MockDriver
from ..adapter import OracleAdapter as BaseAdapter

from ... import interfaces
from ...tests import test_adapter

class Adapter(BaseAdapter):
    # We don't usually have cx_oracle installed, so
    # fake it.

    def _select_driver(self, options=None):
        d = MockDriver()
        d.connect = lambda *args, **kwargs: None
        d.cursor = lambda conn, server_side=False: None
        d.NUMBER = None
        d.BLOB = None
        d.BINARY = None
        d.STRING = None
        d.Binary = None
        d.use_replica_exceptions = ()
        d.binary_column_as_state_type = lambda b: b
        d.binary_column_as_bytes = lambda b: b
        d.__name__ = 'cx_Oracle'
        alsoProvides(d, interfaces.IDBDriver)
        return d

class TestAdapter(test_adapter.AdapterTestBase):

    def _makeOne(self, options):
        return Adapter(options)
