# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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
"""
MySQLdb IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage.adapters._abstract_drivers import AbstractModuleDriver
from relstorage.adapters.interfaces import IDBDriver

__all__ = [
    'MySQLdbDriver',
]


@implementer(IDBDriver)
class MySQLdbDriver(AbstractModuleDriver):
    __name__ = 'MySQLdb'

    MODULE_NAME = 'MySQLdb'
    PRIORITY = 1
    PRIORITY_PYPY = 3
