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
psycopg2cffi IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from .psycopg2 import Psycopg2Driver

__all__ = [
    'Psycopg2cffiDriver'
]

class Psycopg2cffiDriver(Psycopg2Driver):
    __name__ = 'psycopg2cffi'
    MODULE_NAME = __name__
    PRIORITY = 2
    PRIORITY_PYPY = 1
