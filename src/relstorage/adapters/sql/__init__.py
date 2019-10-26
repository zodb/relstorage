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

"""
A small abstraction layer for SQL queries.

Features:

    - Simple, readable syntax for writing queries.

    - Automatic switching between history-free and history-preserving
      schemas.

    - Support for automatically preparing statements (depending on the
      driver; some allow parameters, some do not, for example)

    - Always use bind parameters

    - Take care of minor database syntax issues.

This is inspired by the SQLAlchemy Core, but we don't use it because
we don't want to take a dependency that can conflict with applications
using RelStorage.
"""
# pylint:disable=too-many-lines

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .schema import Table
from .schema import TemporaryTable
from .schema import HistoryVariantTable
from .schema import Column
from .schema import ColumnResolvingProxy
from .schema import ColumnExpression
from .schema import View

from .dialect import DefaultDialect
from .dialect import Compiler

from .types import OID
from .types import TID
from .types import State
from .types import Boolean
from .types import BinaryString
from .types import Char

from .functions import func

it = ColumnResolvingProxy()

__all__ = [
    # Schema elements
    'Table',
    'Column',
    "TemporaryTable",
    "HistoryVariantTable",
    "View",

    # Query helpers
    'it',
    "ColumnExpression",

    # Dialect
    "DefaultDialect",
    "Compiler",

    # Types
    "OID",
    "TID",
    "State",
    "Boolean",
    "BinaryString",
    "Char",

    # Functions
    "func",

]
