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
sqlite3 adapter for RelStorage.

OIDs
====

Sqlite3 ``INTEGER PRIMARY KEY`` values are always 64-bit unsigned
integers (they are the ``ROWID`` for the table). If not given, they
default to an unused integer (typically incrementing). If the
``AUTOINCREMENT`` keyword is provided, then they prevent reuse of
values even after they are deleted via an auxiliary table, which adds
overhead.

Our ``object_state`` table will use a regular ``INTEGER PRIMARY KEY``
table, and our ``new_oids`` table will use one that ``AUTOINCREMENT``.

We keep ``new_oids`` in a separate database file beside the primary database
file. This is because *any* write to a database causes an exclusive write lock to be
taken. Allocating new oids can happen at essentially any time, even outside of the normal
two-phase commit sequence (thanks to Connection.add()) and we cannot allow the
main database to be locked like that.

Locks
=====

Sqlite3 only supports database-wide locks.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
