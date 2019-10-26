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

General Design Notes
====================

Sqlite3 ``INTEGER PRIMARY KEY`` values are always 64-bit unsigned
integers. If not given, they default to an unused integer (typically
incrementing). If the ``AUTOINCREMENT`` keyword is provided, then they
prevent reuse of values even after they are deleted via an auxiliary
table, which adds overhead.

Regular tables are structured as a B*Tree with internal nodes
containing only the ROWID (a 64-bit integer) and all row content
stored in the leaf pages.

If a regular table has an ``INTEGER PRIMARY KEY`` column (it must be
exactly ``INTEGER``, not ``INT``) then that column becomes an alias
for the ROWID. It is enforced to be an actual integer (unlike all
other columnns that can store arbitrary data). Accesses by ROWID are
very efficient. That's a natural for history-free ``OBJECT_STATE`` and
history-preserving ``TRANSACTION`` tables.

If a regular table has a primary key of any other type or of multiple
columns, then it requires two BTrees: one to store the rows, and a
second ``UNIQUE`` index table to implement the primary key and map it
back to the ROWID of the data BTree.

It is possible to create tables ``WITHOUT ROWID``. These are normal
BTrees, with all the data for a row stored in the node where it is
found, whether leaf or interior. For multi-column primary keys, this
can be very attractive because it avoids duplicate storage of the
primary key, and allows direct access to the rest of the row given the
primary key instead of requiring redirection throgh the index.

However, because these tables store data inline, it is not recommended
to use them if the row could be arbitrarily large (more than 200
bytes) because of the effect that has on the BTree and number of pages
needed to traverse it. That makes it a poor fit for history-preserving
``OBJECT_STATE``.

References:

    - https://www.sqlite.org/lang_createtable.html#rowid

    - https://www.sqlite.org/withoutrowid.html

OIDs
====

Our ``object_state`` table will use a regular ``INTEGER PRIMARY KEY``
table. Our ``new_oids`` table COULD use one defined to
``AUTOINCREMENT``. However, that complicates copying transactions, the
implementation of :meth:`IOIDAllocator.set_min_oid`, and introduces
extra ``DELETE`` operations to ensure that the table doesn't grow
forever, so we handle the incrementing ourself.

We keep ``new_oids`` in a separate database file beside the primary
database file. This is because *any* write to a database causes an
exclusive write lock to be taken. Allocating new oids can happen at
essentially any time, even outside of the normal two-phase commit
sequence (thanks to Connection.add()) and we cannot allow the main
database to be locked like that.

Locks
=====

Sqlite3 only supports database-wide write locks. For details on when
and how they are taken and managed, see connmanager.py and locker.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
