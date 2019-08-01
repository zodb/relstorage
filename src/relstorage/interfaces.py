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
Interfaces for top-level RelStorage components.

These interfaces aren't meant to be considered public, they exist to
serve as documentation and for validation of RelStorage internals.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ZODB.interfaces

# pylint:disable=inherit-non-class, no-self-argument, no-method-argument
# pylint:disable=too-many-ancestors



class IRelStorage(
        ZODB.interfaces.IMVCCAfterCompletionStorage, # IMVCCStorage <- IStorage
        ZODB.interfaces.IMultiCommitStorage,  # mandatory in ZODB5, returns tid from tpc_finish.
        ZODB.interfaces.IStorageRestoreable,  # tpc_begin(tid=) and restore()
        ZODB.interfaces.IStorageIteration,    # iterator()
        ZODB.interfaces.ReadVerifyingStorage, # checkCurrentSerialInTransaction()
):
    """
    The relational storage backend.

    These objects are not thread-safe.

    Instances may optionally implement some other interfaces,
    depending on their configuration. These include:

    - :class:`ZODB.interfaces.IBlobStorage` and :class:`ZODB.interfaces.IBlobStorage`
      if a ``blob-dir`` is configured.
    - :class:`ZODB.interfaces.IStorageUndoable` if ``keep-history`` is true.

    """
