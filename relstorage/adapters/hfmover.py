##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
"""History-free IObjectMover implementation.
"""

from base64 import decodestring
from base64 import encodestring
from relstorage.adapters.interfaces import IObjectMover
from ZODB.POSException import StorageError
from zope.interface import implements


class HistoryFreeObjectMover(object):
    implements(IObjectMover)

    _method_names = (
        'get_current_tid',
        'load_current',
        'load_revision',
        'exists',
        'load_before',
        'get_object_tid_after',
        'on_store_opened',
        'store_temp',
        'replace_temp',
        'restore',
        'detect_conflict',
        'move_from_temp',
        'update_current',
        )

    def __init__(self, database_name, runner=None,
            Binary=None, inputsize_BLOB=None, inputsize_BINARY=None):
        # The inputsize parameters are for Oracle only.
        self.database_name = database_name
        self.runner = runner
        self.Binary = Binary
        self.inputsize_BLOB = inputsize_BLOB
        self.inputsize_BINARY = inputsize_BINARY

        for method_name in self._method_names:
            method = getattr(self, '%s_%s' % (database_name, method_name))
            setattr(self, method_name, method)

