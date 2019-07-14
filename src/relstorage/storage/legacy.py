##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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
Mixin to the storage for legacy methods that aren't
needed or don't do anything anymore, but which still need to
exist for interface compliance or simply legacy code bases..

This keeps the main code uncluttered.

"""
from __future__ import absolute_import
from __future__ import print_function

import warnings

class LegacyMethodsMixin(object):

    __slots__ = (
    )

    # BaseStorage (and thus FileStorage) and MappingStorage define
    # tpc_transaction and _transaction, even though that's not part of
    # the interface. We only keep them for compatibility with
    # BaseStorage
    def tpc_transaction(self):
        return self._tpc_phase.transaction

    # For tests
    _transaction = property(tpc_transaction)

    def loadEx(self, oid, version=''):
        # Since we don't support versions, just tack the empty version
        # string onto load's result.
        return self.load(oid, version) + ("",)

    def cleanup(self):
        pass

    def supportsVersions(self):
        return False

    def modifiedInVersion(self, oid):
        # pylint:disable=unused-argument
        return ''

    @property
    def _load_cursor(self):
        warnings.warn("_load_cursor is deprecated", FutureWarning, stacklevel=2)
        return self._load_connection.cursor

    @property
    def _load_conn(self):
        warnings.warn("_load_conn is deprecated", FutureWarning, stacklevel=2)
        return self._load_connection.connection

    @property
    def _load_transaction_open(self):
        warnings.warn("_load_transaction_open is deprecated", FutureWarning, stacklevel=2)
        return bool(self._load_connection)

    @property
    def _store_cursor(self):
        warnings.warn("_store_cursor is deprecated", FutureWarning, stacklevel=2)
        return self._store_connection.cursor

    @property
    def _store_conn(self):
        warnings.warn("_store_conn is deprecated", FutureWarning, stacklevel=2)
        return self._store_connection.connection

    def _drop_load_connection(self):
        warnings.warn("_drop_load_connection is deprecated", FutureWarning, stacklevel=2)
        self._load_connection.drop()


    @property
    def fshelper(self):
        warnings.warn("fshelper is deprecated", FutureWarning, stacklevel=2)
        return self.blobhelper.fshelper
