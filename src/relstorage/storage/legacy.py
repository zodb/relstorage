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


class LegacyMethodsMixin(object):

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
