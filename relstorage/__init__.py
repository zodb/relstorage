##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""relstorage package"""

def check_compatible():
    try:
        from ZODB.interfaces import IMVCCStorage
    except ImportError:
        # see if the polling patch has been applied
        from ZODB.Connection import Connection
        if not hasattr(Connection, '_poll_invalidations'):
            raise ImportError('RelStorage requires the invalidation polling '
                'patch for ZODB.')
    else:
        # We're running a version of ZODB that knows what to do with
        # MVCC storages, so no patch is necessary.
        pass

check_compatible()


def patch_zodb_sync():
    """Patch Connection.sync() and afterCompletion() to pass the 'force' flag.
    """

    def _storage_sync(self, *ignored, **kw):
        if hasattr(self, '_readCurrent'):
            self._readCurrent.clear()
        sync = getattr(self._storage, 'sync', 0)
        if sync:
            # By default, do not force the sync, allowing RelStorage
            # to ignore sync requests for a while.
            force = kw.get('force', False)
            try:
                sync(force=force)
            except TypeError:
                # The 'force' parameter is not accepted.
                sync()
        self._flush_invalidations()

    def sync(self):
        """Manually update the view on the database."""
        self.transaction_manager.abort()
        self._storage_sync(force=True)

    from ZODB.Connection import Connection
    Connection._storage_sync = _storage_sync
    Connection.afterCompletion = _storage_sync
    Connection.newTransaction = _storage_sync
    Connection.sync = sync

patch_zodb_sync()
