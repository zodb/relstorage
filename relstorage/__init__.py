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

# perform a compatibility test
try:
    from ZODB.interfaces import IMVCCStorage
    del IMVCCStorage
except ImportError:
    # see if the polling patch has been applied
    from ZODB.Connection import Connection
    if not hasattr(Connection, '_poll_invalidations'):
        raise ImportError('RelStorage requires the invalidation polling '
            'patch for ZODB.')
    del Connection
else:
    # We're running a version of ZODB that knows what to do with
    # MVCC storages, so no patch is necessary.
    pass
