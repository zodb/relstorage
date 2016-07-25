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
"""
IOIDAllocator implementations.
"""

from __future__ import absolute_import

from ..oidallocator import AbstractOIDAllocator
from perfmetrics import metricmethod
from ..interfaces import IOIDAllocator
from zope.interface import implementer

@implementer(IOIDAllocator)
class PostgreSQLOIDAllocator(AbstractOIDAllocator):

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        n = (oid + 15) // 16
        cursor.execute("""
        SELECT CASE WHEN %s > nextval('zoid_seq')
            THEN setval('zoid_seq', %s)
            ELSE 0
            END
        """, (n, n))

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        stmt = "SELECT NEXTVAL('zoid_seq')"
        cursor.execute(stmt)
        n = cursor.fetchone()[0]
        return range(n * 16 - 15, n * 16 + 1)
