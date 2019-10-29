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

from zope.interface import implementer

from ..._compat import metricmethod
from ..interfaces import IOIDAllocator
from ..oidallocator import AbstractRangedOIDAllocator


@implementer(IOIDAllocator)
class PostgreSQLOIDAllocator(AbstractRangedOIDAllocator):

    __slots__ = ()

    def _set_min_oid_from_range(self, cursor, n):
        # This potentially wastes a value from the sequence to find
        # out that we're already past the max. But that's the only option:
        # curval() is local to this connection, and 'ALTER SEQUENCE ... MINVALUE n STARTS WITH n'
        # takes a lock and doesn't let anyone use nextval() until we commit
        # (which could take some time). (Connections are expensive in PostgreSQL,
        # so we don't want to do what Oracle does and execute ALTER in a new connection.)
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
        return self._oid_range_around(n)

    def reset_oid(self, cursor):
        raise NotImplementedError
