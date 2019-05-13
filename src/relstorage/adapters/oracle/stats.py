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
"""Stats implementations
"""
from __future__ import absolute_import

from ..stats import AbstractStats


class OracleStats(AbstractStats):

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        conn, cursor = self.connmanager.open(
            self.connmanager.isolation_read_only)
        try:
            stmt = """
            SELECT SUM(BYTES)
            FROM USER_SEGMENTS
            """
            cursor.execute(stmt)
            res = cursor.fetchone()[0]
            if res is None:
                res = 0
            else:
                res = int(res)
            return res
        finally:
            self.connmanager.close(conn, cursor)
