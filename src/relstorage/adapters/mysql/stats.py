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
Stats implementations
"""
from __future__ import absolute_import

from ..stats import AbstractStats


class MySQLStats(AbstractStats):

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        conn, cursor = self.connmanager.open()
        try:
            cursor.execute("SHOW TABLE STATUS")
            description = [i[0] for i in cursor.description]
            # The MySQL connector C extension returns these as bytes
            # on python 3
            description = [x.decode('ascii') if not isinstance(x, str) else x
                           for x in description]
            rows = cursor.fetchall()
        finally:
            self.connmanager.close(conn, cursor)
        data_column = description.index('Data_length')
        index_column = description.index('Index_length')
        # We also got back views, which have no size.
        return sum([row[data_column] + row[index_column] for row in rows
                    if row[data_column] and row[index_column]], 0)
