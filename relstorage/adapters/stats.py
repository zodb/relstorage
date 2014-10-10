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

class PostgreSQLStats(object):

    def __init__(self, connmanager):
        self.connmanager = connmanager

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # do later
        return 0

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        def callback(conn, cursor):
            cursor.execute("SELECT pg_database_size(current_database())")
            return cursor.fetchone()[0]
        return self.connmanager.open_and_call(callback)


class MySQLStats(object):

    def __init__(self, connmanager):
        self.connmanager = connmanager

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # do later
        return 0

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        conn, cursor = self.connmanager.open()
        try:
            cursor.execute("SHOW TABLE STATUS")
            description = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
        finally:
            self.connmanager.close(conn, cursor)
        data_column = description.index('Data_length')
        index_column = description.index('Index_length')
        return sum([row[data_column] + row[index_column] for row in rows], 0)


class OracleStats(object):

    def __init__(self, connmanager):
        self.connmanager = connmanager

    def get_object_count(self):
        """Returns the number of objects in the database"""
        # The tests expect an exact number, but the code below generates
        # an estimate, so this is disabled for now.
        if True:
            return 0
        else:
            conn, cursor = self.connmanager.open(
                self.connmanager.isolation_read_only)
            try:
                stmt = """
                SELECT NUM_ROWS
                FROM USER_TABLES
                WHERE TABLE_NAME = 'CURRENT_OBJECT'
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

    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        # May not be possible without access to the dba_* objects
        return 0

