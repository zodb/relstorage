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
"""IOIDAllocator implementations.
"""

# All of these allocators allocate 16 OIDs at a time.  In the sequence
# or table, value (n) represents (n * 16 - 15) through (n * 16).  So,
# value 1 represents OID block 1-16, 2 represents OID block 17-32,
# and so on.

from perfmetrics import metricmethod
from relstorage.adapters.interfaces import IOIDAllocator
from zope.interface import implementer
from relstorage._compat import mysql_connection

@implementer(IOIDAllocator)
class PostgreSQLOIDAllocator(object):

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


@implementer(IOIDAllocator)
class MySQLOIDAllocator(object):

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        n = (oid + 15) // 16
        cursor.execute("REPLACE INTO new_oid VALUES(%s)", (n,))

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        stmt = "INSERT INTO new_oid VALUES ()"
        cursor.execute(stmt)
        conn = mysql_connection(cursor)
        n = conn.insert_id()
        if n % 100 == 0:
            # Clean out previously generated OIDs.
            stmt = "DELETE FROM new_oid WHERE zoid < %s"
            cursor.execute(stmt, (n,))
        return range(n * 16 - 15, n * 16 + 1)


@implementer(IOIDAllocator)
class OracleOIDAllocator(object):

    def __init__(self, connmanager):
        self.connmanager = connmanager

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        n = (oid + 15) // 16
        stmt = "SELECT zoid_seq.nextval FROM DUAL"
        cursor.execute(stmt)
        next_n = cursor.fetchone()[0]
        if next_n < n:
            # Oracle provides no way modify the sequence value
            # except through alter sequence or drop/create sequence,
            # but either statement kills the current transaction.
            # Therefore, open a temporary connection to make the
            # alteration.
            conn2, cursor2 = self.connmanager.open()
            try:
                # Change the sequence by altering the increment.
                # (this is safer than dropping and re-creating the sequence)
                diff = n - next_n
                cursor2.execute(
                    "ALTER SEQUENCE zoid_seq INCREMENT BY %d" % diff)
                cursor2.execute("SELECT zoid_seq.nextval FROM DUAL")
                cursor2.execute("ALTER SEQUENCE zoid_seq INCREMENT BY 1")
                conn2.commit()
            finally:
                self.connmanager.close(conn2, cursor2)

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        stmt = "SELECT zoid_seq.nextval FROM DUAL"
        cursor.execute(stmt)
        n = cursor.fetchone()[0]
        return range(n * 16 - 15, n * 16 + 1)
