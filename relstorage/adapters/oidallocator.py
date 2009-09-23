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
"""IOIDAllocator implementations"""

from relstorage.adapters.interfaces import IOIDAllocator
from zope.interface import implements

class PostgreSQLOIDAllocator(object):
    implements(IOIDAllocator)

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        cursor.execute("""
        SELECT CASE WHEN %s > nextval('zoid_seq')
            THEN setval('zoid_seq', %s)
            ELSE 0
            END
        """, (oid, oid))

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "SELECT NEXTVAL('zoid_seq')"
        cursor.execute(stmt)
        return cursor.fetchone()[0]


class MySQLOIDAllocator(object):
    implements(IOIDAllocator)

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        cursor.execute("REPLACE INTO new_oid VALUES(%s)", (oid,))

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "INSERT INTO new_oid VALUES ()"
        cursor.execute(stmt)
        oid = cursor.connection.insert_id()
        if oid % 100 == 0:
            # Clean out previously generated OIDs.
            stmt = "DELETE FROM new_oid WHERE zoid < %s"
            cursor.execute(stmt, (oid,))
        return oid


class OracleOIDAllocator(object):
    implements(IOIDAllocator)

    def __init__(self, connmanager):
        self.connmanager = connmanager

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        next_oid = self.new_oid(cursor)
        if next_oid < oid:
            # Oracle provides no way modify the sequence value
            # except through alter sequence or drop/create sequence,
            # but either statement kills the current transaction.
            # Therefore, open a temporary connection to make the
            # alteration.
            conn2, cursor2 = self.connmanager.open()
            try:
                # Change the sequence by altering the increment.
                # (this is safer than dropping and re-creating the sequence)
                diff = oid - next_oid
                cursor2.execute(
                    "ALTER SEQUENCE zoid_seq INCREMENT BY %d" % diff)
                cursor2.execute("SELECT zoid_seq.nextval FROM DUAL")
                cursor2.execute("ALTER SEQUENCE zoid_seq INCREMENT BY 1")
                conn2.commit()
            finally:
                self.connmanager.close(conn2, cursor2)

    def new_oid(self, cursor):
        """Return a new, unused OID."""
        stmt = "SELECT zoid_seq.nextval FROM DUAL"
        cursor.execute(stmt)
        return cursor.fetchone()[0]

