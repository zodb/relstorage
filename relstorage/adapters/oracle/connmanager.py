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
"""Oracle adapter for RelStorage."""
from __future__ import absolute_import

from perfmetrics import metricmethod
from ..connmanager import AbstractConnectionManager
from ..interfaces import ReplicaClosedException

import logging

log = logging.getLogger(__name__)

class CXOracleConnectionManager(AbstractConnectionManager):

    isolation_read_committed = "ISOLATION LEVEL READ COMMITTED"

    # Note: the READ ONLY mode should be sufficient according to the
    # Oracle documentation, which says: "All subsequent queries in that
    # transaction see only changes that were committed before the
    # transaction began."
    #
    # See: http://download.oracle.com/docs/cd/B19306_01/server.102/b14200
    #   /statements_10005.htm
    #
    # This would be great for performance if we could rely on it.
    # It's like serializable isolation but with less locking.
    #
    # However, in testing an Oracle 10g RAC environment with
    # RelStorage, Oracle frequently leaked subsequently committed
    # transactions into a read only transaction, suggesting that read
    # only in RAC actually has read committed isolation rather than
    # serializable isolation. Switching to serializable mode solved the
    # problem. Using a DSN that specifies a particular RAC node did
    # *not* solve the problem. It's likely that this is a bug in RAC,
    # but let's be on the safe side and have all Oracle users apply
    # serializable mode instead of read only mode, since serializable
    # is more explicit.
    #
    # If anyone wants to try read only mode anyway, change the
    # class variable below.
    #
    #isolation_read_only = "READ ONLY"

    isolation_read_only = "ISOLATION LEVEL SERIALIZABLE"

    def __init__(self, driver, user, password, dsn, twophase, options):
        self.disconnected_exceptions = driver.disconnected_exceptions
        self.close_exceptions = driver.close_exceptions
        self.use_replica_exceptions = driver.use_replica_exceptions
        self._user = user
        self._password = password
        self._dsn = dsn
        self._twophase = twophase
        self._db_connect = driver.connect
        super(CXOracleConnectionManager, self).__init__(options)

    @metricmethod
    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED",
             twophase=False, replica_selector=None, **kwargs):
        """Open a database connection and return (conn, cursor)."""
        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            dsn = replica_selector.current()
        else:
            dsn = self._dsn

        while True:
            try:
                kw = {'twophase': twophase, 'threaded': True}
                conn = self._db_connect(self._user, self._password, dsn, **kw)
                cursor = conn.cursor()
                cursor.arraysize = 64
                if transaction_mode:
                    cursor.execute("SET TRANSACTION %s" % transaction_mode)
                return conn, cursor

            except self.use_replica_exceptions as e:
                if replica_selector is not None:
                    next_dsn = next(replica_selector)
                    if next_dsn is not None:
                        log.warning("Unable to connect to DSN %s: %s, "
                                    "now trying %s", dsn, e, next_dsn)
                        dsn = next_dsn
                        continue
                log.warning("Unable to connect: %s", e)
                raise

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open(self.isolation_read_only,
                         replica_selector=self.ro_replica_selector)

    def restart_load(self, conn, cursor):
        """Reinitialize a connection for loading objects."""
        self.check_replica(conn, cursor,
                           replica_selector=self.ro_replica_selector)
        conn.rollback()
        cursor.execute("SET TRANSACTION %s" % self.isolation_read_only)

    def check_replica(self, conn, cursor, replica_selector=None):
        """Raise an exception if the connection belongs to an old replica"""
        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            current = replica_selector.current()
            if conn.dsn != current:
                # Prompt the change to a new replica by raising an exception.
                self.close(conn, cursor)
                raise ReplicaClosedException(
                    "Switched replica from %s to %s" % (conn.dsn, current))

    def _set_xid(self, conn, cursor):
        """Set up a distributed transaction"""
        stmt = """
        SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL
        """
        cursor.execute(stmt)
        xid = str(cursor.fetchone()[0])
        conn.begin(0, xid, '0')

    def open_for_store(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        if self._twophase:
            conn, cursor = self.open(transaction_mode=None, twophase=True)
        else:
            conn, cursor = self.open()
        try:
            if self._twophase:
                self._set_xid(conn, cursor)
            if self.on_store_opened is not None:
                self.on_store_opened(cursor, restart=False)
            return conn, cursor
        except:
            self.close(conn, cursor)
            raise

    def restart_store(self, conn, cursor):
        """Reuse a store connection."""
        self.check_replica(conn, cursor)
        conn.rollback()
        if self._twophase:
            self._set_xid(conn, cursor)
        if self.on_store_opened is not None:
            self.on_store_opened(cursor, restart=True)
