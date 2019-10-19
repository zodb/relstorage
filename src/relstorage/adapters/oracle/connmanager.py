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

import logging

from ..._compat import metricmethod
from ..connmanager import AbstractConnectionManager
from ..interfaces import ReplicaClosedException

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
        self._user = user
        self._password = password
        self._dsn = dsn
        self._twophase = twophase
        self._db_connect = driver.connect
        super(CXOracleConnectionManager, self).__init__(options, driver)

    @metricmethod
    def open(self,
             isolation=None,
             read_only=False,
             deferrable=False,
             replica_selector=None,
             application_name=None,
             twophase=False,
             **kwargs):
        """Open a database connection and return (conn, cursor)."""
        # pylint:disable=arguments-differ
        if isolation is None:
            isolation = 'ISOLATION LEVEL READ COMMITTED'

        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            dsn = replica_selector.current()
        else:
            dsn = self._dsn

        while True:
            try:
                # cx_Oracle deprecated `twophase` in 5.3 and removed
                # it in 6.0 in favor of the `internal_name` and
                # `external_name` parameters. In 5.3, these were both
                # implicitly set by `twophase` to "cx_Oracle".
                # According to the docs
                # (https://docs.oracle.com/cd/E18283_01/appdev.112/e10646/ociaahan.htm),
                # OCI_ATTR_EXTERNAL_NAME is "the user-friendly global
                # name stored in sys.props$.value$, where name =
                # 'GLOBAL_DB_NAME'. It is not guaranteed to be unique
                # " and OCI_ATTR_INTERNAL_NAME is "the client database
                # name that is recorded when performing global
                # transactions. The DBA can use the name to track
                # transactions"
                kw = {'threaded': True}
                conn = self._db_connect(self._user, self._password, dsn, **kw)
                conn.outputtypehandler = self._outputtypehandler
                if twophase:
                    conn.internal_name = 'cx_Oracle'
                    conn.external_name = 'cx_Oracle'
                cursor = conn.cursor()
                cursor.arraysize = 64

                if isolation:
                    cursor.execute("SET TRANSACTION %s" % isolation)
                return conn, cursor

            except self.driver.use_replica_exceptions as e:
                if replica_selector is not None:
                    next_dsn = next(replica_selector)
                    if next_dsn is not None:
                        log.warning("Unable to connect to DSN %s: %s, "
                                    "now trying %s", dsn, e, next_dsn)
                        dsn = next_dsn
                        continue
                log.warning("Unable to connect: %s", e)
                raise

    def _do_open_for_load(self):
        return self.open(self.isolation_read_only,
                         replica_selector=self.ro_replica_selector)

    def restart_load(self, conn, cursor, needs_rollback=True):
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

    def _do_open_for_store(self, **open_args):
        if self._twophase:
            conn, cursor = self.open(transaction_mode=None, twophase=True)
        else:
            conn, cursor = self.open()
        return conn, cursor

    def _after_opened_for_store(self, conn, cursor, restart=False):
        if self._twophase:
            self._set_xid(conn, cursor)

    def _outputtypehandler(self, cursor, name, defaultType,
                           size, precision, scale): # pylint:disable=unused-argument
        """
        cx_Oracle outputtypehandler that causes Oracle to send BLOBs
        inline. This works for sizes up to 1GB, which should be fine for object state,
        but not actual blobs.

        Note that if a BLOB in the result is too large, Oracle generates an
        error indicating truncation.  The run_lob_stmt() method works
        around this.
        """
        # pylint:disable=unused-argument
        if defaultType == self.driver.BLOB:
            # Default size for BLOB is 4000, we want the whole blob inline.
            return cursor.var(self.driver.LONG_BINARY, arraysize=cursor.arraysize)
        if defaultType == self.driver.CLOB:
            # Default size for CLOB is 4000, we want the whole blob inline.
            return cursor.var(self.driver.LONG_STRING, arraysize=cursor.arraysize)
