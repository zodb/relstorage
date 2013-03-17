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

from perfmetrics import metricmethod
from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryFreeDatabaseIterator
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.interfaces import IRelStorageAdapter
from relstorage.adapters.interfaces import ReplicaClosedException
from relstorage.adapters.locker import OracleLocker
from relstorage.adapters.mover import ObjectMover
from relstorage.adapters.oidallocator import OracleOIDAllocator
from relstorage.adapters.packundo import OracleHistoryFreePackUndo
from relstorage.adapters.packundo import OracleHistoryPreservingPackUndo
from relstorage.adapters.poller import Poller
from relstorage.adapters.schema import OracleSchemaInstaller
from relstorage.adapters.scriptrunner import OracleScriptRunner
from relstorage.adapters.stats import OracleStats
from relstorage.adapters.txncontrol import OracleTransactionControl
from relstorage.options import Options
from zope.interface import implements
import cx_Oracle
import logging

log = logging.getLogger(__name__)

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (
    cx_Oracle.OperationalError,
    cx_Oracle.InterfaceError,
    cx_Oracle.DatabaseError,
    ReplicaClosedException,
    )

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions

class OracleAdapter(object):
    """Oracle adapter for RelStorage."""
    implements(IRelStorageAdapter)

    def __init__(self, user, password, dsn, commit_lock_id=0,
            twophase=False, options=None):
        """Create an Oracle adapter.

        The user, password, and dsn parameters are provided to cx_Oracle
        at connection time.

        If twophase is true, all commits go through an Oracle-level two-phase
        commit process.  This is disabled by default.  Even when this option
        is disabled, the ZODB two-phase commit is still in effect.
        """
        self._user = user
        self._password = password
        self._dsn = dsn
        self._twophase = twophase
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history

        self.connmanager = CXOracleConnectionManager(
            user=user,
            password=password,
            dsn=dsn,
            twophase=twophase,
            options=options,
            )
        self.runner = CXOracleScriptRunner()
        self.locker = OracleLocker(
            options=self.options,
            lock_exceptions=(cx_Oracle.DatabaseError,),
            inputsize_NUMBER=cx_Oracle.NUMBER,
            )
        self.schema = OracleSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
            )
        self.mover = ObjectMover(
            database_type='oracle',
            options=options,
            runner=self.runner,
            Binary=cx_Oracle.Binary,
            inputsizes={
                'blobdata': cx_Oracle.BLOB,
                'rawdata': cx_Oracle.BINARY,
                'oid': cx_Oracle.NUMBER,
                'tid': cx_Oracle.NUMBER,
                'prev_tid': cx_Oracle.NUMBER,
                'chunk_num': cx_Oracle.NUMBER,
                'md5sum': cx_Oracle.STRING,
                },
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = OracleOIDAllocator(
            connmanager=self.connmanager,
            )
        self.txncontrol = OracleTransactionControl(
            keep_history=self.keep_history,
            Binary=cx_Oracle.Binary,
            twophase=twophase,
            )

        if self.keep_history:
            poll_query="SELECT MAX(tid) FROM transaction"
        else:
            poll_query="SELECT MAX(tid) FROM object_state"
        self.poller = Poller(
            poll_query=poll_query,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
        )

        if self.keep_history:
            self.packundo = OracleHistoryPreservingPackUndo(
                database_type='oracle',
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
                )
            self.dbiter = HistoryPreservingDatabaseIterator(
                database_type='oracle',
                runner=self.runner,
                )
        else:
            self.packundo = OracleHistoryFreePackUndo(
                database_type='oracle',
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
                )
            self.dbiter = HistoryFreeDatabaseIterator(
                database_type='oracle',
                runner=self.runner,
                )

        self.stats = OracleStats(
            connmanager=self.connmanager,
            )

    def new_instance(self):
        # This adapter and its components are stateless, so it's
        # safe to share it between threads.
        return OracleAdapter(
            user=self._user,
            password=self._password,
            dsn=self._dsn,
            twophase=self._twophase,
            options=self.options,
            )

    def __str__(self):
        parts = [self.__class__.__name__]
        if self.keep_history:
            parts.append('history preserving')
        else:
            parts.append('history free')
        parts.append('user=%r' % self._user)
        parts.append('dsn=%r' % self._dsn)
        parts.append('twophase=%r' % self._twophase)
        return ", ".join(parts)


class CXOracleScriptRunner(OracleScriptRunner):

    def __init__(self):
        self.use_inline_lobs = (cx_Oracle.version >= '5.0')

    def _outputtypehandler(self,
            cursor, name, defaultType, size, precision, scale):
        """cx_Oracle outputtypehandler that causes Oracle to send BLOBs inline.

        Note that if a BLOB in the result is too large, Oracle generates an
        error indicating truncation.  The run_lob_stmt() method works
        around this.
        """
        if defaultType == cx_Oracle.BLOB:
            # Default size for BLOB is 4, we want the whole blob inline.
            # Typical chunk size is 8132, we choose a multiple - 32528
            return cursor.var(cx_Oracle.LONG_BINARY, 32528, cursor.arraysize)

    def _read_lob(self, value):
        """Handle an Oracle LOB by returning its byte stream.

        Returns other objects unchanged.
        """
        if isinstance(value, cx_Oracle.LOB):
            return value.read()
        return value

    def run_lob_stmt(self, cursor, stmt, args=(), default=None):
        """Execute a statement and return one row with all LOBs inline.

        Returns the value of the default parameter if the result was empty.
        """
        if self.use_inline_lobs:
            try:
                cursor.outputtypehandler = self._outputtypehandler
                try:
                    cursor.execute(stmt, args)
                    for row in cursor:
                        return row
                finally:
                    del cursor.outputtypehandler
            except cx_Oracle.DatabaseError, e:
                # ORA-01406: fetched column value was truncated
                error, = e
                if ((isinstance(error, str) and not error.endswith(' 1406'))
                        or error.code != 1406):
                    raise
                # Execute the query, but alter it slightly without
                # changing its meaning, so that the query cache
                # will see it as a statement that has to be compiled
                # with different output type parameters.
                cursor.execute(stmt + ' ', args)
                for row in cursor:
                    return tuple(map(self._read_lob, row))
        else:
            cursor.execute(stmt, args)
            for row in cursor:
                return tuple(map(self._read_lob, row))
        return default


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

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, user, password, dsn, twophase, options):
        self._user = user
        self._password = password
        self._dsn = dsn
        self._twophase = twophase
        super(CXOracleConnectionManager, self).__init__(options)

    @metricmethod
    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED",
            twophase=False, replica_selector=None):
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
                conn = cx_Oracle.connect(self._user, self._password, dsn, **kw)
                cursor = conn.cursor()
                cursor.arraysize = 64
                if transaction_mode:
                    cursor.execute("SET TRANSACTION %s" % transaction_mode)
                return conn, cursor

            except cx_Oracle.OperationalError, e:
                if replica_selector is not None:
                    next_dsn = replica_selector.next()
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

