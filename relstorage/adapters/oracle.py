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

import logging
import cx_Oracle
from ZODB.POSException import StorageError
from zope.interface import implements

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryFreeDatabaseIterator
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.interfaces import IRelStorageAdapter
from relstorage.adapters.locker import OracleLocker
from relstorage.adapters.mover import ObjectMover
from relstorage.adapters.oidallocator import OracleOIDAllocator
from relstorage.adapters.packundo import HistoryFreePackUndo
from relstorage.adapters.packundo import OracleHistoryPreservingPackUndo
from relstorage.adapters.poller import Poller
from relstorage.adapters.schema import OracleSchemaInstaller
from relstorage.adapters.scriptrunner import OracleScriptRunner
from relstorage.adapters.stats import OracleStats
from relstorage.adapters.txncontrol import OracleTransactionControl

log = logging.getLogger(__name__)

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (
    cx_Oracle.OperationalError,
    cx_Oracle.InterfaceError,
    cx_Oracle.DatabaseError,
    )

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions

class OracleAdapter(object):
    """Oracle adapter for RelStorage."""
    implements(IRelStorageAdapter)

    def __init__(self, user, password, dsn, twophase=False, arraysize=64,
            use_inline_lobs=None, keep_history=True):
        """Create an Oracle adapter.

        The user, password, and dsn parameters are provided to cx_Oracle
        at connection time.

        If twophase is true, all commits go through an Oracle-level two-phase
        commit process.  This is disabled by default.  Even when this option
        is disabled, the ZODB two-phase commit is still in effect.

        arraysize sets the number of rows to buffer in cx_Oracle.  The default
        is 64.

        use_inline_lobs enables Oracle to send BLOBs inline in response to
        queries.  It depends on features in cx_Oracle 5.  The default is None,
        telling the adapter to auto-detect the presence of cx_Oracle 5.
        """
        if use_inline_lobs is None:
            use_inline_lobs = (cx_Oracle.version >= '5.0')
        self.keep_history = keep_history

        self.connmanager = CXOracleConnectionManager(
            params=(user, password, dsn),
            arraysize=arraysize,
            twophase=twophase,
            )
        self.runner = CXOracleScriptRunner(
            use_inline_lobs=use_inline_lobs,
            )
        self.locker = OracleLocker(
            keep_history=self.keep_history,
            lock_exceptions=(cx_Oracle.DatabaseError,),
            )
        self.schema = OracleSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
            )
        self.mover = ObjectMover(
            database_name='oracle',
            keep_history=self.keep_history,
            runner=self.runner,
            Binary=cx_Oracle.Binary,
            inputsize_BLOB=cx_Oracle.BLOB,
            inputsize_BINARY=cx_Oracle.BINARY,
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
            )

        if self.keep_history:
            self.packundo = OracleHistoryPreservingPackUndo(
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                )
            self.dbiter = HistoryPreservingDatabaseIterator(
                runner=self.runner,
                )
        else:
            self.packundo = HistoryFreePackUndo(
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                )
            self.dbiter = HistoryFreeDatabaseIterator(
                runner=self.runner,
                )

        self.stats = OracleStats(
            connmanager=self.connmanager,
            )


class CXOracleScriptRunner(OracleScriptRunner):

    def __init__(self, use_inline_lobs):
        self.use_inline_lobs = use_inline_lobs

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
    isolation_read_only = "READ ONLY"

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, params, arraysize, twophase):
        self._params = params
        self._arraysize = arraysize
        self._twophase = twophase

    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED",
            twophase=False):
        """Open a database connection and return (conn, cursor)."""
        try:
            kw = {'twophase': twophase}  #, 'threaded': True}
            conn = cx_Oracle.connect(*self._params, **kw)
            cursor = conn.cursor()
            cursor.arraysize = self._arraysize
            if transaction_mode:
                cursor.execute("SET TRANSACTION %s" % transaction_mode)
            return conn, cursor

        except cx_Oracle.OperationalError, e:
            log.warning("Unable to connect: %s", e)
            raise

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open(self.isolation_read_only)

    def restart_load(self, conn, cursor):
        """Reinitialize a connection for loading objects."""
        try:
            conn.rollback()
            cursor.execute("SET TRANSACTION READ ONLY")
        except self.disconnected_exceptions, e:
            raise StorageError(e)

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
        try:
            conn.rollback()
            if self._twophase:
                self._set_xid(conn, cursor)
            if self.on_store_opened is not None:
                self.on_store_opened(cursor, restart=True)
        except self.disconnected_exceptions, e:
            raise StorageError(e)

