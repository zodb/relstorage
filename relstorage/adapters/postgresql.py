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
"""PostgreSQL adapter for RelStorage."""

import logging
import psycopg2
import psycopg2.extensions
from zope.interface import implements

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryFreeDatabaseIterator
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.interfaces import IRelStorageAdapter
from relstorage.adapters.locker import PostgreSQLLocker
from relstorage.adapters.mover import ObjectMover
from relstorage.adapters.oidallocator import PostgreSQLOIDAllocator
from relstorage.adapters.packundo import HistoryFreePackUndo
from relstorage.adapters.packundo import HistoryPreservingPackUndo
from relstorage.adapters.poller import Poller
from relstorage.adapters.schema import PostgreSQLSchemaInstaller
from relstorage.adapters.scriptrunner import ScriptRunner
from relstorage.adapters.stats import PostgreSQLStats
from relstorage.adapters.txncontrol import PostgreSQLTransactionControl

log = logging.getLogger(__name__)

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
    )

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions

class PostgreSQLAdapter(object):
    """PostgreSQL adapter for RelStorage."""
    implements(IRelStorageAdapter)

    def __init__(self, dsn='', keep_history=True):
        self.keep_history = keep_history
        self._dsn = dsn
        self.connmanager = Psycopg2ConnectionManager(
            dsn=dsn,
            keep_history=self.keep_history,
            )
        self.runner = ScriptRunner()
        self.locker = PostgreSQLLocker(
            keep_history=self.keep_history,
            lock_exceptions=(psycopg2.DatabaseError,),
            )
        self.schema = PostgreSQLSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            locker=self.locker,
            keep_history=self.keep_history,
            )
        self.mover = ObjectMover(
            database_name='postgresql',
            keep_history=self.keep_history,
            runner=self.runner,
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = PostgreSQLOIDAllocator()
        self.txncontrol = PostgreSQLTransactionControl(
            keep_history=self.keep_history,
            )

        self.poller = Poller(
            poll_query="EXECUTE get_latest_tid",
            keep_history=self.keep_history,
            runner=self.runner,
            )

        if self.keep_history:
            self.packundo = HistoryPreservingPackUndo(
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

        self.stats = PostgreSQLStats(
            connmanager=self.connmanager,
            )

    def new_instance(self):
        # This adapter and its components are stateless, so it's
        # safe to share it between threads.
        return self

    def __str__(self):
        if self.keep_history:
            t = 'history preserving'
        else:
            t = 'history free'
        parts = self._dsn.split()
        s = ' '.join(p for p in parts if not p.startswith('password'))
        return "%s, %s, dsn=%r" % (self.__class__.__name__, t, s)


class Psycopg2ConnectionManager(AbstractConnectionManager):

    isolation_read_committed = (
        psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    isolation_serializable = (
        psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, dsn, keep_history):
        self._dsn = dsn
        self.keep_history = keep_history

    def open(self,
            isolation=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED):
        """Open a database connection and return (conn, cursor)."""
        try:
            conn = psycopg2.connect(self._dsn)
            conn.set_isolation_level(isolation)
            cursor = conn.cursor()
            cursor.arraysize = 64
        except psycopg2.OperationalError, e:
            log.warning("Unable to connect: %s", e)
            raise
        return conn, cursor

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open(self.isolation_serializable)
        if self.keep_history:
            stmt = """
            PREPARE get_latest_tid AS
            SELECT tid
            FROM transaction
            ORDER BY tid DESC
            LIMIT 1
            """
        else:
            stmt = """
            PREPARE get_latest_tid AS
            SELECT tid
            FROM object_state
            ORDER BY tid DESC
            LIMIT 1
            """            
        cursor.execute(stmt)
        return conn, cursor

