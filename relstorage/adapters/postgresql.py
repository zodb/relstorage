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
from relstorage.adapters.interfaces import ReplicaClosedException
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
    ReplicaClosedException,
    )

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions

class PostgreSQLAdapter(object):
    """PostgreSQL adapter for RelStorage."""
    implements(IRelStorageAdapter)

    def __init__(self, dsn='', keep_history=True, replica_conf=None):
        self._dsn = dsn
        self.keep_history = keep_history
        self.replica_conf = replica_conf
        self.connmanager = Psycopg2ConnectionManager(
            dsn=dsn,
            keep_history=self.keep_history,
            replica_conf=replica_conf,
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
        return PostgreSQLAdapter(
            dsn=self._dsn, keep_history=self.keep_history,
            replica_conf=self.replica_conf)

    def __str__(self):
        parts = [self.__class__.__name__]
        if self.keep_history:
            parts.append('history preserving')
        else:
            parts.append('history free')
        dsnparts = self._dsn.split()
        s = ' '.join(p for p in dsnparts if not p.startswith('password'))
        parts.append('dsn=%r' % s)
        parts.append('replica_conf=%r' % self.replica_conf)
        return ", ".join(parts)


class Psycopg2Connection(psycopg2.extensions.connection):
    # The replica attribute holds the name of the replica this
    # connection is bound to.
    __slots__ = ('replica',)


class Psycopg2ConnectionManager(AbstractConnectionManager):

    isolation_read_committed = (
        psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    isolation_serializable = (
        psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, dsn, keep_history, replica_conf=None):
        self._orig_dsn = dsn
        self._dsn = dsn
        self.keep_history = keep_history
        self._current_replica = None
        super(Psycopg2ConnectionManager, self).__init__(
            replica_conf=replica_conf)

    def _set_dsn(self, replica):
        """Alter the DSN to use the specified replica.

        The replica parameter is a string specifying either host or host:port.
        """
        if replica != self._current_replica:
            if ':' in replica:
                host, port = replica.split(':')
                self._dsn = self._orig_dsn + ' host=%s port=%s' % (host, port)
            else:
                self._dsn = self._orig_dsn + ' host=%s' % replica
            self._current_replica = replica

    def open(self,
            isolation=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED):
        """Open a database connection and return (conn, cursor)."""
        if self.replicas is not None:
            self._set_dsn(self.replicas.current())
        while True:
            try:
                conn = Psycopg2Connection(self._dsn)
                conn.set_isolation_level(isolation)
                cursor = conn.cursor()
                cursor.arraysize = 64
                conn.replica = self._current_replica
                return conn, cursor
            except psycopg2.OperationalError, e:
                if self._current_replica:
                    log.warning("Unable to connect to replica %s: %s",
                        self._current_replica, e)
                else:
                    log.warning("Unable to connect: %s", e)
                if self.replicas is not None:
                    replica = self.replicas.next()
                    if replica is not None:
                        # try the new replica
                        self._set_dsn(replica)
                        continue
                raise

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

