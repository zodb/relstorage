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
from perfmetrics import metricmethod
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
from relstorage.options import Options
from zope.interface import implements
import logging
import psycopg2
import psycopg2.extensions
import re
"""PostgreSQL adapter for RelStorage."""



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

    def __init__(self, dsn='', options=None):
        # options is a relstorage.options.Options or None
        self._dsn = dsn
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history
        self.version_detector = PostgreSQLVersionDetector()
        self.connmanager = Psycopg2ConnectionManager(
            dsn=dsn,
            options=options,
            )
        self.runner = ScriptRunner()
        self.locker = PostgreSQLLocker(
            options=options,
            lock_exceptions=(psycopg2.DatabaseError,),
            version_detector=self.version_detector,
            )
        self.schema = PostgreSQLSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            locker=self.locker,
            keep_history=self.keep_history,
            )
        self.mover = ObjectMover(
            database_type='postgresql',
            options=options,
            runner=self.runner,
            version_detector=self.version_detector,
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
            revert_when_stale=options.revert_when_stale,
        )

        if self.keep_history:
            self.packundo = HistoryPreservingPackUndo(
                database_type='postgresql',
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
                )
            self.dbiter = HistoryPreservingDatabaseIterator(
                database_type='postgresql',
                runner=self.runner,
                )
        else:
            self.packundo = HistoryFreePackUndo(
                database_type='postgresql',
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
                )
            self.dbiter = HistoryFreeDatabaseIterator(
                database_type='postgresql',
                runner=self.runner,
                )

        self.stats = PostgreSQLStats(
            connmanager=self.connmanager,
            )

    def new_instance(self):
        return PostgreSQLAdapter(dsn=self._dsn, options=self.options)

    def __str__(self):
        parts = [self.__class__.__name__]
        if self.keep_history:
            parts.append('history preserving')
        else:
            parts.append('history free')
        dsnparts = self._dsn.split()
        s = ' '.join(p for p in dsnparts if not p.startswith('password'))
        parts.append('dsn=%r' % s)
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

    def __init__(self, dsn, options):
        self._dsn = dsn
        self.keep_history = options.keep_history
        super(Psycopg2ConnectionManager, self).__init__(options)

    def _alter_dsn(self, replica):
        """Alter the DSN to use the specified replica.

        The replica parameter is a string specifying either host or host:port.
        """
        if ':' in replica:
            host, port = replica.split(':')
            dsn = '%s host=%s port=%s' % (self._dsn, host, port)
        else:
            dsn = '%s host=%s' % (self._dsn, replica)
        return dsn

    @metricmethod
    def open(self,
            isolation=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            replica_selector=None):
        """Open a database connection and return (conn, cursor)."""
        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            replica = replica_selector.current()
            dsn = self._alter_dsn(replica)
        else:
            replica = None
            dsn = self._dsn

        while True:
            try:
                conn = Psycopg2Connection(dsn)
                conn.set_isolation_level(isolation)
                cursor = conn.cursor()
                cursor.arraysize = 64
                conn.replica = replica
                return conn, cursor
            except psycopg2.OperationalError, e:
                if replica is not None:
                    next_replica = replica_selector.next()
                    if next_replica is not None:
                        log.warning("Unable to connect to replica %s: %s, "
                            "now trying %s", replica, e, next_replica)
                        replica = next_replica
                        dsn = self._alter_dsn(replica)
                        continue
                log.warning("Unable to connect: %s", e)
                raise

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open(self.isolation_serializable,
            replica_selector=self.ro_replica_selector)
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


class PostgreSQLVersionDetector(object):

    version = None

    def get_version(self, cursor):
        """Return the (major, minor) version of the database"""
        if self.version is None:
            cursor.execute("SELECT version()")
            v = cursor.fetchone()[0]
            m = re.search(r"([0-9]+)[.]([0-9]+)", v)
            if m is None:
                raise AssertionError("Unable to detect database version: " + v)
            self.version = int(m.group(1)), int(m.group(2))
        return self.version
