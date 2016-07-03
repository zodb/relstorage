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

from perfmetrics import metricmethod
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
from relstorage.options import Options
from zope.interface import implementer
import logging

from . import _postgresql_drivers
from ._abstract_drivers import _select_driver

import re

log = logging.getLogger(__name__)

def select_driver(options=None):
    return _select_driver(options or Options(), _postgresql_drivers)

@implementer(IRelStorageAdapter)
class PostgreSQLAdapter(object):
    """PostgreSQL adapter for RelStorage."""

    def __init__(self, dsn='', options=None):
        # options is a relstorage.options.Options or None
        self._dsn = dsn
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history
        self.version_detector = PostgreSQLVersionDetector()

        driver = select_driver(options)
        log.debug("Using driver %r", driver)

        self.connmanager = Psycopg2ConnectionManager(
            driver,
            dsn=dsn,
            options=options,
            )
        self.runner = ScriptRunner()
        self.locker = PostgreSQLLocker(
            options=options,
            lock_exceptions=driver.lock_exceptions,
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
            Binary=driver.Binary,
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = PostgreSQLOIDAllocator()
        self.txncontrol = PostgreSQLTransactionControl(
            keep_history=self.keep_history,
            driver=driver,
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
        inst = type(self)(dsn=self._dsn, options=self.options)
        inst.version_detector.version = self.version_detector.version
        return inst

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


class Psycopg2ConnectionManager(AbstractConnectionManager):

    def __init__(self, driver, dsn, options):
        self._dsn = dsn
        self.disconnected_exceptions = driver.disconnected_exceptions
        self.close_exceptions = driver.close_exceptions
        self.use_replica_exceptions = driver.use_replica_exceptions
        self.isolation_read_committed = driver.ISOLATION_LEVEL_READ_COMMITTED
        self.isolation_serializable = driver.ISOLATION_LEVEL_SERIALIZABLE
        self.keep_history = options.keep_history
        self._db_connect_with_isolation = driver.connect_with_isolation
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
    def open(self, isolation=None, replica_selector=None):
        """Open a database connection and return (conn, cursor)."""
        if isolation is None:
            isolation = self.isolation_read_committed
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
                conn, cursor = self._db_connect_with_isolation(isolation, dsn)
                cursor.arraysize = 64
                conn.replica = replica
                return conn, cursor
            except self.use_replica_exceptions as e:
                if replica is not None:
                    next_replica = next(replica_selector)
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
