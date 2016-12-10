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
from __future__ import absolute_import

from .._abstract_drivers import _select_driver
from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..packundo import HistoryFreePackUndo
from ..packundo import HistoryPreservingPackUndo
from ..poller import Poller
from ..scriptrunner import ScriptRunner

from . import drivers
from .connmanager import Psycopg2ConnectionManager
from .locker import PostgreSQLLocker
from .mover import PostgreSQLObjectMover
from .oidallocator import PostgreSQLOIDAllocator
from .schema import PostgreSQLSchemaInstaller
from .stats import PostgreSQLStats
from .txncontrol import PostgreSQLTransactionControl


from relstorage.options import Options
from zope.interface import implementer
import logging
import re

log = logging.getLogger(__name__)

def select_driver(options=None):
    return _select_driver(options or Options(), drivers)

@implementer(IRelStorageAdapter)
class PostgreSQLAdapter(object):
    """PostgreSQL adapter for RelStorage."""

    # pylint:disable=too-many-instance-attributes
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
        self.mover = PostgreSQLObjectMover(
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
        # pylint:disable=redefined-variable-type
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
