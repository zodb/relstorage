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
from __future__ import absolute_import, print_function

import logging
import re

from zope.interface import implementer
from ZODB.POSException import Unsupported

from ...options import Options
from .._abstract_drivers import _select_driver

from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..packundo import HistoryFreePackUndo
from ..packundo import HistoryPreservingPackUndo
from ..poller import Poller
from ..schema import Schema
from ..scriptrunner import ScriptRunner
from . import drivers
from .batch import PostgreSQLRowBatcher
from .connmanager import Psycopg2ConnectionManager
from .locker import PostgreSQLLocker
from .mover import PostgreSQLObjectMover

from .oidallocator import PostgreSQLOIDAllocator
from .schema import PostgreSQLSchemaInstaller
from .stats import PostgreSQLStats
from .txncontrol import PostgreSQLTransactionControl


log = logging.getLogger(__name__)

def select_driver(options=None):
    return _select_driver(options or Options(), drivers)

# TODO: Move to own file
class PGPoller(Poller):

    poll_query = Schema.all_transaction.select(
        Schema.all_transaction.c.tid
    ).order_by(
        Schema.all_transaction.c.tid, dir='DESC'
    ).limit(
        1
    ).prepared()


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

        self.driver = driver = select_driver(options)
        log.debug("Using driver %r", driver)

        self.connmanager = Psycopg2ConnectionManager(
            driver,
            dsn=dsn,
            options=options,
        )
        self.runner = ScriptRunner()
        self.locker = PostgreSQLLocker(
            options,
            driver,
            PostgreSQLRowBatcher,
        )
        self.schema = PostgreSQLSchemaInstaller(
            options=options,
            connmanager=self.connmanager,
            runner=self.runner,
            locker=self.locker,
        )

        self.mover = PostgreSQLObjectMover(
            driver,
            options=options,
            runner=self.runner,
            version_detector=self.version_detector,
            batcher_factory=PostgreSQLRowBatcher,
        )
        self.oidallocator = PostgreSQLOIDAllocator()

        self.poller = PGPoller(
            self.driver,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
        )

        self.txncontrol = PostgreSQLTransactionControl(
            connmanager=self.connmanager,
            poller=self.poller,
            keep_history=self.keep_history,
            Binary=driver.Binary,
        )

        if self.keep_history:
            self.packundo = HistoryPreservingPackUndo(
                driver,
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
            )
            self.dbiter = HistoryPreservingDatabaseIterator(
                driver,
            )
        else:
            self.packundo = HistoryFreePackUndo(
                driver,
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
            )
            # TODO: Subclass for this.
            self.packundo._lock_for_share = 'FOR KEY SHARE OF object_state'
            self.dbiter = HistoryFreeDatabaseIterator(
                driver,
            )

        self.stats = PostgreSQLStats(
            connmanager=self.connmanager,
            keep_history=self.keep_history
        )

        self.connmanager.add_on_store_opened(self.mover.on_store_opened)
        self.connmanager.add_on_load_opened(self.mover.on_load_opened)
        self.connmanager.add_on_store_opened(self.__prepare_store_statements)


    def __prepare_store_statements(self, cursor, restart=False):
        if not restart:
            try:
                stmt = self.txncontrol._prepare_add_transaction_query
            except (Unsupported, AttributeError):
                # AttributeError is from the PG8000 version,
                # Unsupported is from history-free
                pass
            else:
                cursor.execute(stmt)

            # If we don't commit now, any INSERT prepared statement
            # for some reason, hold an *exclusive* lock on the table
            # it references. That can prevent commits from working,
            # depending no the table, because we also lock tables when
            # committing. (Must be commit, not rollback, or the temp
            # tables we created would vanish.)
            #
            # We're the last hook, we handle this for ours and for
            # the mover's statements.
            cursor.execute('SET lock_timeout = 0') # restore infinite lock timeout
            cursor.connection.commit()


    def new_instance(self):
        inst = type(self)(dsn=self._dsn, options=self.options)
        return inst

    def __str__(self):
        parts = []
        if self.keep_history:
            parts.append('history preserving')
        else:
            parts.append('history free')
        dsnparts = self._dsn.split()
        s = ' '.join(p for p in dsnparts if not p.startswith('password'))
        parts.append('dsn=%r' % s)
        return "<%s at %x %s>" % (
            self.__class__.__name__, id(self), ",".join(parts)
        )

    __repr__ = __str__



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
