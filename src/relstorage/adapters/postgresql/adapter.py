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
from .._util import query_property
from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..packundo import HistoryFreePackUndo
from ..packundo import HistoryPreservingPackUndo
from ..poller import Poller
from ..scriptrunner import ScriptRunner
from . import drivers
from .batch import PostgreSQLRowBatcher
from .connmanager import Psycopg2ConnectionManager
from .locker import PostgreSQLLocker
from .mover import PG8000ObjectMover
from .mover import PostgreSQLObjectMover
from .mover import to_prepared_queries
from .oidallocator import PostgreSQLOIDAllocator
from .schema import PostgreSQLSchemaInstaller
from .stats import PostgreSQLStats
from .txncontrol import PostgreSQLTransactionControl
from .txncontrol import PG8000TransactionControl

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

        mover_type = PostgreSQLObjectMover
        txn_type = PostgreSQLTransactionControl
        if driver.__name__ == 'pg8000':
            mover_type = PG8000ObjectMover
            txn_type = PG8000TransactionControl

        self.mover = mover_type(
            driver,
            options=options,
            runner=self.runner,
            version_detector=self.version_detector,
            batcher_factory=PostgreSQLRowBatcher,
        )
        self.oidallocator = PostgreSQLOIDAllocator()
        self.txncontrol = txn_type(
            connmanager=self.connmanager,
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
                driver,
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
            )
            self.dbiter = HistoryPreservingDatabaseIterator(
                driver,
                runner=self.runner,
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
                runner=self.runner,
            )

        self.stats = PostgreSQLStats(
            connmanager=self.connmanager,
            keep_history=self.keep_history
        )

        self.connmanager.add_on_store_opened(self.mover.on_store_opened)
        self.connmanager.add_on_load_opened(self.mover.on_load_opened)
        self.connmanager.add_on_load_opened(self.__prepare_statements)
        self.connmanager.add_on_store_opened(self.__prepare_store_statements)


    _get_latest_tid_queries = (
        """
        SELECT tid
        FROM transaction
        ORDER BY tid DESC
        LIMIT 1
        """,
        """
        SELECT tid
        FROM object_state
        ORDER BY tid DESC
        LIMIT 1
        """
    )

    _prepare_get_latest_tid_queries = to_prepared_queries(
        'get_latest_tid',
        _get_latest_tid_queries)

    _prepare_get_latest_tid_query = query_property('_prepare_get_latest_tid')

    def __prepare_statements(self, cursor, restart=False):
        if restart:
            return

        # TODO: Generalize all of this better. There should be a
        # registry of things to prepare, or we should wrap cursors to
        # detect and prepare when needed. Preparation and switching to
        # EXECUTE should be automatic for drivers that don't already do that.

        # A meta-class or base class __new__ could handle proper
        # history/free query selection without this mass of tuples and
        # manual properties and property names.
        stmt = self._prepare_get_latest_tid_query
        cursor.execute(stmt)

    def __prepare_store_statements(self, cursor, restart=False):
        if not restart:
            self.__prepare_statements(cursor, restart)
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
