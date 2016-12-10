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


from .._abstract_drivers import _select_driver
from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..poller import Poller

from . import drivers
from .batch import OracleRowBatcher
from .connmanager import CXOracleConnectionManager
from .locker import OracleLocker
from .mover import OracleObjectMover
from .oidallocator import OracleOIDAllocator
from .packundo import OracleHistoryFreePackUndo
from .packundo import OracleHistoryPreservingPackUndo
from .schema import OracleSchemaInstaller
from .scriptrunner import CXOracleScriptRunner
from .stats import OracleStats
from .txncontrol import OracleTransactionControl

from relstorage.options import Options
from zope.interface import implementer

import logging

log = logging.getLogger(__name__)

def select_driver(options=None):
    return _select_driver(options or Options(), drivers)

@implementer(IRelStorageAdapter)
class OracleAdapter(object):
    """Oracle adapter for RelStorage."""
    # pylint:disable=too-many-instance-attributes
    def __init__(self, user, password, dsn, commit_lock_id=0,
                 twophase=False, options=None):
        """Create an Oracle adapter.

        The user, password, and dsn parameters are provided to cx_Oracle
        at connection time.

        If twophase is true, all commits go through an Oracle-level two-phase
        commit process.  This is disabled by default.  Even when this option
        is disabled, the ZODB two-phase commit is still in effect.
        """
        # pylint:disable=unused-argument
        self._user = user
        self._password = password
        self._dsn = dsn
        self._twophase = twophase
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history

        driver = select_driver(options)
        log.debug("Using driver %s", driver)

        self.connmanager = CXOracleConnectionManager(
            driver,
            user=user,
            password=password,
            dsn=dsn,
            twophase=twophase,
            options=options,
        )
        self.runner = CXOracleScriptRunner(driver)
        self.locker = OracleLocker(
            options=self.options,
            lock_exceptions=driver.lock_exceptions,
            inputsize_NUMBER=driver.NUMBER,
        )
        self.schema = OracleSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
        )
        inputsizes = {
            'blobdata': driver.BLOB,
            'rawdata': driver.BINARY,
            'oid': driver.NUMBER,
            'tid': driver.NUMBER,
            'prev_tid': driver.NUMBER,
            'chunk_num': driver.NUMBER,
            'md5sum': driver.STRING,
        }
        self.mover = OracleObjectMover(
            database_type='oracle',
            options=options,
            runner=self.runner,
            Binary=driver.Binary,
            batcher_factory=lambda cursor, row_limit: OracleRowBatcher(cursor, inputsizes, row_limit),
        )
        self.mover.inputsizes = inputsizes
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = OracleOIDAllocator(
            connmanager=self.connmanager,
        )
        self.txncontrol = OracleTransactionControl(
            keep_history=self.keep_history,
            Binary=driver.Binary,
            twophase=twophase,
        )

        if self.keep_history:
            poll_query = "SELECT MAX(tid) FROM transaction"
        else:
            poll_query = "SELECT MAX(tid) FROM object_state"
        self.poller = Poller(
            poll_query=poll_query,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
        )

        # pylint:disable=redefined-variable-type
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
