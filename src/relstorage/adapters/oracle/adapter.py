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

import logging

from zope.interface import implementer

from ..adapter import AbstractAdapter
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

log = logging.getLogger(__name__)


@implementer(IRelStorageAdapter)
class OracleAdapter(AbstractAdapter):
    """Oracle adapter for RelStorage."""
    # pylint:disable=too-many-instance-attributes

    driver_options = drivers

    def __init__(self,
                 user='relstoragetest',
                 password='relstoragetest',
                 dsn='192.168.1.131/orcl',
                 commit_lock_id=0,
                 twophase=False, options=None,
                 locker=None,
                 mover=None,
                 connmanager=None,
                 ):
        """Create an Oracle adapter.

        The user, password, and dsn parameters are provided to
        cx_Oracle at connection time.

        If twophase is true, all commits go through an Oracle-level
        two-phase commit process. This is disabled by default; it causes
        deadlock detection to become slow and based on a timeout. Even
        when this option is disabled, the ZODB two-phase commit is
        still in effect.
        """
        # pylint:disable=unused-argument
        self._user = user
        self._password = password
        self._dsn = dsn
        self._twophase = twophase
        self.locker = locker
        self.mover = mover
        self.connmanager = connmanager

        super(OracleAdapter, self).__init__(options)

    def _create(self):
        driver = self.driver
        user = self._user
        password = self._password
        twophase = self._twophase
        options = self.options
        dsn = self._dsn

        batcher_factory = lambda cursor, row_limit=None: OracleRowBatcher(
            cursor, inputsizes, row_limit
        )
        if self.connmanager is None:
            self.connmanager = CXOracleConnectionManager(
                driver,
                user=user,
                password=password,
                dsn=dsn,
                twophase=twophase,
                options=options,
            )
        self.runner = CXOracleScriptRunner(driver)
        if self.locker is None:
            self.locker = OracleLocker(
                options=self.options,
                driver=driver,
                inputsize_NUMBER=driver.NUMBER,
                batcher_factory=batcher_factory,
            )
        self.schema = OracleSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
        )
        if self.mover is None:
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
                driver,
                options=options,
                runner=self.runner,
                batcher_factory=batcher_factory,
            )
            self.mover.inputsizes = inputsizes

        self.oidallocator = OracleOIDAllocator(
            connmanager=self.connmanager,
        )

        self.poller = Poller(
            self.driver,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
            transactions_may_go_backwards=(
                self.connmanager.replica_selector is not None
                or self.connmanager.ro_replica_selector is not None
            )
        )

        self.txncontrol = OracleTransactionControl(
            connmanager=self.connmanager,
            poller=self.poller,
            keep_history=self.keep_history,
            Binary=driver.Binary,
            twophase=twophase,
        )



        if self.keep_history:
            self.packundo = OracleHistoryPreservingPackUndo(
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
            self.packundo = OracleHistoryFreePackUndo(
                driver,
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
            )
            self.dbiter = HistoryFreeDatabaseIterator(
                driver,
            )

        self.stats = OracleStats(
            connmanager=self.connmanager,
            keep_history=self.keep_history
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
            locker=self.locker,
            mover=self.mover,
            connmanager=self.connmanager,
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
