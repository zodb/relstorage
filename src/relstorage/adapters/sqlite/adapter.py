# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
"""sqlite3 adapter for RelStorage."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os.path

from zope.interface import implementer

from ..adapter import AbstractAdapter
from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..packundo import HistoryFreePackUndo
from ..packundo import HistoryPreservingPackUndo

from . import drivers
from .batch import Sqlite3RowBatcher
from .connmanager import Sqlite3ConnectionManager
from .locker import Sqlite3Locker
from .mover import Sqlite3ObjectMover
from .oidallocator import Sqlite3OIDAllocator
from .schema import Sqlite3SchemaInstaller
from .stats import Sqlite3Stats
from .txncontrol import Sqlite3TransactionControl
from .poller import Sqlite3Poller
from .scriptrunner import Sqlite3ScriptRunner


@implementer(IRelStorageAdapter)
class Sqlite3Adapter(AbstractAdapter):
    driver_options = drivers
    WRITING_REQUIRES_EXCLUSIVE_LOCK = True


    def __init__(self, data_dir, pragmas,
                 options=None, oidallocator=None,
                 gevent_yield_interval=None):
        self.data_dir = os.path.abspath(data_dir)
        self.pragmas = pragmas
        self.oidallocator = oidallocator
        self.gevent_yield_interval = gevent_yield_interval
        super(Sqlite3Adapter, self).__init__(options)

    def _create(self):
        driver = self.driver
        options = self.options
        self.connmanager = Sqlite3ConnectionManager(
            driver,
            path=os.path.join(self.data_dir, 'main.sqlite3'),
            pragmas=self.pragmas,
            options=options
        )

        self.mover = Sqlite3ObjectMover(
            driver,
            options=options,
        )
        self.locker = Sqlite3Locker(
            options,
            driver,
            batcher_factory=Sqlite3RowBatcher)

        if not self.oidallocator:
            self.oidallocator = Sqlite3OIDAllocator(
                os.path.join(self.data_dir, 'oids.sqlite3'),
                # No switching during OID allocation. It holds an exclusive
                # lock anyway.
                driver=drivers.Sqlite3Driver()
            )

        self.runner = Sqlite3ScriptRunner()
        self.poller = Sqlite3Poller(
            self.driver,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
            transactions_may_go_backwards=False
        )

        self.txncontrol = Sqlite3TransactionControl(
            connmanager=self.connmanager,
            poller=self.poller,
            keep_history=self.keep_history,
            Binary=driver.Binary,
        )


        self.schema = Sqlite3SchemaInstaller(
            driver=driver,
            oid_allocator=self.oidallocator,
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history
        )

        self.stats = Sqlite3Stats(
            self.connmanager,
            self.keep_history
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

    def new_instance(self):
        inst = type(self)(
            self.data_dir,
            self.pragmas,
            options=self.options,
            oidallocator=self.oidallocator.new_instance())
        return inst
