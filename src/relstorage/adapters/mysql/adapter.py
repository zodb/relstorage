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
"""MySQL adapter for RelStorage.

Connection parameters supported by MySQLdb:

host
    string, host to connect
user
    string, user to connect as
passwd
    string, password to use
db
    string, database to use
port
    integer, TCP/IP port to connect to
unix_socket
    string, location of unix_socket (UNIX-ish only)
conv
    mapping, maps MySQL FIELD_TYPE.* to Python functions which convert a
    string to the appropriate Python type
connect_timeout
    number of seconds to wait before the connection attempt fails.
compress
    if set, gzip compression is enabled
named_pipe
    if set, connect to server via named pipe (Windows only)
init_command
    command which is run once the connection is created
read_default_file
    see the MySQL documentation for mysql_options()
read_default_group
    see the MySQL documentation for mysql_options()
client_flag
    client flags from MySQLdb.constants.CLIENT
load_infile
    int, non-zero enables LOAD LOCAL INFILE, zero disables
"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import json

from zope.interface import implementer

from relstorage._compat import iteritems
from relstorage._compat import metricmethod_sampled
from relstorage._util import TRACE

from ..adapter import AbstractAdapter

from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..interfaces import UnableToLockRowsToReadCurrentError
from ..interfaces import UnableToLockRowsToModifyError
from ..poller import Poller
from ..scriptrunner import ScriptRunner
from ..batch import RowBatcher

from . import drivers
from .connmanager import MySQLdbConnectionManager
from .locker import MySQLLocker
from .mover import MySQLObjectMover

from .oidallocator import MySQLOIDAllocator
from .packundo import MySQLHistoryFreePackUndo
from .packundo import MySQLHistoryPreservingPackUndo
from .schema import MySQLSchemaInstaller
from .schema import MySQLVersionDetector
from .stats import MySQLStats
from .txncontrol import MySQLTransactionControl

logger = logging.getLogger(__name__)


@implementer(IRelStorageAdapter)
class MySQLAdapter(AbstractAdapter):
    """MySQL adapter for RelStorage."""
    # pylint:disable=too-many-instance-attributes

    driver_options = drivers

    def __init__(self, options=None, oidallocator=None, version_detector=None, **params):
        self._params = params
        self.oidallocator = oidallocator
        self.version_detector = version_detector
        super(MySQLAdapter, self).__init__(options)

    def _create(self):
        options = self.options
        driver = self.driver
        params = self._params


        if self.version_detector is None:
            self.version_detector = MySQLVersionDetector()

        self.connmanager = MySQLdbConnectionManager(
            driver,
            params=params,
            options=options,
        )
        self.runner = ScriptRunner()
        self.locker = MySQLLocker(
            options=options,
            driver=driver,
            batcher_factory=RowBatcher,
            version_detector=self.version_detector,
        )

        self.schema = MySQLSchemaInstaller(
            driver=driver,
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
            version_detector=self.version_detector,
        )
        self.mover = MySQLObjectMover(
            driver,
            options=options,
        )

        if self.oidallocator is None:
            self.oidallocator = MySQLOIDAllocator(driver)

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

        self.txncontrol = MySQLTransactionControl(
            connmanager=self.connmanager,
            poller=self.poller,
            keep_history=self.keep_history,
            Binary=driver.Binary,
        )

        if self.keep_history:
            self.packundo = MySQLHistoryPreservingPackUndo(
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
            self.packundo = MySQLHistoryFreePackUndo(
                driver,
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
            )
            self.dbiter = HistoryFreeDatabaseIterator(
                driver,
            )

        self.stats = MySQLStats(
            connmanager=self.connmanager,
            keep_history=self.keep_history
        )

    def new_instance(self):
        return type(self)(
            options=self.options,
            oidallocator=self.oidallocator.new_instance(),
            version_detector=self.version_detector,
            **self._params
        )

    def __str__(self):
        parts = [self.__class__.__name__]
        if self.keep_history:
            parts.append('history preserving')
        else:
            parts.append('history free')
        p = self._params.copy()
        if 'passwd' in p:
            del p['passwd']
        p = sorted(iteritems(p))
        parts.extend('%s=%r' % item for item in p)
        return ", ".join(parts)

    # A temporary magic variable as we move TID allocation into some
    # databases; with an external clock, we *do* need to sleep waiting for
    # TIDs to change in a manner we can exploit; that or we need to be very
    # careful about choosing pack times.
    RS_TEST_TXN_PACK_NEEDS_SLEEP = 1

    @metricmethod_sampled
    def lock_database_and_choose_next_tid(self,
                                          cursor,
                                          username,
                                          description,
                                          extension):
        proc = 'lock_and_choose_tid'
        args = ()
        if self.keep_history:
            # (packed, username, descr, extension)
            proc = proc + '(%s, %s, %s, %s)'
            args = (False, username, description, extension)

        multi_results = self.driver.callproc_multi_result(cursor, proc, args)
        tid, = multi_results[0][0]
        logger.log(TRACE, "Locked database only to choose tid %s", tid)
        return tid

    @metricmethod_sampled
    def lock_database_and_move(self,
                               store_connection,
                               blobhelper,
                               ude,
                               commit=True,
                               committing_tid_int=None,
                               after_selecting_tid=lambda tid: None):
        if not self.version_detector.supports_good_stored_procs(store_connection.cursor):
            # XXX: When can we drop this? Probably not until AppVeyor upgrades
            # MySQL past 5.7.12.
            return super(MySQLAdapter, self).lock_database_and_move(
                store_connection,
                blobhelper,
                ude,
                commit=commit,
                committing_tid_int=committing_tid_int,
                after_selecting_tid=after_selecting_tid)

        params = (committing_tid_int, commit)
        # (p_committing_tid, p_commit)
        proc = 'lock_and_choose_tid_and_move(%s, %s)'
        if self.keep_history:
            params += ude
            # (p_committing_tid, p_commit, p_user, p_desc, p_ext)
            proc = 'lock_and_choose_tid_and_move(%s, %s, %s, %s, %s)'

        multi_results = self.driver.callproc_multi_result(
            store_connection.cursor,
            proc,
            params,
            exit_critical_phase=commit
        )

        tid_int, = multi_results[0][0]
        after_selecting_tid(tid_int)
        logger.log(TRACE, "Locked database and moved rows for tid %s", tid_int)
        return tid_int, "-"

    DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = False

    def _best_lock_objects_and_detect_conflicts(self, cursor, read_current_oids):
        read_current_param = None
        if read_current_oids:
            # In MySQL 8, we could pass in a JSON array and use JSON_TABLE
            # to join directly against the data.

            # In earlier versions, we could do some tricks with strings and
            # preparing dynamic SQL.

            # In all versions, we could write a query like
            #   select tid from object_state where json_contains('[1, 8]', cast(zoid as json), '$')
            # but that is very slow (entire table is scanned).
            #
            # We pass the string array, parse it as json, loop over it to put in a temp table
            # and join that.
            #
            # I also benchmarked sending the data in a format suitable
            # for concatenating to the end of 'INSERT ... VALUES' and
            # using dynamic SQL in the stored proc to execute that
            # ('PREPARE stmt FROM @str; EXECUTE stmt') and it
            # benchmarked essentially the same. Usually there are a
            # small number of things being readCurrent so it probably
            # doesn't matter.
            read_current_param = json.dumps(list(read_current_oids.items()))

        proc = 'lock_objects_and_detect_conflicts(%s, %s)'
        try:
            multi_results = self.driver.callproc_multi_result(
                cursor,
                proc,
                (read_current_param, 0)
            )
        except self.locker.lock_exceptions as e:
            # On MySQL 5.7, the time-based mechanism to determine that
            # we failed to take NOWAIT shared locks is not reliable
            # when the commit lock timeout is very small (close to 1),
            # because it can take several seconds for the procedure te
            # error out. Thus we provide a specific error message that
            # we key off. (We don't change errno or state or anything
            # like that in case anybody introspects that stuff.)
            if 'shared locks' in str(e):
                self.locker.reraise_commit_lock_error(
                    cursor,
                    self._describe_best_lock_objects_and_detect_conflicts(),
                    UnableToLockRowsToReadCurrentError
                )
            elif 'exclusive locks' in str(e):
                self.locker.reraise_commit_lock_error(
                    cursor,
                    self._describe_best_lock_objects_and_detect_conflicts(),
                    UnableToLockRowsToModifyError
                )
            raise

        # There's always a useless last result, the result of the stored procedure itself.
        proc_result = multi_results.pop()
        assert not proc_result, proc_result
        assert len(multi_results) == 1, multi_results
        conflicts = multi_results[0]

        return conflicts

    def _describe_best_lock_objects_and_detect_conflicts(self):
        return 'lock_objects_and_detect_conflicts(%s)'
