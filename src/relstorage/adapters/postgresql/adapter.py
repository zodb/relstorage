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

from ..adapter import AbstractAdapter

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


# TODO: Move to own file
class PGPoller(Poller):

    _poll_newest_tid_query = Schema.all_transaction.select(
        Schema.all_transaction.c.tid
    ).order_by(
        Schema.all_transaction.c.tid, dir='DESC'
    ).limit(
        1
    ).prepared()


@implementer(IRelStorageAdapter)
class PostgreSQLAdapter(AbstractAdapter):
    """PostgreSQL adapter for RelStorage."""
    # pylint:disable=too-many-instance-attributes

    driver_options = drivers

    def __init__(self, dsn='', options=None, oidallocator=None):
        # options is a relstorage.options.Options or None
        self._dsn = dsn
        self.oidallocator = oidallocator
        super(PostgreSQLAdapter, self).__init__(options)

    def _create(self):
        driver = self.driver
        options = self.options
        dsn = self._dsn

        self.version_detector = PostgreSQLVersionDetector()
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
        if self.oidallocator is None:
            self.oidallocator = PostgreSQLOIDAllocator()

        self.poller = PGPoller(
            self.driver,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
            transactions_may_go_backwards=(
                self.connmanager.replica_selector is not None
                or self.connmanager.ro_replica_selector is not None
            )
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

    def new_instance(self):
        inst = type(self)(
            dsn=self._dsn,
            options=self.options,
            oidallocator=self.oidallocator.new_instance()
        )
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

    # A temporary magic variable as we move TID allocation into some
    # databases; with an external clock, we *do* need to sleep waiting for
    # TIDs to change in a manner we can exploit; that or we need to be very
    # careful about choosing pack times.
    RS_TEST_TXN_PACK_NEEDS_SLEEP = 1

    def lock_database_and_choose_next_tid(self,
                                          cursor,
                                          username,
                                          description,
                                          extension):
        proc_name = 'SELECT lock_and_choose_tid'
        proc = proc_name + '()'
        args = ()
        if self.keep_history:
            # (packed, username, descr, extension)
            proc = proc_name + '(%s, %s, %s, %s)'
            b = self._binary
            args = (False, b(username), b(description), b(extension))

        cursor.execute(proc, args)
        tid, = cursor.fetchone()
        return tid

    def lock_database_and_move(self,
                               store_connection,
                               blobhelper, # pylint:disable=unused-argument
                               ude,
                               commit=True,
                               committing_tid_int=None,
                               after_selecting_tid=lambda tid: None):

        # In all versions of Postgres (up through 11 anyway),
        # stored functions cannot COMMIT. In Postgres 11,
        # the newly-introduced stored procedures *can* COMMIT,
        # if they're at the top level; that includes anonymous
        # DO blocks, BUT (and this goes for both anonymous and CALL'd procs)
        # ONLY if they're not already part of a transaction.
        #
        # Options:
        #
        # We can tack ``; COMMIT`` on to the end of the ``SELECT``
        # statement, but pg8000 doesn't like that ("cannot insert
        # multiple commands into a prepared statement") psycopg2 will
        # allow it, but because the last statement wasn't a ``SELECT``
        # we lose access to the TID.
        #
        # If we alter the temp tables to preserve their rows on
        # COMMIT, we could COMMIT now, turn on autocommit, and call
        # the function to move rows and make current. The problem
        # there is that we would lose our row locks, so we're not
        # guaranteed that we'd actually be able to finish the COMMIT.
        #
        # We can use the GUC (grand unified config) as session variables
        # and store the return value in the session (as text) and select it back out
        # after the commit. This seems to work, at the expense of extra
        # DB communication, but it gets the COMMIT to happen in one
        # trip to the DB: This is confirmed by database statement logging.
        # The only problem here is that it still fails on pg8000;
        # we'll just ignore that.
        params = (committing_tid_int, commit)
        # (p_committing_tid, p_commit)
        proc = 'lock_and_choose_tid_and_move(%s, %s)'
        if self.keep_history:
            username, description, extension = ude
            b = self._binary
            params += (b(username), b(description), b(extension))
            # (p_committing_tid, p_commit, p_user, p_desc, p_ext)
            proc = 'lock_and_choose_tid_and_move(%s, %s, %s, %s, %s)'

        if commit and self.driver.supports_multiple_statement_execute:
            # Do this all in one trip to the database so that we don't need to
            # wake up to handle the commit. Unfortunately, though, this
            # will make the internal state of our connection object in libpq not match
            # the actual state of the transaction on the server so we must still
            # execute connection.commit() to bring them back in sync. This results
            # in a warning on the server about no transaction being in progress.
            proc = (
                "SELECT SET_CONFIG('rs.tid', " + proc + "::text, FALSE); "
                "COMMIT; "
                "SELECT current_setting('rs.tid')"
            )
        else:
            proc = 'SELECT ' + proc


        cursor = store_connection.cursor
        cursor.execute(proc, params)
        tid_int, = cursor.fetchone()
        tid_int = int(tid_int)
        if commit:
            if self.driver.supports_multiple_statement_execute:
                self.driver.sync_status_after_commit(store_connection.connection)
            else:
                self.txncontrol.commit_phase2(store_connection, "-")
        after_selecting_tid(tid_int)
        return tid_int, "-"


    DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = False

    def _best_lock_objects_and_detect_conflicts(self, cursor, read_current_oids):
        read_current_oids_p = None
        read_current_tids_p = None
        if read_current_oids:
            # Pass both the OIDs and TIDS and make the database do an extra query
            # to filter the non-matching so that we only have to deal with rows that
            # actually conflict. This keeps the Python-level loop that we do once the rows are
            # locked as short as possible.
            # Separate arrays are faster to deal with than an array of tuples,
            # they just have to be in the same order.

            # Just pass in the OIDs and let it return to us the committed tids
            # which we will loop over to compare. This simplifies the SQL
            # and reduces the number of queries we have to do. If that looping is
            # a problem
            read_current_oids_p = []
            read_current_tids_p = []
            for k, v in read_current_oids.items():
                read_current_oids_p.append(k)
                read_current_tids_p.append(v)

        cursor.execute('SELECT * FROM lock_objects_and_detect_conflicts(%s, %s)',
                       (read_current_oids_p, read_current_tids_p,))
        conflicts = cursor.fetchall()
        return conflicts

    def _describe_best_lock_objects_and_detect_conflicts(self):
        return 'lock_objects_and_detect_conflicts(%s)'


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
