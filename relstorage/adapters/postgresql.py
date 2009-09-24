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

import logging
import psycopg2
import psycopg2.extensions

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.hpmover import HistoryPreservingObjectMover
from relstorage.adapters.locker import PostgreSQLLocker
from relstorage.adapters.oidallocator import PostgreSQLOIDAllocator
from relstorage.adapters.packundo import HistoryPreservingPackUndo
from relstorage.adapters.poller import Poller
from relstorage.adapters.schema import PostgreSQLSchemaInstaller
from relstorage.adapters.scriptrunner import ScriptRunner
from relstorage.adapters.stats import PostgreSQLStats
from relstorage.adapters.txncontrol import PostgreSQLTransactionControl

log = logging.getLogger(__name__)

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
    )

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions

class PostgreSQLAdapter(object):
    """PostgreSQL adapter for RelStorage."""

    keep_history = True

    def __init__(self, dsn=''):
        self.connmanager = Psycopg2ConnectionManager(dsn)
        self.runner = ScriptRunner()
        self.locker = PostgreSQLLocker((psycopg2.DatabaseError,))
        self.schema = PostgreSQLSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            locker=self.locker,
            keep_history=self.keep_history,
            )
        self.mover = HistoryPreservingObjectMover(
            database_name='postgresql',
            runner=self.runner,
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = PostgreSQLOIDAllocator()
        self.txncontrol = PostgreSQLTransactionControl()
        self.poller = Poller(
            poll_query="EXECUTE get_latest_tid",
            keep_history=self.keep_history,
            runner=self.runner,
            )
        self.packundo = HistoryPreservingPackUndo(
            connmanager=self.connmanager,
            runner=self.runner,
            locker=self.locker,
            )
        self.dbiter = HistoryPreservingDatabaseIterator(
            runner=self.runner,
            )
        self.stats = PostgreSQLStats(
            connmanager=self.connmanager,
            )

        self.open = self.connmanager.open
        self.close = self.connmanager.close
        self.open_for_load = self.connmanager.open_for_load
        self.restart_load = self.connmanager.restart_load
        self.open_for_store = self.connmanager.open_for_store
        self.restart_store = self.connmanager.restart_store

        self.hold_commit_lock = self.locker.hold_commit_lock
        self.release_commit_lock = self.locker.release_commit_lock
        self.hold_pack_lock = self.locker.hold_pack_lock
        self.release_pack_lock = self.locker.release_pack_lock

        self.create_schema = self.schema.create
        self.prepare_schema = self.schema.prepare
        self.zap_all = self.schema.zap_all
        self.drop_all = self.schema.drop_all

        self.get_current_tid = self.mover.get_current_tid
        self.load_current = self.mover.load_current
        self.load_revision = self.mover.load_revision
        self.exists = self.mover.exists
        self.load_before = self.mover.load_before
        self.get_object_tid_after = self.mover.get_object_tid_after
        self.store_temp = self.mover.store_temp
        self.replace_temp = self.mover.replace_temp
        self.restore = self.mover.restore
        self.detect_conflict = self.mover.detect_conflict
        self.move_from_temp = self.mover.move_from_temp
        self.update_current = self.mover.update_current

        self.set_min_oid = self.oidallocator.set_min_oid
        self.new_oid = self.oidallocator.new_oid

        self.get_tid_and_time = self.txncontrol.get_tid_and_time
        self.add_transaction = self.txncontrol.add_transaction
        self.commit_phase1 = self.txncontrol.commit_phase1
        self.commit_phase2 = self.txncontrol.commit_phase2
        self.abort = self.txncontrol.abort

        self.poll_invalidations = self.poller.poll_invalidations

        self.fill_object_refs = self.packundo.fill_object_refs
        self.open_for_pre_pack = self.packundo.open_for_pre_pack
        self.choose_pack_transaction = self.packundo.choose_pack_transaction
        self.pre_pack = self.packundo.pre_pack
        self.pack = self.packundo.pack
        self.verify_undoable = self.packundo.verify_undoable
        self.undo = self.packundo.undo

        self.iter_objects = self.dbiter.iter_objects
        self.iter_transactions = self.dbiter.iter_transactions
        self.iter_transactions_range = self.dbiter.iter_transactions_range
        self.iter_object_history = self.dbiter.iter_object_history

        self.get_object_count = self.stats.get_object_count
        self.get_db_size = self.stats.get_db_size


class Psycopg2ConnectionManager(AbstractConnectionManager):

    isolation_read_committed = (
        psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    isolation_serializable = (
        psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, dsn):
        self._dsn = dsn

    def open(self,
            isolation=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED):
        """Open a database connection and return (conn, cursor)."""
        try:
            conn = psycopg2.connect(self._dsn)
            conn.set_isolation_level(isolation)
            cursor = conn.cursor()
            cursor.arraysize = 64
        except psycopg2.OperationalError, e:
            log.warning("Unable to connect: %s", e)
            raise
        return conn, cursor

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open(self.isolation_serializable)
        stmt = """
        PREPARE get_latest_tid AS
        SELECT tid
        FROM transaction
        ORDER BY tid DESC
        LIMIT 1
        """
        cursor.execute(stmt)
        return conn, cursor

