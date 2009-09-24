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

import logging
import MySQLdb

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.hpmover import HistoryPreservingObjectMover
from relstorage.adapters.locker import MySQLLocker
from relstorage.adapters.oidallocator import MySQLOIDAllocator
from relstorage.adapters.packundo import HistoryPreservingPackUndo
from relstorage.adapters.poller import Poller
from relstorage.adapters.schema import MySQLSchemaInstaller
from relstorage.adapters.scriptrunner import ScriptRunner
from relstorage.adapters.stats import MySQLStats
from relstorage.adapters.txncontrol import MySQLTransactionControl

log = logging.getLogger(__name__)

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (MySQLdb.OperationalError, MySQLdb.InterfaceError)

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions + (MySQLdb.ProgrammingError,)


class MySQLAdapter(object):
    """MySQL adapter for RelStorage."""

    keep_history = True

    def __init__(self, **params):
        self.connmanager = MySQLdbConnectionManager(params)
        self.runner = ScriptRunner()
        self.locker = MySQLLocker((MySQLdb.DatabaseError,))
        self.schema = MySQLSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
            )
        self.mover = HistoryPreservingObjectMover(
            database_name='mysql',
            runner=self.runner,
            Binary=MySQLdb.Binary,
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = MySQLOIDAllocator()
        self.txncontrol = MySQLTransactionControl(
            Binary=MySQLdb.Binary,
            )
        self.poller = Poller(
            poll_query="SELECT tid FROM transaction ORDER BY tid DESC LIMIT 1",
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
        self.stats = MySQLStats(
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


class MySQLdbConnectionManager(AbstractConnectionManager):

    isolation_read_committed = "ISOLATION LEVEL READ COMMITTED"
    isolation_repeatable_read = "ISOLATION LEVEL REPEATABLE READ"

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, params):
        self._params = params.copy()

    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED"):
        """Open a database connection and return (conn, cursor)."""
        try:
            conn = MySQLdb.connect(**self._params)
            cursor = conn.cursor()
            cursor.arraysize = 64
            if transaction_mode:
                conn.autocommit(True)
                cursor.execute("SET SESSION TRANSACTION %s" % transaction_mode)
                conn.autocommit(False)
            return conn, cursor
        except MySQLdb.OperationalError, e:
            log.warning("Unable to connect: %s", e)
            raise

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open(self.isolation_repeatable_read)

