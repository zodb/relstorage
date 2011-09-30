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
from zope.interface import implements

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryFreeDatabaseIterator
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.interfaces import IRelStorageAdapter
from relstorage.adapters.interfaces import ReplicaClosedException
from relstorage.adapters.locker import MySQLLocker
from relstorage.adapters.mover import ObjectMover
from relstorage.adapters.oidallocator import MySQLOIDAllocator
from relstorage.adapters.packundo import MySQLHistoryFreePackUndo
from relstorage.adapters.packundo import MySQLHistoryPreservingPackUndo
from relstorage.adapters.poller import Poller
from relstorage.adapters.schema import MySQLSchemaInstaller
from relstorage.adapters.scriptrunner import ScriptRunner
from relstorage.adapters.stats import MySQLStats
from relstorage.adapters.txncontrol import MySQLTransactionControl
from relstorage.options import Options

log = logging.getLogger(__name__)

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (
    MySQLdb.OperationalError,
    MySQLdb.InterfaceError,
    ReplicaClosedException,
    )

# close_exceptions contains the exception types to ignore
# when the adapter attempts to close a database connection.
close_exceptions = disconnected_exceptions + (MySQLdb.ProgrammingError,)


class MySQLAdapter(object):
    """MySQL adapter for RelStorage."""
    implements(IRelStorageAdapter)

    def __init__(self, options=None, **params):
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history
        self._params = params

        self.connmanager = MySQLdbConnectionManager(
            params=params,
            options=options,
            )
        self.runner = ScriptRunner()
        self.locker = MySQLLocker(
            options=options,
            lock_exceptions=(MySQLdb.DatabaseError,),
            )
        self.schema = MySQLSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
            )
        self.mover = ObjectMover(
            database_type='mysql',
            options=options,
            Binary=MySQLdb.Binary,
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = MySQLOIDAllocator()
        self.txncontrol = MySQLTransactionControl(
            keep_history=self.keep_history,
            Binary=MySQLdb.Binary,
            )

        if self.keep_history:
            poll_query="SELECT tid FROM transaction ORDER BY tid DESC LIMIT 1"
        else:
            poll_query="SELECT tid FROM object_state ORDER BY tid DESC LIMIT 1"
        self.poller = Poller(
            poll_query=poll_query,
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
        )

        if self.keep_history:
            self.packundo = MySQLHistoryPreservingPackUndo(
                database_type='mysql',
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
                )
            self.dbiter = HistoryPreservingDatabaseIterator(
                database_type='mysql',
                runner=self.runner,
                )
        else:
            self.packundo = MySQLHistoryFreePackUndo(
                database_type='mysql',
                connmanager=self.connmanager,
                runner=self.runner,
                locker=self.locker,
                options=options,
                )
            self.dbiter = HistoryFreeDatabaseIterator(
                database_type='mysql',
                runner=self.runner,
                )

        self.stats = MySQLStats(
            connmanager=self.connmanager,
            )

    def new_instance(self):
        return MySQLAdapter(options=self.options, **self._params)

    def __str__(self):
        parts = [self.__class__.__name__]
        if self.keep_history:
            parts.append('history preserving')
        else:
            parts.append('history free')
        p = self._params.copy()
        if 'passwd' in p:
            del p['passwd']
        p = p.items()
        p.sort()
        parts.extend('%s=%r' % item for item in p)
        return ", ".join(parts)


class MySQLdbConnectionManager(AbstractConnectionManager):

    isolation_read_committed = "ISOLATION LEVEL READ COMMITTED"
    isolation_repeatable_read = "ISOLATION LEVEL REPEATABLE READ"

    disconnected_exceptions = disconnected_exceptions
    close_exceptions = close_exceptions

    def __init__(self, params, options):
        self._params = params.copy()
        super(MySQLdbConnectionManager, self).__init__(options)

    def _alter_params(self, replica):
        """Alter the connection parameters to use the specified replica.

        The replica parameter is a string specifying either host or host:port.
        """
        params = self._params.copy()
        if ':' in replica:
            host, port = replica.split(':')
            params['host'] = host
            params['port'] = int(port)
        else:
            params['host'] = replica
        return params

    def open(self, transaction_mode="ISOLATION LEVEL READ COMMITTED",
            replica_selector=None):
        """Open a database connection and return (conn, cursor)."""
        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            replica = replica_selector.current()
            params = self._alter_params(replica)
        else:
            replica = None
            params = self._params

        while True:
            try:
                conn = MySQLdb.connect(**params)
                cursor = conn.cursor()
                cursor.arraysize = 64
                if transaction_mode:
                    conn.autocommit(True)
                    cursor.execute(
                        "SET SESSION TRANSACTION %s" % transaction_mode)
                    conn.autocommit(False)
                conn.replica = replica
                return conn, cursor
            except MySQLdb.OperationalError, e:
                if replica is not None:
                    next_replica = replica_selector.next()
                    if next_replica is not None:
                        log.warning("Unable to connect to replica %s: %s, "
                            "now trying %s", replica, e, next_replica)
                        replica = next_replica
                        params = self._alter_params(replica)
                        continue
                log.warning("Unable to connect: %s", e)
                raise

    def open_for_load(self):
        """Open and initialize a connection for loading objects.

        Returns (conn, cursor).
        """
        return self.open(self.isolation_repeatable_read,
            replica_selector=self.ro_replica_selector)

    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).

        This overrides a method.
        """
        conn, cursor = self.open()
        try:
            # This phase of packing works best with transactions
            # disabled.  It changes no user-facing data.
            conn.autocommit(True)
            return conn, cursor
        except:
            self.close(conn, cursor)
            raise
