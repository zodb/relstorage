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
from __future__ import print_function
import logging

from zope.interface import implementer

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters.dbiter import HistoryFreeDatabaseIterator
from relstorage.adapters.dbiter import HistoryPreservingDatabaseIterator
from relstorage.adapters.interfaces import IRelStorageAdapter

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
from relstorage._compat import iteritems

from . import _mysql_drivers
from ._abstract_drivers import _select_driver

log = logging.getLogger(__name__)

def select_driver(options=None):
    return _select_driver(options or Options(), _mysql_drivers)

@implementer(IRelStorageAdapter)
class MySQLAdapter(object):
    """MySQL adapter for RelStorage."""

    def __init__(self, options=None, **params):
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history
        self._params = params

        driver = select_driver(options)
        log.debug("Using driver %r", driver)

        self.connmanager = MySQLdbConnectionManager(
            driver,
            params=params,
            options=options,
            )
        self.runner = ScriptRunner()
        self.locker = MySQLLocker(
            options=options,
            lock_exceptions=driver.lock_exceptions,
            )
        self.schema = MySQLSchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            keep_history=self.keep_history,
            )
        self.mover = ObjectMover(
            database_type='mysql',
            options=options,
            Binary=driver.Binary,
            )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)
        self.oidallocator = MySQLOIDAllocator()
        self.txncontrol = MySQLTransactionControl(
            keep_history=self.keep_history,
            Binary=driver.Binary,
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
        p = sorted(iteritems(p))
        parts.extend('%s=%r' % item for item in p)
        return ", ".join(parts)


class MySQLdbConnectionManager(AbstractConnectionManager):

    isolation_read_committed = "ISOLATION LEVEL READ COMMITTED"
    isolation_repeatable_read = "ISOLATION LEVEL REPEATABLE READ"

    def __init__(self, driver, params, options):
        self._params = params.copy()
        self.disconnected_exceptions = driver.disconnected_exceptions
        self.close_exceptions = driver.close_exceptions
        self.use_replica_exceptions = driver.use_replica_exceptions
        self._db_connect = driver.connect
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
                conn = self._db_connect(**params)
                cursor = conn.cursor()
                cursor.arraysize = 64
                if transaction_mode:
                    conn.autocommit(True)
                    cursor.execute(
                        "SET SESSION TRANSACTION %s" % transaction_mode)
                    conn.autocommit(False)
                # Don't try to decode pickle states as UTF-8 (or
                # whatever the environment is configured as); See
                # https://github.com/zodb/relstorage/issues/57
                cursor.execute("SET NAMES binary")
                conn.replica = replica
                return conn, cursor
            except self.use_replica_exceptions as e:
                if replica is not None:
                    next_replica = next(replica_selector)
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
