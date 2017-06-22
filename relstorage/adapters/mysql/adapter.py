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
from __future__ import print_function, absolute_import

from .._abstract_drivers import _select_driver
from .._util import query_property
from ..dbiter import HistoryFreeDatabaseIterator
from ..dbiter import HistoryPreservingDatabaseIterator
from ..interfaces import IRelStorageAdapter
from ..poller import Poller
from ..scriptrunner import ScriptRunner

from . import drivers
from .connmanager import MySQLdbConnectionManager
from .locker import MySQLLocker
from .mover import MySQLObjectMover
from .mover import to_prepared_queries
from .oidallocator import MySQLOIDAllocator
from .packundo import MySQLHistoryFreePackUndo
from .packundo import MySQLHistoryPreservingPackUndo
from .schema import MySQLSchemaInstaller
from .stats import MySQLStats
from .txncontrol import MySQLTransactionControl

from relstorage._compat import iteritems
from relstorage.options import Options
from zope.interface import implementer
import logging

log = logging.getLogger(__name__)

def select_driver(options=None):
    return _select_driver(options or Options(), drivers)

@implementer(IRelStorageAdapter)
class MySQLAdapter(object):
    """MySQL adapter for RelStorage."""
    # pylint:disable=too-many-instance-attributes

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
        self.mover = MySQLObjectMover(
            database_type='mysql',
            options=options,
            Binary=driver.Binary,
        )
        self.connmanager.add_on_store_opened(self.mover.on_store_opened)
        self.connmanager.add_on_load_opened(self.mover.on_load_opened)
        self.oidallocator = MySQLOIDAllocator(self.connmanager.disconnected_exceptions[0])
        self.txncontrol = MySQLTransactionControl(
            keep_history=self.keep_history,
            Binary=driver.Binary,
        )

        self.poller = Poller(
            poll_query='EXECUTE get_latest_tid',
            keep_history=self.keep_history,
            runner=self.runner,
            revert_when_stale=options.revert_when_stale,
        )

        self.connmanager.add_on_load_opened(self._prepare_get_latest_tid)
        self.connmanager.add_on_store_opened(self._prepare_get_latest_tid)

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
            keep_history=self.keep_history
        )

    _get_latest_tid_queries = (
        "SELECT MAX(tid) FROM transaction",
        "SELECT MAX(tid) FROM object_state",
    )

    _prepare_get_latest_tid_queries = to_prepared_queries(
        'get_latest_tid',
        _get_latest_tid_queries)

    _prepare_get_latest_tid_query = query_property('_prepare_get_latest_tid')

    def _prepare_get_latest_tid(self, cursor, restart=False):
        if restart:
            return
        stmt = self._prepare_get_latest_tid_query
        cursor.execute(stmt)

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
