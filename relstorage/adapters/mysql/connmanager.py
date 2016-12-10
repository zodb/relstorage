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
"""
MySQL adapter for RelStorage.
"""
from __future__ import print_function, absolute_import
import logging

from ..connmanager import AbstractConnectionManager

log = logging.getLogger(__name__)

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
             replica_selector=None, **kwargs):
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
