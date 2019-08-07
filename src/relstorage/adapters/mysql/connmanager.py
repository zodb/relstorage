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
from __future__ import absolute_import
from __future__ import print_function

import logging

from ..connmanager import AbstractConnectionManager

log = logging.getLogger(__name__)

class MySQLdbConnectionManager(AbstractConnectionManager):

    # https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html

    # Each statement gets its own snapshot. Locking is minimized to
    # the exact index records found.
    isolation_read_committed = "ISOLATION LEVEL READ COMMITTED"
    # (DEFAULT) A snapshot is taken when the first statement is executed.
    # All reads are from that snapshot.
    isolation_repeatable_read = "ISOLATION LEVEL REPEATABLE READ"
    # READ ONLY was new in 5.6
    isolation_repeatable_read_ro = isolation_repeatable_read + ' , READ ONLY'

    def __init__(self, driver, params, options):
        self._params = params.copy()
        self._db_connect = driver.connect
        self._db_driver = driver
        self._fetchall_on_rollback = driver.fetchall_on_rollback
        super(MySQLdbConnectionManager, self).__init__(options, driver)

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
        # pylint:disable=arguments-differ
        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            replica = replica_selector.current()
            params = self._alter_params(replica)
        else:
            replica = None
            params = self._params

        while True:
            __traceback_info__ = {
                k: v if k != 'passwd' else '<*****>'
                for k, v in params.items()
            }

            try:
                conn = self._db_connect(**params)
                cursor = self._db_driver.cursor(conn)
                cursor.arraysize = 64
                if transaction_mode:
                    self._db_driver.set_autocommit(conn, True)
                    # Transaction isolation cannot be changed inside a
                    # transaction. 'SET SESSION' changes it for all
                    # upcoming transactions.
                    stmt = "SET SESSION TRANSACTION %s" % transaction_mode
                    __traceback_info__ = stmt
                    cursor.execute(stmt)
                    self._db_driver.set_autocommit(conn, False)
                conn.replica = replica
                return conn, cursor
            except self.driver.use_replica_exceptions as e:
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

    def _do_open_for_load(self):
        return self.open(
            self.isolation_repeatable_read_ro,
            replica_selector=self.ro_replica_selector)

    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).

        This overrides a method.
        """
        conn, cursor = self.open(self.isolation_read_committed)
        return conn, cursor
