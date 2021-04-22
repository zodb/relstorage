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
from __future__ import absolute_import
from __future__ import print_function

import logging

from ..._util import metricmethod
from ..connmanager import AbstractConnectionManager
from .util import backend_pid_for_connection

logger = logging.getLogger(__name__)


class Psycopg2ConnectionManager(AbstractConnectionManager):

    def __init__(self, driver, dsn, options):
        self._dsn = dsn
        self.isolation_read_committed = driver.ISOLATION_LEVEL_READ_COMMITTED
        self.isolation_serializable = driver.ISOLATION_LEVEL_SERIALIZABLE
        self.isolation_repeatable_read = driver.ISOLATION_LEVEL_REPEATABLE_READ
        self.keep_history = options.keep_history
        self._db_connect_with_isolation = driver.connect_with_isolation
        super(Psycopg2ConnectionManager, self).__init__(options, driver)


    def _alter_dsn(self, replica):
        """Alter the DSN to use the specified replica.

        The replica parameter is a string specifying either host or host:port.
        """
        if ':' in replica:
            host, port = replica.split(':')
            dsn = '%s host=%s port=%s' % (self._dsn, host, port)
        else:
            dsn = '%s host=%s' % (self._dsn, replica)
        return dsn

    @metricmethod
    def open(self, isolation=None, deferrable=False, read_only=False,
             replica_selector=None, application_name=None, **kwargs):
        """Open a database connection and return (conn, cursor)."""
        # pylint:disable=arguments-differ
        if isolation is None:
            isolation = self.isolation_store

        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            replica = replica_selector.current()
            dsn = self._alter_dsn(replica)
        else:
            replica = None
            dsn = self._dsn

        while True:
            try:
                # psycopg2 seems to have a cache of Connection objects
                # so closing one and then opening again often gets the same
                # object back.
                conn = self._db_connect_with_isolation(
                    dsn,
                    isolation=isolation,
                    deferrable=deferrable,
                    read_only=read_only,
                    application_name=application_name
                )
                cursor = self.cursor_for_connection(conn)
                conn.replica = replica
                return conn, cursor
            except self.driver.use_replica_exceptions as e:
                if replica is not None:
                    next_replica = replica_selector.next()
                    if next_replica is not None:
                        logger.warning("Unable to connect to replica %s: %s, "
                                       "now trying %s", replica, e, next_replica)
                        replica = next_replica
                        dsn = self._alter_dsn(replica)
                        continue
                logger.warning("Unable to connect: %s", e)
                raise

    def _do_open_for_load(self):
        # In RelStorage 1, 2 and <= 3.0b2, we used SERIALIZABLE isolation,
        # while MySQL used REPEATABLE READ and Oracle used SERIALIZABLE (but
        # only because of an apparent issue with RAC).
        #
        # Although SERIALIZABLE is much cheaper on PostgreSQL than
        # most other databases, it has its issues. Most notably,
        # SERIALIZABLE isn't allowed on streaming replicas
        # (https://www.enterprisedb.com/blog/serializable-postgresql-11-and-beyond),
        # and prior to PostgreSQL 12 it disables parallel queries (not
        # that we expect many queries to be something that can benefit
        # from parallel workers.)
        #
        # The differences that SERIALIZABLE brings shouldn't be
        # relevant as we don't run the write transactions at that
        # level, and we never try to commit this transaction. So it's
        # mostly just overhead for tracking read anomalies that can
        # never happen. And the standby issue became a problem
        # (https://github.com/zodb/relstorage/issues/376) and we
        # dropped down to REPEATABLE READ.

        # Of course, there's a chance that if we could get the store
        # connections to work in SERIALIZABLE mode, we'd be able to
        # stop the explicit locking altogether. With judicious use of
        # savepoints, and proper re-raising of ConflictError, that
        # might be possible.

        # Using READ ONLY mode lets transactions (especially
        # SERIALIZABLE) elide some locks. If we were SERIALIZABLE,
        # we'd probably also want to enable deferrable transactions as
        # there's special support to make them cheaper (but they might
        # have to wait on other serializable transactions, but since
        # our only other serializable transactions would be READ ONLY
        # that shouldn't matter.)

        return self.open(
            self.isolation_repeatable_read,
            read_only=True,
            deferrable=False,
            replica_selector=self.ro_replica_selector,
            application_name='RS: Load'
        )

    def describe_connection(self, conn, cursor):
        return {'backend_pid': backend_pid_for_connection(conn, cursor)}
