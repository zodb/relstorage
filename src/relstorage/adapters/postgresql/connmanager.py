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

from ..._compat import metricmethod
from ..connmanager import AbstractConnectionManager

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
        # XXX: SERIALIZABLE isn't allowed on streaming replicas
        # (https://www.enterprisedb.com/blog/serializable-postgresql-11-and-beyond)
        # Do we really need SERIALIZABLE? Wouldn't REPEATABLE READ be
        # sufficient? That's what we use on MySQL. OTOH, SERIALIZABLE
        # is much more efficient on PostgreSQL than other systems
        # and has some nice features.

        # If we could get the store connections to work in
        # SERIALIZABLE mode, we'd probably be able to stop the
        # explicit locking altogether. With judicious use of
        # savepoints, and proper re-raising of ConflictError, that
        # might be possible.

        # Set the transaction to READ ONLY mode. This lets
        # transactions (especially SERIALIZABLE) elide some locks.

        # TODO: Enable deferrable transactions if we stay in
        # serializable, read only mode. This should generally be
        # faster, as the *only* serializable transactions we have
        # should be READ ONLY.
        return self.open(
            self.isolation_load,
            read_only=True,
            deferrable=False,
            replica_selector=self.ro_replica_selector,
            application_name='RS load'
        )
