# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gevent import sleep as gevent_sleep

# pylint:disable=wrong-import-position,no-name-in-module,import-error
from MySQLdb.connections import Connection as BaseConnection
from MySQLdb.cursors import SSCursor

from relstorage.adapters.drivers import GeventConnectionMixin
from . import IterateFetchmanyMixin

class Cursor(IterateFetchmanyMixin, SSCursor):
    # Internally this calls mysql_use_result(). The source
    # code for that function has this comment: "There
    # shouldn't be much processing per row because mysql
    # server shouldn't have to wait for the client (and
    # will not wait more than 30 sec/packet)." Empirically
    # that doesn't seem to be true.


    # Somewhat surprisingly, if we just wait on read,
    # we end up blocking forever. This is because of the buffers
    # maintained inside the MySQL library: we might already have the
    # rows that we need buffered.

    # Blocking on write is pointless: by definition
    # we're here to read results so we can always
    # write. That just forces us to take a trip around
    # the event loop for no good reason.

    # Therefore, our best option to periodically yield
    # is to explicitly invoke gevent.sleep(). Without
    # any time given, it will yield to other ready
    # greenlets; only sometimes will it force a trip
    # around the event loop.

    # TODO: But if we're actually going to need to read data,
    # we want to wait until it arrives. Can we get that info
    # somehow? Can we develop a heuristic?

    # Somewhat amusingly, the low-levels of libmysqlclient use
    # kevent to do waiting: mysql_real_query -> cli_read_query_result ->
    # my_net_read -> net_read_packet -> via_read_buff -> vio_read
    # -> vio_io_wait -> kevent
    #
    # We do this *after* fetching results, not before, just in case
    # we only needed to make one fetch.

    def fetchall(self):
        sleep = self.connection.sleep
        if sleep is _noop:
            return SSCursor.fetchall(self)

        result = []
        fetch = self.fetchmany # calls _fetch_row with the arraysize
        while 1:
            # Even if self.rowcount is 0 we must still call
            # or we get the connection out of sync.
            rows = fetch()
            if not rows:
                break
            result.extend(rows)
            if self.rownumber == self.rowcount:
                # Avoid a useless extra trip at the end.
                break
            sleep()
        return result


# Prior to mysqlclient 1.4, there was a 'waiter' Connection
# argument that could be used to do this, but it was removed.
# So we implement it ourself.
def _noop():
    "Does nothing"

class Connection(GeventConnectionMixin,
                 BaseConnection):
    default_cursor = Cursor
    gevent_read_watcher = None
    gevent_write_watcher = None
    gevent_hub = None
    sleep = staticmethod(gevent_sleep)

    # pylint:disable=method-hidden

    def enter_critical_phase_until_transaction_end(self):
        # Must be idempotent
        self.commit = self._critical_commit
        self.rollback = self._critical_rollback
        self.query = self._critical_query
        self.sleep = _noop

    def exit_critical_phase_at_transaction_end(self):
        del self.rollback
        del self.commit
        del self.query
        del self.sleep

    def _critical_query(self, query):
        return BaseConnection.query(self, query)

    def query(self, query):
        # From the mysqlclient implementation:
        # "Since _mysql releases the GIL while querying, we need immutable buffer"
        if isinstance(query, bytearray):
            query = bytes(query)

        self.gevent_wait_write()
        self.send_query(query)

        self.gevent_wait_read()
        self.read_query_result()

    # The default implementations of 'rollback' and
    # 'commit' use only C API functions `mysql_rollback`
    # and `mysql_commit`; it doesn't touch any internal
    # state. Those in turn simply call
    # `mysql_real_query("rollback")` and
    # `mysql_real_query("commit")`. That's a synchronous
    # function that waits for the result to be ready. We
    # don't want to block like that (commit could
    # potentially take some time.)

    def _critical_rollback(self):
        try:
            BaseConnection.rollback(self)
        finally:
            self.exit_critical_phase_at_transaction_end()

    def rollback(self):
        self.query('rollback')

    def _critical_commit(self):
        try:
            BaseConnection.commit(self)
        finally:
            self.exit_critical_phase_at_transaction_end()

    def commit(self):
        self.query('commit')

    def __delattr__(self, name):
        # BaseConnection has a delattr that forbids
        # all deletion. Not helpful.
        if name in self.__dict__:
            self.__dict__.pop(name)
        else:
            BaseConnection.__delattr__(self, name)
