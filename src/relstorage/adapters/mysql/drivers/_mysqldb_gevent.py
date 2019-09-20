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

from gevent import socket
from gevent import get_hub
from gevent import sleep as gevent_sleep
wait = socket.wait # pylint:disable=no-member


# pylint:disable=wrong-import-position,no-name-in-module,import-error
from MySQLdb.connections import Connection as BaseConnection
from MySQLdb.cursors import SSCursor

from . import IterateFetchmanyMixin

class Cursor(IterateFetchmanyMixin, SSCursor):
    # Internally this calls mysql_use_result(). The source
    # code for that function has this comment: "There
    # shouldn't be much processing per row because mysql
    # server shouldn't have to wait for the client (and
    # will not wait more than 30 sec/packet)." Empirically
    # that doesn't seem to be true.

    def _noop(self):
        """Does nothing."""

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
    sleep = staticmethod(gevent_sleep)

    def fetchall(self):
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
            self.sleep()
        return result

    def enter_critical_phase_until_transaction_end(self):
        # May make multiple enters.
        if 'sleep' not in self.__dict__:
            self.sleep = self._noop
            self.connection.enter_critical_phase_until_transaction_end(self)

    def exit_critical_phase_at_transaction_end(self):
        del self.sleep


# Prior to mysqlclient 1.4, there was a 'waiter' Connection
# argument that could be used to do this, but it was removed.
# So we implement it ourself.
class Connection(BaseConnection):
    default_cursor = Cursor
    gevent_read_watcher = None
    gevent_write_watcher = None
    gevent_hub = None
    cursor_in_critical = None

    # pylint:disable=method-hidden

    def enter_critical_phase_until_transaction_end(self, cursor):
        assert self.cursor_in_critical is None
        self.cursor_in_critical = cursor
        # The queries, unfortunately, need to go through and allow switches without blocking.
        # Otherwise we can deadlock. So this makes it not very useful for our intention
        # of granting priority to a single greenlet and avoiding switches: other
        # greenlets can still come in and lock objects and hold those locks for a
        # period of time --- though, in testing, if we *do* make query() blocking,
        # under 60 greenlets committing 100 objects, we stop getting the 1.5s warning
        # about objects being locked too long: it does seem that the lock does something
        # there, but the overall time doesn't change much.
        self.commit = self._critical_commit
        self.rollback = self._critical_rollback

    def exit_critical_phase_at_transaction_end(self):
        del self.rollback
        del self.commit
        cur = self.cursor_in_critical
        del self.cursor_in_critical
        cur.exit_critical_phase_at_transaction_end()
        assert self.cursor_in_critical is None

    def check_watchers(self):
        # We can be used from more than one thread in a sequential
        # fashion.
        hub = get_hub()
        if hub is not self.gevent_hub:
            self.__close_watchers()

            fileno = self.fileno()
            hub = self.gevent_hub = get_hub()
            self.gevent_read_watcher = hub.loop.io(fileno, 1)
            self.gevent_write_watcher = hub.loop.io(fileno, 2)

    def __close_watchers(self):
        if self.gevent_read_watcher is not None:
            self.gevent_read_watcher.close()
            self.gevent_write_watcher.close()
            self.gevent_hub = None

    def query(self, query):
        # From the mysqlclient implementation:
        # "Since _mysql releases the GIL while querying, we need immutable buffer"
        if isinstance(query, bytearray):
            query = bytes(query)

        self.check_watchers()

        wait(self.gevent_write_watcher, hub=self.gevent_hub)
        self.send_query(query)

        wait(self.gevent_read_watcher, hub=self.gevent_hub)
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

    def close(self):
        self.__close_watchers()
        BaseConnection.close(self)

    def __delattr__(self, name):
        # BaseConnection has a delattr that forbids
        # all deletion. Not helpful.
        if name in self.__dict__:
            self.__dict__.pop(name)
        else:
            BaseConnection.__delattr__(self, name)
