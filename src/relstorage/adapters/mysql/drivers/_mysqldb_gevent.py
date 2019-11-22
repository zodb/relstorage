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

# Perhaps pylint gets confused because of the mysqldb module in this package?
# pylint:disable=no-name-in-module,import-error
from MySQLdb.connections import Connection as BaseConnection
from MySQLdb.cursors import SSCursor

from ...drivers import GeventConnectionMixin
from . import IterateFetchmanyMixin

class Cursor(IterateFetchmanyMixin,
             SSCursor):
    """
    ``SSCursor.execute`` and ``SSCursor.callproc`` call
    ``SSCursor._query``; ``self_query`` and ``self.nextset`` both call
    ``self._get_result``. That produces a ``ResultObject``, the
    constructor of which calls the libmysql function
    ``mysql_use_result()`` and ``mysql_more_results``. The ``ResultObject`` is stored in
    ``self._result`` and has a ``has_next`` property that's statically
    determined (without reading from the network) saying whether
    there is another result set to read by ``mysql_more_results``.

    Before ``SSCursor.nextset()`` calls ``_get_result`` it calls
    the Connection's ``next_result`` method to advance the connection
    state. That method may block.
    """

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

    def execute(self, query, args=None):
        # The super implementation likes to call nextset() in a loop to
        # scroll through anything left dangling. But we promise not to do that
        # so we can save some effort.
        self.nextset = _noop # pylint:disable=attribute-defined-outside-init
        try:
            return SSCursor.execute(self, query, args)
        finally:
            del self.nextset

    def fetchall(self):
        if self.connection.is_in_critical_phase():
            return SSCursor.fetchall(self)
        sleep = self.connection.gevent_sleep
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

def _noop(_duration=None):
    "Does nothing"

class AbstractConnection(GeventConnectionMixin):
    default_cursor = Cursor

    # pylint:disable=method-hidden

    def query(self, query):
        # From the mysqlclient implementation:
        # "Since _mysql releases the GIL while querying, we need immutable buffer"
        # RelStorage never passes a bytearray, but third-parties have
        # been known to use this connection implementation.
        if isinstance(query, bytearray):
            query = bytes(query)

        # Prior to mysqlclient 1.4, there was a 'waiter' Connection
        # argument that could be used to do this, but it was removed.
        # So we implement it ourself.
        self.gevent_wait_write()
        self.send_query(query)

        self.gevent_wait_read()
        self.read_query_result()

    # The default implementations of 'rollback' and 'commit' use only
    # C API functions `mysql_rollback` and `mysql_commit`; it doesn't
    # touch any internal state. Those in turn simply call
    # `mysql_real_query("rollback")` and `mysql_real_query("commit")`.
    # That's a synchronous function that waits for the result to be
    # ready. We don't want to block like that (commit could
    # potentially take some time.)

    def rollback(self):
        self.query(b'ROLLBACK')

    def commit(self):
        self.query(b'COMMIT')

    ###
    # Critical sections
    ###
    def enter_critical_phase_until_transaction_end(self):
        # Must be idempotent
        self.commit = self._critical_commit
        self.rollback = self._critical_rollback
        self.query = self._critical_query
        self.gevent_sleep = _noop

    def is_in_critical_phase(self):
        return 'gevent_sleep' in self.__dict__

    def __exit_critical_phase(self):
        # Unconditional, assumes we're in one.
        del self.rollback
        del self.commit
        del self.query
        del self.gevent_sleep

    def exit_critical_phase(self):
        # Public, may or may not be in one.
        if self.is_in_critical_phase():
            self.__exit_critical_phase()

    def _critical_query(self, query):
        return BaseConnection.query(self, query)

    def _critical_rollback(self):
        try:
            self._direct_rollback()
        finally:
            self.__exit_critical_phase()

    def _critical_commit(self):
        try:
            self._direct_commit()
        finally:
            self.__exit_critical_phase()


class Connection(AbstractConnection,
                 BaseConnection):
    """
    gevent (mostly) compatible connection.

    .. rubric Blocking

    Where, when, and how long we could block the event loop is complex
    and depends on what we're doing.

    "how long" is the easy part. That's the value of the
    ``read_timeout`` constructor argument, times two (which must be >
    0; 0 means infinite). libmysql automatically includes a retry
    after an EAGAIN. If the timeout is set, then after the second try
    mysqlclient will raise ``OperationalError: 2013, Lost Connection``
    and the internal state of the connection is broken.

    This retry is done using ``vio_io_wait``, which calls
    select/epoll/kevent. We'd really like to hook into that and make
    it use the gevent loop in all cases, but I couldn't find a way to
    do that. Setting the socket to nonblocking mode does nothing
    except lead to unexpected disconnects.

    "Where" depends on the query and where it blocks. The server will
    always wait to send us the first result set, so our ``query()``
    function can successfully ``gevent_wait_read()`` to detect when
    the first batch of results arrive. If the query is going to
    produce multiple result sets (either because it contained multiple
    statements like "SELECT ...; SELECT ...;" or because it was a
    ``CALL``), the blocking (if any) will occur during
    ``next_result``. It's not possible for this object to determine
    whether the network IO done by ``next_result`` will block, or,
    indeed, if it will even do any network IO.

    You'd think it would be possible to predict whether it would need
    to block or not, or do network IO, based on whether we have
    statically determined that there's a next result set
    (``ResultObject.has_next``). That turns out not to be correct. For
    example, given a stored procedure that will return three result
    sets (the first two from queries that may return no rows), after
    executing the query, the initial ``ResultObject`` will have
    ``has_next`` of ``False`` --- but ``self.next_result()`` returns
    0, indicating that there are in fact more results (apparently
    ``mysql_more_results`` is buggy). This next ``ResultObject`` will
    have a correct value for ``has_next``, indicating a third result
    is available, but ``gevent_wait_read`` will never return,
    suggesting that the results have already been buffered.

    The upshot is that stored procedures should either return no more
    than one result set, or if there is going to be blocking, they
    should block in the first result set. We can't properly deal with
    blocking that occurs at the arbitrary time of ``next_result``
    without the possibility of deadlocking. (See
    ``GeventConnectionMixin``.)
    """

    _direct_rollback = BaseConnection.rollback
    _direct_commit = BaseConnection.commit

    def __delattr__(self, name):
        # BaseConnection has a delattr that forbids
        # all deletion. Not helpful.
        if name in self.__dict__:
            self.__dict__.pop(name)
        else:
            BaseConnection.__delattr__(self, name)

    def gevent_check_write(self):
        # This protocol is a request/reply nature, so reason dictates
        # that we will *always* be able to write to the socket: We
        # don't get control back until we've sent everything we need
        # to send.
        # Empirical testing backs that up. This is always true. So we
        # can save the syscall and just go.
        return True
