# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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
Connection management for the storage layer.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib

from zope.interface import implementer

from .._compat import wraps
from .._util import Lazy
from .._util import get_positive_integer_from_environ
from . import interfaces

logger = __import__('logging').getLogger(__name__)

__all__ = [
    'LoadConnection',
    'StoreConnection',
    'StoreConnectionPool',
]


class AbstractManagedConnection(object):

    _NEW_CONNECTION_NAME = None
    _RESTART_NAME = None
    _ROLLBACK_NAME = 'rollback_quietly'

    def __init__(self, connmanager):
        self.connection = None
        self.connmanager = connmanager
        self._cursor = None
        self._connection_description = None
        self.active = False
        self._new_connection = getattr(connmanager, self._NEW_CONNECTION_NAME)
        self._restart = getattr(connmanager, self._RESTART_NAME)
        self._rollback = getattr(connmanager, self._ROLLBACK_NAME)
        # TODO: connmanager has this private, promote to public.
        self._closed_exceptions = connmanager._ignored_exceptions

    # Hook functions
    on_opened = staticmethod(lambda conn, cursor: None)
    # on_rolledback can be called with None, None if an error occurred
    # during the rollback.
    on_rolledback = on_opened
    on_first_use = on_opened

    def __bool__(self):
        """
        This object is true if it has an open connection and cursor that
        has previously been accessed and the ``on_first_use`` hook has been
        called and so we've established a database snapshot.

        This is useful to keep from polling a load connection, for example,
        when not explicitly asked for.
        """
        return self.connection is not None and 'cursor' in self.__dict__

    __nonzero__ = __bool__

    def get_cursor(self):
        if not self.active or self._cursor is None:
            # XXX: If we've been explicitly dropped, do we always want to
            # automatically re-open? Probably not; bad things could happen:
            # a load connection could skip into the future, and a store connection
            # could lose temp tables.
            conn, cursor = self.open_if_needed()
            self.on_first_use(conn, cursor)
            self.active = True
        return self._cursor

    cursor = Lazy(get_cursor, 'cursor')

    def enter_critical_phase_until_transaction_end(self):
        """
        Given an already opened connection, cause it to attempt to
        raise it's priority and return results faster.

        This mostly has meaning for gevent drivers.
        """
        self.connmanager.driver.enter_critical_phase_until_transaction_end(
            *self.open_if_needed()
        )

    def exit_critical_phase(self):
        if self.connection is not None:
            self.connmanager.driver.exit_critical_phase(self.connection, self._cursor)

    def drop(self):
        self.active = False
        conn, cursor = self.connection, self._cursor
        self.connection, self._cursor = None, None
        self.__dict__.pop('cursor', None)
        self.connmanager.rollback_and_close(conn, cursor)

    def commit(self, force=False):
        if self.connection is not None:
            self.connmanager.commit(self.connection, self._cursor, force=force)
            self.__dict__.pop('cursor', None)

    def rollback_quietly(self):
        """
        Make the connection inactive and quietly roll it back.

        If an error occurs, drop the connection.
        """
        clean_rollback = True
        self.active = False
        if self.connection is None:
            assert self._cursor is None
            assert 'cursor' not in self.__dict__
            return clean_rollback

        conn = self.connection
        cur = self._cursor
        self.__dict__.pop('cursor', None) # force on_first_use to be called.
        clean_rollback = self._rollback(conn, cur)
        assert clean_rollback is not None
        if not clean_rollback:
            self.drop()
            assert self.connection is None and self._cursor is None

        self.on_rolledback(self.connection, self._cursor)
        return clean_rollback

    def open_if_needed(self):
        # XXX: This bypasses putting _cursor into self.__dict__
        # as cursor, so that this object appears true.
        # That's a bit surprising. Should it be this way?
        if self.connection is None or self._cursor is None:
            self.drop()
            self._open_connection()
        return self.connection, self._cursor

    def _open_connection(self):
        """
        Open a new connection, assigning it to ``connection`` and ``cursor``
        """
        new_conn, new_cursor = self._new_connection()
        self.connection, self._cursor = new_conn, new_cursor
        self._connection_description = self.connmanager.describe_connection(new_conn, new_cursor)
        self.on_opened(new_conn, new_cursor)

    @staticmethod
    def __noop(*args):
        "does nothing"

    def _restart_connection(self):
        "Restart just the connection when we have no cursor."

    def restart(self):
        """
        Restart the connection if there is any chance that it has any associated state.

        If the connection has been disconnected, this may drop it.
        """
        try:
            if not self:
                assert not self.active, self.__dict__
                if self.connection is not None: # But we have no cursor
                    # We do this so that if the connection has closed
                    # itself (or been closed by a unit test) we can detect
                    # that and restart automatically. We only actually do
                    # anything there for store connections.
                    self._restart_connection()
                return

            self.active = False
            self.restart_and_call(self.__noop)
        except self._closed_exceptions:
            # Uh-oh, the thing we wanted went away.
            self.drop()

    def restart_and_call(self, f, *args, **kw):
        """
        Restart the connection (roll it back) and call a function
        after doing this.

        This may drop and re-connect the connection if necessary.

        :param callable f:
            The function to call: ``f(conn, cursor, *args, **kwargs)``.
            May be called up to twice if it raises a disconnected exception
            on the first try.

        :return: The return value of ``f``.
        """
        @wraps(f)
        def callback(conn, cursor, fresh, *args, **kwargs):
            assert conn is self.connection and cursor is self._cursor
            if not fresh:
                # We need to restart.
                needs_rollback = 'cursor' in self.__dict__
                # This could raise a disconnected exception, or a
                # ReplicaClosedException.
                self._restart(conn, cursor, needs_rollback)
                self.on_rolledback(conn, cursor)
            if f == self.on_first_use:
                self.cursor = cursor
            return f(conn, cursor, *args, **kwargs)

        return self.call(callback, True, *args, **kw)

    def call(self, f, can_reconnect, *args, **kwargs):
        """
        Call a function with the cursor, connecting it if needed.
        If a connection is already open, use that without rolling it back.

        Note that this does not count as a first usage and won't invoke
        that callback.

        :param callable f: Function to call
            ``f(conn, cursor, fresh_connection_p, *args, **kwargs)``.
            The function may be called up to twice, if the *fresh_connection_p* is false
            on the first call and a disconnected exception is raised.
        :keyword bool can_reconnect: If True, then we will attempt to reconnect
            the connection and try again if a disconnected exception is raised in *f*. If False,
            we let that exception propagate. For example, if a transaction is in progress,
            set this to false.
        """
        fresh_connection = False
        if self.connection is None or self._cursor is None:
            # We're closed or disconnected. Start a new connection entirely.
            self.drop()
            self._open_connection()
            fresh_connection = True
            # If we just connected no point in trying again.
            can_reconnect = False

        try:
            return f(self.connection, self._cursor, fresh_connection, *args, **kwargs)
        except self._closed_exceptions as e:
            # XXX: This feels like it's factored wrong.
            if not can_reconnect:
                raise
            logger.warning("Disconnected (%s) when running %s; attempting reconnect",
                           e, f, exc_info=True)
            self.drop()
            try:
                self._open_connection()
            except:
                logger.exception("Reconnect %s failed", self)
                raise
            logger.info("Reconnected %s to run %s", self, f)
            return f(self.connection, self._cursor, True, *args, **kwargs)

    @contextlib.contextmanager
    def isolated_connection(self):
        conn, cursor = self._new_connection()
        try:
            yield cursor
        finally:
            self.connmanager.rollback_and_close(conn, cursor)

    @contextlib.contextmanager
    def server_side_cursor(self):
        conn, _ = self.open_if_needed()
        ss_cursor = self.connmanager.driver.cursor(conn, server_side=True)
        try:
            yield ss_cursor
        finally:
            self.connmanager.close(cursor=ss_cursor)

    def __repr__(self):
        return "<%s at 0x%x active=%s description=%s conn=%r cur=%r>" % (
            self.__class__.__name__,
            id(self),
            self.active,
            self._connection_description,
            self.connection,
            self._cursor
        )


@implementer(interfaces.IManagedLoadConnection)
class LoadConnection(AbstractManagedConnection):

    __slots__ = ()

    _NEW_CONNECTION_NAME = 'open_for_load'
    _RESTART_NAME = 'restart_load'


@implementer(interfaces.IManagedStoreConnection)
class StoreConnection(AbstractManagedConnection):

    __slots__ = ()

    _NEW_CONNECTION_NAME = 'open_for_store'
    _RESTART_NAME = 'restart_store'
    _ROLLBACK_NAME = 'rollback_store_quietly'

    def begin(self):
        self.connmanager.begin(*self.open_if_needed())

    def _restart_connection(self):
        if self.rollback_quietly():
            # If we failed to rollback, we dropped the connection,
            # so there's no restarting.
            self.connmanager.restart_store(self.connection,
                                           self._cursor,
                                           needs_rollback=False)


class PrePackConnection(StoreConnection):
    __slots__ = ()
    _NEW_CONNECTION_NAME = 'open_for_pre_pack'


@implementer(interfaces.IManagedDBConnection)
class ClosedConnection(object):
    """
    Represents a permanently closed connection.
    """
    __slots__ = ()

    cursor = None
    connection = None

    def __init__(self, *args):
        "We have no state."

    def drop(self):
        "Does nothing."

    rollback_quietly = drop

    __bool__ = __nonzero__ = lambda self: False

    def isolated_connection(self, *args, **kwargs):
        raise NotImplementedError

    restart_and_call = isolated_connection
    enter_critical_phase_until_transaction_end = isolated_connection


class StoreConnectionPool(object):
    """
    A thread-safe pool of `StoreConnection` objects.

    Connections are opened on demand; opening a connection on demand
    does not block.

    By default, it will keep around a `StoreConnection` for every instance
    of a RelStorage that has ever used one, which ordinarily corresponds to the
    ZODB Connection pool size. It can be made to keep a smaller (but not larger)
    number around by setting ``MAX_STORE_CONNECTIONS_IN_POOL``.
    """

    MAX_STORE_CONNECTIONS_IN_POOL = get_positive_integer_from_environ(
        'RS_MAX_POOLED_STORE_CONNECTIONS',
        None,
        logger=logger
    )

    def __init__(self, connmanager):
        import threading
        self._lock = threading.Lock() # Not RLock, cheaper that way
        self._connmanager = connmanager
        # LIFO of connections: Next to use is at the right end.
        self._connections = []
        self._count = 1
        self._factory = StoreConnection

    # MVCC protocol
    def new_instance(self):
        with self._lock:
            self._count += 1
        return self

    def release(self):
        with self._lock:
            self._count -= 1
            self._shrink()

    def close(self):
        with self._lock:
            self._count = 0
            self._factory = ClosedConnection
            self._shrink()
            self._connections = ()

    @contextlib.contextmanager
    def borrowing(self, commit=False):
        # self.borrow() either correctly returns a Connection,
        # or guarantees not to leak it in the event of on exception.
        # We want such an exception to propagate.
        conn = self.borrow()
        rollback = True
        try:
            yield conn
            if commit:
                conn.commit()
                rollback = False
        finally:
            self._replace(conn, rollback)

    def borrow(self):
        """
        Returns a begun, not-None ``StoreConnection``, or raises an exception.

        If it raises an exception, the internals are still consistent and
        no connection is leaked.
        """
        conn = None
        with self._lock:
            if self._connections:
                # Can't rely on the GIL for this, need to do the two-step check
                # because _shrink isn't atomic.
                conn = self._connections.pop()

        try:
            if conn is None:
                conn = self._factory(self._connmanager)
            else:
                conn.restart()

            conn.begin()
        except:
            # Whether or not we took it from the pool,
            # if we got an error restarting or beginning,
            # just drop it; don't put it in the pool.
            # conn could still be None if self._factory raised.
            logger.exception("Failed to borrow a store connection: %s", conn)
            if conn is not None:
                conn.drop()
            raise

        return conn

    def replace(self, connection):
        self._replace(connection, True)

    def _replace(self, connection, needs_rollback):
        if needs_rollback:
            clean_rollback = connection.rollback_quietly()
        else:
            clean_rollback = True
        if not clean_rollback:
            connection.drop()
        else:
            connection.exit_critical_phase()
            with self._lock:
                if  connection.connection is not None:
                    # If it's been dropped, don't add it; better just start
                    # fresh.
                    self._connections.append(connection)
                self._shrink()

    def _shrink(self):
        # Call while holding the lock, after putting a connection
        # back in the pool.
        # Limits the number of pooled connections to be no more than
        # ``instance_count`` (i.e., one per open RelStorage), or ``MAX_STORE_CONNECTIONS_IN_POOL``,
        # if set and if less than instance_count
        keep_connections = min(self._count, self.MAX_STORE_CONNECTIONS_IN_POOL or self._count)

        while len(self._connections) > keep_connections and self._connections:
            # Pop off the oldest connection to eliminate.
            #
            # Because of the MVCC protocol, we will have an instance
            # of this class for the "root" object associated with the
            # ZODB.DB, that is usually never actually used in a
            # transaction, plus one for every extent ZODB
            # Connection/RelStorage instance. That first index might be a very old
            # connection indeed, and could continue to exist even if all
            # ZODB Connection objects have been closed. To mitigate the risk of that
            # causing a problem, remove starting with the oldest.
            conn = self._connections.pop(0)
            conn.drop() # TODO: This could potentially be slow? Might
                        # want to do this outside the lock.
            conn.connmanager = None # It can't be opened again.

    def drop_all(self):
        with self._lock:
            while self._connections:
                conn = self._connections.pop()
                conn.drop()

    def hard_close_all_connections(self):
        # Testing only.
        for conn in self._connections:
            conn.connection.close()

    @property
    def pooled_connection_count(self):
        return len(self._connections)

    @property
    def instance_count(self):
        return self._count


class ClosedConnectionPool(object):

    def borrow(self):
        return ClosedConnection()

    def replace(self, connection):
        "Does Nothing"

    def new_instance(self):
        "Does nothing"

    release = new_instance
    close = release
    drop_all = release

    pooled_connection_count = instance_count = 0


class SingleConnectionPool(object):
    __slots__ = ('connection',)

    def __init__(self, connection):
        self.connection = connection

    @contextlib.contextmanager
    def borrowing(self, commit=False): # pylint:disable=unused-argument
        """
        The *commit* parameter is ignored
        """
        yield self.connection
