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
from . import interfaces

logger = __import__('logging').getLogger(__name__)

__all__ = [
    'LoadConnection',
    'StoreConnection',
]


class AbstractManagedConnection(object):

    _NEW_CONNECTION_NAME = None
    _RESTART_NAME = None
    _ROLLBACK_NAME = 'rollback_quietly'

    def __init__(self, connmanager):
        self.connection = None
        self.connmanager = connmanager
        self._cursor = None
        self.active = False
        self._new_connection = getattr(connmanager, self._NEW_CONNECTION_NAME)
        self._restart = getattr(connmanager, self._RESTART_NAME)
        self._rollback = getattr(connmanager, self._ROLLBACK_NAME)

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
        if not clean_rollback:
            self.drop()

        self.on_rolledback(self.connection, self._cursor)
        return clean_rollback

    def open_if_needed(self):
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
        self.on_opened(new_conn, new_cursor)

    @staticmethod
    def __noop(*args):
        "does nothing"

    def restart(self):
        """
        Restart the connection if there is any chance that it has any associated state.
        """
        if not self:
            assert not self.active, self.__dict__
            return

        # If we've never accessed the cursor, we shouldn't have any
        # state to restart.
        if not self.active and 'cursor' not in self.__dict__:
            return

        self.active = False
        self.restart_and_call(self.__noop)


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
            the connection and try again if an exception is raised if *f*. If False,
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
        except self.connmanager.driver.disconnected_exceptions as e:
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
            ss_cursor.close()

    def __repr__(self):
        return "<%s at 0x%x active=%s, conn=%r cur=%r>" % (
            self.__class__.__name__,
            id(self),
            self.active,
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
