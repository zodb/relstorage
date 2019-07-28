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

    def __init__(self, connmanager):
        self.connection = None
        self.connmanager = connmanager
        self._cursor = None
        self.active = False
        self._new_connection = getattr(connmanager, self._NEW_CONNECTION_NAME)
        self._restart = getattr(connmanager, self._RESTART_NAME)

    # Hook functions
    on_opened = staticmethod(lambda conn, cursor: None)
    # on_rolledback can be called with None, None if an error occurred
    # during the rollback.
    on_rolledback = on_opened
    on_first_use = on_opened

    def __bool__(self):
        return self.connection is not None and self._cursor is not None

    __nonzero__ = __bool__

    @Lazy
    def cursor(self):
        if not self.active or not self:
            # XXX: If we've been explicitly dropped, do we always want to
            # automatically re-open? Probably not; bad things could happen:
            # a load connection could skip into the future, and a store connection
            # could lose temp tables.
            conn, cursor = self.open_if_needed()
            self.active = True
            self.on_first_use(conn, cursor)
        return self._cursor

    def drop(self):
        self.active = False
        conn, cursor = self.connection, self._cursor
        self.connection, self._cursor = None, None
        self.__dict__.pop('cursor', None)
        self.connmanager.rollback_and_close(conn, cursor)

    # TODO: Better tracking of whether we are in a transaction
    # and need to rollback or not. We currently send lots of
    # extra ROLLBACK commands. That costs network time, and
    # on Postgres it results in WARNINGS in the server logs.

    def commit(self):
        if self.connection is not None:
            self.connmanager.commit(self.connection, self._cursor)

    def rollback_quietly(self):
        """
        Make the connection inactive and quietly roll it back.

        If an error occurs, drop the connection.
        """
        clean_rollback = True
        self.active = False
        if self.connection is None:
            return clean_rollback

        conn = self.connection
        cur = self._cursor
        self.__dict__.pop('cursor', None)
        clean_rollback = self.connmanager.rollback_quietly(conn, cur)
        if not clean_rollback:
            self.drop()

        self.on_rolledback(self.connection, self._cursor)
        return clean_rollback

    def open_if_needed(self):
        if not self:
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

    def __noop(self, *args):
        "does nothing"

    def restart(self):
        """
        Unconditionally restart the connection.
        """
        self.active = False
        if not self:
            return

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
        def callback(conn, cursor, fresh, *args, **kwargs):
            assert conn is self.connection and cursor is self._cursor
            if not fresh:
                # This could raise a disconnected exception, or a
                # ReplicaClosedException.
                self._restart(conn, cursor)
                self.on_rolledback(conn, cursor)
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
        if not self:
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
            logger.warning("Reconnecting %s: %s", e, self)
            self.drop()
            try:
                self._open_connection()
            except:
                logger.exception("Reconnect %s failed", self)
                raise
            logger.info("Reconnected %s", self)
            return f(self.connection, self._cursor, True, *args, **kwargs)

    @contextlib.contextmanager
    def isolated_connection(self):
        conn, cursor = self._new_connection()
        try:
            yield cursor
        finally:
            self.connmanager.rollback_and_close(conn, cursor)


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
