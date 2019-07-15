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

logger = __import__('logging').getLogger(__name__)

class AbstractConnection(object):
    """
    Represents a database connection and its single cursor.

    If the connection is not open and presumed to be good,
    this object has a false value.

    "Restarting" a connection means to bring it to a current view of the
    database. Typically this means a rollback so that a new transaction
    can begin with a new MVCC snapshot.
    """

    __slots__ = (
        'connection',
        'cursor',
        'connmanager',
    )

    def __init__(self, connmanager):
        self.connection = None
        self.cursor = None
        self.connmanager = connmanager

    def __bool__(self):
        return self.connection is not None and self.cursor is not None

    __nonzero__ = __bool__

    def drop(self):
        """
        Unconditionally drop the connection.
        """
        conn, cursor = self.connection, self.cursor
        self.connection, self.cursor = None, None
        self.connmanager.rollback_and_close(conn, cursor)

    def rollback(self):
        """
        Rollback the connection.

        When this completes, the connection will be in a neutral state,
        not idle in a transaction.

        If an error occurs during rollback, the connection is dropped.
        """
        if not self:
            return

        try:
            self.connmanager.rollback(self.connection, self.cursor)
        except:
            self.drop()
            raise

    def open_if_needed(self):
        if not self:
            self.drop()
            self._open_connection()
        return self.connection, self.cursor

    def _restart(self):
        raise NotImplementedError

    def _open_connection(self):
        """
        Open a new connection, assigning it to ``connection`` and ``cursor``
        """
        new_conn, new_cursor = self._new_connection()
        self.connection, self.cursor = new_conn, new_cursor

    def _new_connection(self):
        """
        Open and return a new ``(connection, cursor)`` pair.
        """
        raise NotImplementedError

    def __noop(self, *args):
        "does nothing"

    def restart(self):
        """
        Unconditionally restart the connection.
        """
        self.restart_and_call(self.__noop)

    def restart_and_call(self, f, *args, **kw):
        """
        Restart the connection and call a function. This may
        drop and reload the connection if necessary.

        :param callable f: The function to call: ``f(conn, cursor, *args, **kwargs)``.
            May be called up to twice.
        :return: The return value of ``f``.
        """
        def callback(conn, cursor, fresh, *args, **kwargs):
            if not fresh:
                assert conn is self.connection and cursor is self.cursor
                self._restart()
            return f(conn, cursor, *args, **kwargs)

        return self.call(callback, True, *args, **kw)

    def call(self, f, can_reconnect, *args, **kwargs):
        """
        Call a function with the cursor, connecting it if needed.
        If a connection is already open, use that without rolling it back.

        :param callable f: Function to call
            ``f(store_conn, store_cursor, fresh_connection, *args, **kwargs)``.
            The function may be called up to twice, if the *fresh_connection* is false
            on the first call and a disconnected exception is raised.
        :keyword bool can_reconnect: If True, then we will attempt to reconnect
            the connection and try again if an exception is raised. If False,
            we let that exception propagate. For example, if a transaction is in progress,
            set this to false.
        """
        fresh_connection = False
        if not self:
            self.drop()
            self._open_connection()
            fresh_connection = True
            # If we just connected no point in trying again.
            can_reconnect = False

        try:
            return f(self.connection, self.cursor, fresh_connection, *args, **kwargs)
        except self.connmanager.disconnected_exceptions as e:
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
            return f(self.connection, self.cursor, True, *args, **kwargs)

    @contextlib.contextmanager
    def isolated_connection(self):
        """
        Context manager that opens a distinct connection and return
        its cursor.

        No matter what happens in the block, the connection will be
        dropped afterwards.
        """
        conn, cursor = self._new_connection()
        try:
            yield cursor
        finally:
            self.connmanager.rollback_and_close(conn, cursor)

class LoadConnection(AbstractConnection):

    __slots__ = ()

    def _new_connection(self):
        return self.connmanager.open_for_load()

    def _restart(self):
        self.connmanager.restart_load(self.connection, self.cursor)


class StoreConnection(AbstractConnection):

    __slots__ = ()

    def _new_connection(self):
        return self.connmanager.open_for_store()

    def _restart(self):
        self.connmanager.restart_store(self.connection, self.cursor)


class EventedConnectionWrapper(object):
    """
    A facade around a connection that keeps track of certain
    events about the state of the connection and
    handles those events automatically.

    Events are handled via callbacks on this object. You can subclass
    or you can assign to them.
    """

    # Assigning to hook attributes makes it more difficult to use
    # __slots__ because the method and slot conflict.

    def __init__(self, connection):
        """
        The connection should not already be open.
        """
        assert not connection
        self.__connection = connection
        self.__active = False
        self.isolated_connection = connection.isolated_connection
        self.restart_and_call = connection.restart_and_call

    def __bool__(self):
        return bool(self.__connection)

    __nonzero__ = __bool__

    @property
    def connection(self):
        return self.__connection.connection

    @property
    def cursor(self):
        if not self.__active or not self.__connection:
            self.__active = True
            conn, cursor = self.__connection.open_if_needed()
            self.activated(conn, cursor)
        return self.__connection.cursor

    def activated(self, connection, cursor):
        """
        Hook method you can assign to notice when a cursor becomes active.
        """

    def drop(self):
        self.__active = False
        self.__connection.drop()

    def rolledback(self):
        """
        Hook method. Inspect sys.exc_info() to determine if we're called with an
        exception.

        Always called, even in the event of a disconnection.
        """

    def rollback(self):
        self.__active = False
        try:
            self.__connection.rollback()
        finally:
            self.rolledback()

class ClosedConnection(object):
    """
    Represents a permanently closed connection.
    """
    __slots__ = ()

    def drop(self):
        "Does nothing."

    rollback = drop
