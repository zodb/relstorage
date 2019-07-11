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
from __future__ import absolute_import, print_function

from perfmetrics import metricmethod
from zope.interface import implementer

from .interfaces import IConnectionManager
from .interfaces import ReplicaClosedException
from .replica import ReplicaSelector


@implementer(IConnectionManager)
class AbstractConnectionManager(object):
    """Abstract base class for connection management.

    Responsible for opening and closing database connections.
    """

    # disconnected_exceptions contains the exception types that might be
    # raised when the connection to the database has been broken.
    disconnected_exceptions = (ReplicaClosedException,)

    # close_exceptions contains the exception types to ignore
    # when the adapter attempts to close a database connection.
    close_exceptions = ()

    # a series of callables (cursor, restart=bool)
    # for when a store connection is opened.
    _on_store_opened = ()

    # a series of callables (cursor,) that will be called
    # when a load connection is opened
    _on_load_opened = ()

    # psycopg2 raises ProgrammingError if we rollback when nothing is present.
    # mysql-connector-python raises InterfaceError.
    # OTOH, mysqlclient raises nothing and even wants it.
    _fetchall_on_rollback = True

    def __init__(self, options):
        # options is a relstorage.options.Options instance
        if options.replica_conf:
            self.replica_selector = ReplicaSelector(
                options.replica_conf, options.replica_timeout)
        else:
            self.replica_selector = None

        if options.ro_replica_conf:
            self.ro_replica_selector = ReplicaSelector(
                options.ro_replica_conf, options.replica_timeout)
        else:
            self.ro_replica_selector = self.replica_selector

    def add_on_store_opened(self, f):
        """
        Add a callable(cursor, restart=bool) for when a store connection
        is opened.

        Hooks are called in the order added.

        .. versionadded:: 2.1a1
        """
        self._on_store_opened += (f,)

    set_on_store_opened = add_on_store_opened # BWC

    def add_on_load_opened(self, f):
        """
        Add a callable (cursor, restart=bool) for when a load connection is opened.

        Hooks are called in the order added.

        .. versionadded:: 2.1a1
        """
        self._on_load_opened += (f,)

    def open(self, **kwargs):
        """Open a database connection and return (conn, cursor)."""
        raise NotImplementedError()

    @metricmethod
    def close(self, conn=None, cursor=None):
        """
        Close a connection and cursor, ignoring certain errors.
        """
        for obj in (cursor, conn):
            if obj is not None:
                try:
                    obj.close()
                except self.close_exceptions:
                    pass

    def rollback_and_close(self, conn, cursor):
        # Some drivers require the cursor to be closed first.

        # Some drivers also don't allow you to close without fetching
        # all rows.
        if cursor is not None and self._fetchall_on_rollback:
            try:
                cursor.fetchall()
            except Exception: # pylint:disable=broad-except
                pass
        self.close(cursor)

        if conn is not None:
            assert cursor is not None
            try:
                conn.rollback()
            except self.close_exceptions:
                pass
            finally:
                self.close(conn)

    def rollback(self, conn, cursor):
        # Like rollback and close, but doesn't bury exceptions.
        if self._fetchall_on_rollback:
            cursor.fetchall()
        conn.rollback()

    def open_and_call(self, callback):
        """Call a function with an open connection and cursor.

        If the function returns, commits the transaction and returns the
        result returned by the function.
        If the function raises an exception, aborts the transaction
        then propagates the exception.
        """
        conn, cursor = self.open()
        try:
            try:
                res = callback(conn, cursor)
            except:
                self.rollback_and_close(conn, cursor)
                conn, cursor = None, None
                raise
            else:
                self.close(cursor)
                conn.commit()
                return res
        finally:
            self.close(conn, cursor)

    def _call_hooks(self, hooks, conn, cursor,
                    *args, **kwargs):
        try:
            for hook in hooks:
                hook(*args, **kwargs)
        except:
            self.close(conn, cursor)
            raise

    def _do_open_for_load(self):
        raise NotImplementedError()

    def open_for_load(self):
        conn, cursor = self._do_open_for_load()
        self._call_hooks(self._on_load_opened, conn, cursor,
                         cursor, restart=False)
        return conn, cursor

    def restart_load(self, conn, cursor):
        """Reinitialize a connection for loading objects."""
        self.check_replica(conn, cursor,
                           replica_selector=self.ro_replica_selector)
        self.rollback(conn, cursor)
        self._call_hooks(self._on_load_opened, conn, cursor,
                         cursor, restart=True)

    def check_replica(self, conn, cursor, replica_selector=None):
        """Raise an exception if the connection belongs to an old replica"""
        if replica_selector is None:
            replica_selector = self.replica_selector

        if replica_selector is not None:
            current = replica_selector.current()
            if conn.replica != current:
                # Prompt the change to a new replica by raising an exception.
                self.close(conn, cursor)
                raise ReplicaClosedException(
                    "Switched replica from %s to %s" % (conn.replica, current))

    def _do_open_for_store(self):
        """
        Subclasses may override.

        Returns (conn, cursor)
        """
        return self.open()

    def _after_opened_for_store(self, conn, cursor, restart=False):
        """
        Called after a store is opened or restarted but
        before hooks are called.

        Subclasses may override.
        """
        # pylint:disable=unused-argument
        return

    def open_for_store(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        conn, cursor = self._do_open_for_store()
        try:
            self._after_opened_for_store(conn, cursor)
        except:
            self.close(conn, cursor)
            raise
        self._call_hooks(self._on_store_opened, conn, cursor,
                         cursor, restart=False)

        return conn, cursor

    def restart_store(self, conn, cursor):
        """Reuse a store connection."""
        self.check_replica(conn, cursor)
        self.rollback(conn, cursor)
        self._after_opened_for_store(conn, cursor)
        self._call_hooks(self._on_store_opened, conn, cursor,
                         cursor, restart=True)


    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).
        """
        return self.open_for_store()
