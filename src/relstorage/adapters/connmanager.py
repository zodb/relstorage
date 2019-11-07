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


from zope.interface import implementer

from .._compat import metricmethod
from .interfaces import IConnectionManager
from .interfaces import ReplicaClosedException
from .replica import ReplicaSelector

logger = __import__('logging').getLogger(__name__)


def connection_callback(isolation_level=None,
                        read_only=None,
                        deferrable=None,
                        application_name=None,
                        inherit=None):
    """
    Decorator for functions used in :meth:`IConnectionManager.open_and_call`.

    When the function is called, it will be given a connection that has
    the given traits.

    Use ``inherit=BaseClass.func`` when you're overriding a callback
    function ``func``.
    """
    if inherit is not None:
        isolation_level = isolation_level or inherit.transaction_isolation_level
        application_name = application_name or inherit.transaction_application_name
        read_only = read_only or inherit.transaction_read_only
        deferrable = deferrable or inherit.transaction_deferrable
    def f(func):
        func.transaction_isolation_level = isolation_level
        func.transaction_read_only = read_only
        func.transaction_deferrable = deferrable
        func.transaction_application_name = application_name
        return func
    return f


def _connection_callback_open_args(connmanager, callback):
    # Read the settings from `connection_callback` or defaults.
    isolation = getattr(callback, 'transaction_isolation_level', None)
    read_only = getattr(callback, 'transaction_read_only', None)
    deferrable = getattr(callback, 'transaction_deferrable', None)
    application_name = getattr(callback, 'transaction_application_name', None)
    return dict(
        isolation=isolation or connmanager.isolation_store,
        read_only=read_only or False,
        deferrable=deferrable or False,
        application_name=application_name or ('RS: ' + callback.__name__)
    )


@implementer(IConnectionManager)
class AbstractConnectionManager(object):
    """
    Abstract base class for connection management.

    Responsible for opening and closing database connections.
    """

    # a series of callables (cursor, restart=bool)
    # for when a store connection is opened.
    _on_store_opened = ()

    # a series of callables (cursor,) that will be called
    # when a load connection is opened
    _on_load_opened = ()

    # The list of exceptions to ignore on a rollback *or* close. We
    # take this as the union of the driver's close exceptions and disconnected
    # exceptions (drivers aren't required to organize them to overlap, but
    # in practice they should.)
    _ignored_exceptions = ()

    # Subclasses should set these to get semantics as close
    # as possible to these standard levels.
    isolation_serializable = None
    isolation_read_committed = None
    # If these are not set by a subclass, they will be copied
    # from isolation_serializable and read_committed, respectively.
    isolation_load = None
    isolation_store = None

    replica_selector = None
    ro_replica_selector = None

    def __init__(self, options, driver):
        """
        :param options: A :class:`relstorage.options.Options`.
        :param driver: A :class:`relstorage.adapters.interfaces.IDBDriver`,
            which we use for its exceptions.
        """
        self.keep_history = options.keep_history
        self.driver = driver
        self.options = options

        self._ignored_exceptions = tuple(set(
            driver.close_exceptions
            + driver.disconnected_exceptions
            + (ReplicaClosedException,)
        ))

        if options.replica_conf:
            self.replica_selector = ReplicaSelector(
                options.replica_conf, options.replica_timeout)

        if options.ro_replica_conf:
            self.ro_replica_selector = ReplicaSelector(
                options.ro_replica_conf, options.replica_timeout)
        else:
            self.ro_replica_selector = self.replica_selector

        if not self.isolation_load:
            self.isolation_load = self.isolation_serializable
        if not self.isolation_store:
            self.isolation_store = self.isolation_read_committed

        self._may_need_rollback = driver.connection_may_need_rollback
        self._may_need_commit = driver.connection_may_need_commit
        self._synchronize_cursor_for_rollback = driver.synchronize_cursor_for_rollback
        self._do_commit = driver.commit
        self._do_rollback = driver.rollback

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

    def open(self,
             isolation=None,
             read_only=False,
             deferrable=False,
             replica_selector=None,
             application_name=None,
             **kwargs):
        """Open a database connection and return (conn, cursor)."""
        raise NotImplementedError()

    @metricmethod
    def close(self, conn=None, cursor=None):
        """
        Close a connection and cursor, ignoring certain errors.

        Return a True value if the connection was closed cleanly. Return
        a False value if the processes ignored an error.
        """
        clean = True
        for obj in (cursor, conn): # cursor first; some drivers want that done
            if obj is not None:
                try:
                    obj.close()
                except self._ignored_exceptions: # pylint:disable=catching-non-exception
                    clean = False
        return clean

    def __rollback_connection(self, conn, ignored_exceptions, restarting):
        """Return True if we successfully rolled back."""
        clean = True
        if conn is not None:
            if self._may_need_rollback(conn):
                try:
                    self._do_rollback(conn)
                except ignored_exceptions:
                    clean = False
            elif restarting:
                self._begin_for_restart(conn)
        return clean

    def _begin_for_restart(self, conn):
        pass

    def _may_need_rollback(self, conn): # pylint:disable=unused-argument,method-hidden
        """
        Answer if this connection might need to be rolled back.

        If a subclass can definitively say that it does *not* need to
        be rolled back, because it is not in a transaction,
        it can override to return false.
        """
        return True

    def _may_need_commit(self, conn): # pylint:disable=unused-argument,method-hidden
        """
        Answer if this connection might need to be committed.

        If a subclass can definitively say that it does *not* need to
        be committed, because it is not in a transaction,
        it can override to return false.
        """
        return True

    def __rollback(self, conn, cursor, quietly, restarting):
        # If an error occurs, close the connection and cursor.
        #
        # Some drivers require the cursor to be closed before closing
        # the connection.
        #
        # Some drivers also don't allow you to close the cursor
        # without fetching all rows.
        self._synchronize_cursor_for_rollback(cursor)
        try:
            clean = self.__rollback_connection(
                conn,
                # Let it raise if we're not meant to be quiet.
                self._ignored_exceptions if quietly else (),
                restarting
            )
        except:
            clean = False
            raise
        finally:
            if not clean:
                self.close(conn, cursor)
        return clean

    def rollback_and_close(self, conn, cursor):
        clean = self.__rollback(conn, cursor, True, False)
        if clean:
            # if an error already occurred, we closed things.
            clean = self.close(conn, cursor)

        return clean

    def rollback(self, conn, cursor):
        return self.__rollback(conn, cursor, False, None)

    def rollback_for_restart(self, conn, cursor):
        return self.__rollback(conn, cursor, False, True)

    def rollback_quietly(self, conn, cursor):
        return self.__rollback(conn, cursor, True, None)

    rollback_store_quietly = rollback_quietly

    def commit(self, conn, cursor=None, force=False):
        if self._may_need_commit(conn) or force:
            self._do_commit(conn, cursor)

    def begin(self, conn, cursor):
        pass

    def cursor_for_connection(self, conn):
        return self.driver.cursor(conn)

    def open_and_call(self, callback):
        """
        Call ``callback(connection, cursor)`` with a newly open connection and cursor.

        If the function returns, commits the transaction and returns the
        result returned by the function.
        If the function raises an exception, aborts the transaction
        then propagates the exception.
        """
        conn, cursor = self._do_open_for_call(callback)
        try:
            try:
                res = callback(conn, cursor)
            except:
                self.rollback_and_close(conn, cursor)
                conn, cursor = None, None
                raise
            else:
                self.close(None, cursor)
                cursor = None
                self.commit(conn)
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

    def restart_load(self, conn, cursor, needs_rollback=True):
        """Reinitialize a connection for loading objects."""
        self.check_replica(conn, cursor,
                           replica_selector=self.ro_replica_selector)
        if needs_rollback:
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

    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).
        """
        return self.open_for_store(application_name='RS prepack')

    def open_for_pack_lock(self):
        return self.open()

    def _do_open_for_store(self, **open_args):
        open_args['isolation'] = self.isolation_store
        open_args['read_only'] = False
        open_args['deferrable'] = False
        if 'application_name' not in open_args:
            open_args['application_name'] = 'RS store'
        return self.open(**open_args)

    def _do_open_for_call(self, callback):
        return self.open(**_connection_callback_open_args(self, callback))

    def _after_opened_for_store(self, conn, cursor, restart=False):
        """
        Called after a store is opened or restarted but
        before hooks are called.

        Subclasses may override.
        """
        # pylint:disable=unused-argument
        return

    def open_for_store(self, **open_args):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        conn, cursor = self._do_open_for_store(**open_args)
        try:
            self._after_opened_for_store(conn, cursor)
        except:
            self.close(conn, cursor)
            raise
        self._call_hooks(self._on_store_opened, conn, cursor,
                         cursor, restart=False)

        return conn, cursor

    def restart_store(self, conn, cursor, needs_rollback=True):
        """Reuse a store connection."""
        self.check_replica(conn, cursor)
        if needs_rollback:
            self.rollback_for_restart(conn, cursor)
        self._after_opened_for_store(conn, cursor)
        self._call_hooks(self._on_store_opened, conn, cursor,
                         cursor, restart=True)
