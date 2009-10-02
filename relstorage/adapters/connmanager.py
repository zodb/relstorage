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

from relstorage.adapters.interfaces import IConnectionManager
from relstorage.adapters.interfaces import ReplicaClosedException
from zope.interface import implements
import os
import time


class AbstractConnectionManager(object):
    """Abstract base class for connection management.

    Responsible for opening and closing database connections.
    """
    implements(IConnectionManager)

    # disconnected_exceptions contains the exception types that might be
    # raised when the connection to the database has been broken.
    disconnected_exceptions = (ReplicaClosedException,)

    # close_exceptions contains the exception types to ignore
    # when the adapter attempts to close a database connection.
    close_exceptions = ()

    # on_store_opened is either None or a callable that
    # will be called whenever a store cursor is opened or rolled back.
    on_store_opened = None

    def __init__(self, replica_conf=None):
        if replica_conf:
            self.replicas = ReplicaSelector(replica_conf)
        else:
            self.replicas = None

    def set_on_store_opened(self, f):
        """Set the on_store_opened hook"""
        self.on_store_opened = f

    def open(self):
        """Open a database connection and return (conn, cursor)."""
        raise NotImplementedError()

    def close(self, conn, cursor):
        """Close a connection and cursor, ignoring certain errors.
        """
        for obj in (cursor, conn):
            if obj is not None:
                try:
                    obj.close()
                except self.close_exceptions:
                    pass

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
                conn.rollback()
                raise
            else:
                conn.commit()
                return res
        finally:
            self.close(conn, cursor)

    def open_for_load(self):
        raise NotImplementedError()

    def restart_load(self, conn, cursor):
        """Reinitialize a connection for loading objects."""
        if self.replicas is not None:
            if conn.replica != self.replicas.current():
                # Prompt the change to a new replica by raising an exception.
                self.close(conn, cursor)
                raise ReplicaClosedException()
        conn.rollback()

    def open_for_store(self):
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """
        conn, cursor = self.open()
        try:
            if self.on_store_opened is not None:
                self.on_store_opened(cursor, restart=False)
            return conn, cursor
        except:
            self.close(conn, cursor)
            raise

    def restart_store(self, conn, cursor):
        """Reuse a store connection."""
        if self.replicas is not None:
            if conn.replica != self.replicas.current():
                # Prompt the change to a new replica by raising an exception.
                self.close(conn, cursor)
                raise ReplicaClosedException()
        conn.rollback()
        if self.on_store_opened is not None:
            self.on_store_opened(cursor, restart=True)

    def open_for_pre_pack(self):
        """Open a connection to be used for the pre-pack phase.
        Returns (conn, cursor).
        """
        return self.open()


class ReplicaSelector(object):

    def __init__(self, replica_conf, alt_timeout=600):
        self.replica_conf = replica_conf
        self.alt_timeout = alt_timeout
        self._read_config()
        self._select(0)
        self._iterating = False
        self._skip_index = None

    def _read_config(self):
        self._config_modified = os.path.getmtime(self.replica_conf)
        self._config_checked = time.time()
        f = open(self.replica_conf, 'r')
        try:
            lines = f.readlines()
        finally:
            f.close()
        replicas = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            replicas.append(line)
        if not replicas:
            raise IndexError(
                "No replicas specified in %s" % self.replica_conf)
        self._replicas = replicas

    def _is_config_modified(self):
        now = time.time()
        if now < self._config_checked + 1:
            # don't check the last mod time more often than once per second
            return False
        self._config_checked = now
        t = os.path.getmtime(self.replica_conf)
        return t != self._config_modified

    def _select(self, index):
        self._current_replica = self._replicas[index]
        self._current_index = index
        if index > 0 and self.alt_timeout:
            self._expiration = time.time() + self.alt_timeout
        else:
            self._expiration = None

    def current(self):
        """Get the current replica."""
        self._iterating = False
        if self._is_config_modified():
            self._read_config()
            self._select(0)
        elif self._expiration is not None and time.time() >= self._expiration:
            self._select(0)
        return self._current_replica

    def next(self):
        """Return the next replica to try.

        Return None if there are no more replicas defined.
        """
        if self._is_config_modified():
            # Start over even if iteration was already in progress.
            self._read_config()
            self._select(0)
            self._skip_index = None
            self._iterating = True
        elif not self._iterating:
            # Start iterating.
            self._skip_index = self._current_index
            i = 0
            if i == self._skip_index:
                i = 1
                if i >= len(self._replicas):
                    # There are no more replicas to try.
                    self._select(0)
                    return None
            self._select(i)
            self._iterating = True
        else:
            # Continue iterating.
            i = self._current_index + 1
            if i == self._skip_index:
                i += 1
            if i >= len(self._replicas):
                # There are no more replicas to try.
                self._select(0)
                return None
            self._select(i)

        return self._current_replica
