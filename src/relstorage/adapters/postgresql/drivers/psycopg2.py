# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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
psycopg2 IDBDriver implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage.adapters.drivers import GeventConnectionMixin
from relstorage.adapters.drivers import GeventDriverMixin

from ...interfaces import IDBDriver
from ...interfaces import IDBDriverSupportsCritical
from ...drivers import MemoryViewBlobDriverMixin

from . import AbstractPostgreSQLDriver
from ._lobject import LobConnectionMixin

__all__ = [
    'Psycopg2Driver',
    'GeventPsycopg2Driver',
]

class WaitCallbackStateError(ImportError):

    def __init__(self, driver_name, wait_callback_installed, need_wait_callback):
        ImportError.__init__(
            self,
            "The driver %r %s a wait callback but %s wait callback is installed" % (
                driver_name,
                "needs" if need_wait_callback else "cannot have",
                "a" if wait_callback_installed else "no"
            )
        )

@implementer(IDBDriver)
class Psycopg2Driver(MemoryViewBlobDriverMixin,
                     AbstractPostgreSQLDriver):
    __name__ = 'psycopg2'
    MODULE_NAME = __name__

    PRIORITY = 1
    PRIORITY_PYPY = 2

    def __init__(self):
        super(Psycopg2Driver, self).__init__()

        psycopg2 = self.get_driver_module()

        # pylint:disable=no-member

        self.Binary = psycopg2.Binary
        self.connect = self._create_connection(psycopg2)

        # extensions
        self.ISOLATION_LEVEL_READ_COMMITTED = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
        self.ISOLATION_LEVEL_SERIALIZABLE = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
        self.ISOLATION_LEVEL_REPEATABLE_READ = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
        self.STATUS_READY = psycopg2.extensions.STATUS_READY

        self.TS_ACTIVE = psycopg2.extensions.TRANSACTION_STATUS_ACTIVE
        self.TS_INTRANS = psycopg2.extensions.TRANSACTION_STATUS_INTRANS
        self.TS_INERROR = psycopg2.extensions.TRANSACTION_STATUS_INERROR

        self.TS_NEEDS_COMMIT = (self.TS_ACTIVE, self.TS_INTRANS)
        self.TS_NOT_NEEDROLLBACK = psycopg2.extensions.TRANSACTION_STATUS_IDLE

    def _create_connection(self, mod, *extra_slots):
        if getattr(mod, 'RSPsycopg2Connection', self) is self:
            class RSPsycopg2Connection(mod.extensions.connection):
                # The replica attribute holds the name of the replica this
                # connection is bound to.
                __slots__ = ('replica',) + extra_slots

            mod.RSPsycopg2Connection = RSPsycopg2Connection

        return mod.RSPsycopg2Connection

    def _get_extension_module(self):
        # Subclasses should override this method if they use a different
        # DB-API module.
        from psycopg2 import extensions # pylint:disable=no-name-in-module,import-error
        return extensions

    _WANT_WAIT_CALLBACK = False

    def _check_wait_callback(self):
        # Subclasses may override this method if they have no concept of
        # this.
        extensions = self._get_extension_module()
        callback = extensions.get_wait_callback()
        # Truth table:
        # callback is not None | Need callback    | pass?
        # True                 | True             | Pass
        # False                | False            | Pass
        # True                 | False            | Fail
        # False                | True             | Fail
        #
        # This is XNOR, exclusive nor, or iff. Note how it passes
        # only when both conditions are the same.

        callback_not_none = bool(callback is not None)
        need_callback = bool(self._WANT_WAIT_CALLBACK)
        if callback_not_none is not need_callback:
            raise WaitCallbackStateError(self.__name__, callback_not_none, need_callback)

    def get_driver_module(self):
        self._check_wait_callback()
        return super(Psycopg2Driver, self).get_driver_module()

    def connect_with_isolation(self, dsn,
                               isolation=None,
                               read_only=False,
                               deferrable=False,
                               application_name=None):
        # Even though the documentation claims that "Any other
        # connection parameter supported by the client library/server
        # can be passed either in the connection string or as a
        # keyword," it doesn't allow application_name as a kwarg. So we have to
        # resort to changing the dsn.
        if application_name and 'application_name' not in dsn:
            dsn = "%s application_name='%s'" % (dsn, application_name)
        conn = self.connect(dsn)
        assert not conn.autocommit
        if isolation or deferrable or read_only:
            conn.set_session(isolation_level=isolation, readonly=read_only,
                             deferrable=deferrable)

        return conn

    def cursor(self, conn, server_side=False):
        if server_side:
            cursor = conn.cursor(name=str(id(conn)))
            cursor.arraysize = self.cursor_arraysize
            cursor.itersize = self.cursor_arraysize
        else:
            cursor = super(Psycopg2Driver, self).cursor(conn)
        return cursor

    def debug_connection(self, conn, *extra): # pragma: no cover
        print(conn,
              'ts', conn.info.transaction_status,
              's', conn.status, 'tss', conn.info.status,
              'readonly', conn.readonly,
              "needs commit", self.connection_may_need_commit(conn),
              "needs rollback", self.connection_may_need_rollback(conn),
              *extra)

    def connection_may_need_rollback(self, conn):
        # If we've immediately executed a 'BEGIN' command,
        # the connection will report itself in a transaction, but
        # unless we've actually executed some sort of statement the
        # database will still know we're not and could issue a warning.
        result = conn.info.transaction_status != self.TS_NOT_NEEDROLLBACK or conn.readonly
        return result

    def connection_may_need_commit(self, conn):
        if conn.readonly:
            return False
        return conn.info.transaction_status in self.TS_NEEDS_COMMIT

    def sync_status_after_hidden_commit(self, conn):
        """
        If you execute a multiple statement or function that commits,
        *you must immediately* call this method.
        """
        # Sadly we can't do anything except commit (or rollback; it
        # has the same issue). The .status variable is untouchable.
        # This generates a notice from the server and presumably
        # undesired database round trips. Sigh.
        #
        #   WARNING:  there is no transaction in progress\n
        #
        # So we attempt to filter it here to prevent logging it. This is the one place
        # we expect to generate it. Coming from anywhere else, it needs looked into.
        conn.notices = _FilteredNotices(conn.notices)
        try:
            self.commit(conn)
        finally:
            conn.notices = conn.notices.notices


class _FilteredNotices(object):

    __slots__ = ('notices',)

    def __init__(self, notices):
        # notices could be None
        self.notices = notices

    def __len__(self):
        return len(self.notices or ())

    def append(self, msg):
        if msg != 'WARNING:  there is no transaction in progress\n':
            print("append", repr(msg))
            if self.notices is None:
                self.notices = []
            self.notices.append(msg)

    def __delitem__(self, ix):
        del self.notices[ix]

    def __iter__(self):
        return iter(self.notices or ())


@implementer(IDBDriverSupportsCritical)
class GeventPsycopg2Driver(GeventDriverMixin, Psycopg2Driver):
    __name__ = 'gevent ' + Psycopg2Driver.MODULE_NAME

    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = False

    supports_copy = False

    def _create_connection(self, mod, *extra_slots):
        if getattr(mod, 'RSGeventPsycopg2Connection', self) is self:
            Base = super(GeventPsycopg2Driver, self)._create_connection(mod, *extra_slots)

            class RSGeventPsycopg2Connection(GeventConnectionMixin,
                                             LobConnectionMixin,
                                             Base):
                RSDriverBinary = self.Binary
                _in_critical_phase = False

                def enter_critical_phase_until_transaction_end(self):
                    self._in_critical_phase = True

                def exit_critical_phase(self):
                    self._in_critical_phase = False

                def is_in_critical_phase(self):
                    return self._in_critical_phase

                def commit(self):
                    try:
                        super(RSGeventPsycopg2Connection, self).commit()
                    finally:
                        self._in_critical_phase = False

                def rollback(self):
                    try:
                        super(RSGeventPsycopg2Connection, self).rollback()
                    finally:
                        self._in_critical_phase = False

            mod.RSGeventPsycopg2Connection = RSGeventPsycopg2Connection

        return mod.RSGeventPsycopg2Connection

    def get_driver_module(self):
        # Make sure we can use gevent; if we can't the ImportError
        # will prevent this driver from being used.
        # TODO: Unify this better with inheritance.
        __import__('gevent')
        return super(GeventPsycopg2Driver, self).get_driver_module()

    _WANT_WAIT_CALLBACK = True

    ###
    # Critical sections
    ###

    def enter_critical_phase_until_transaction_end(self, connection, cursor):
        connection.enter_critical_phase_until_transaction_end()

    def is_in_critical_phase(self, connection, cursor):
        return connection.is_in_critical_phase()

    def exit_critical_phase(self, connection, cursor):
        connection.exit_critical_phase()

    # Because we have no insight into each individual query that's
    # run, and one of our queries commits in a single multi-statement,
    # if we're in critical phase, we need to commit or rollback to
    # revert that....except, the only time we do a multi-statement
    # that executes a commit we immediately turn around and call
    # ``sync_status_after_commit``, which in turn always calls
    # ``commit()``. So that's the simplest way. We've made this more
    # explicit with the
    # ``execute_multiple_statement_with_hidden_commit`` API. This
    # automatically ends a critical section.
    #
    # Previously, we briefly thought it necessary to override the
    # ``may_need_rollback`` and ``may_need_commit`` APIs.


class _GeventPsycopg2WaitCallback(object):

    def __init__(self):
        from gevent.socket import wait_read
        from gevent.socket import wait_write
        self.wait_read = wait_read
        self.wait_write = wait_write
        # pylint:disable=import-error,no-name-in-module
        from psycopg2.extensions import POLL_OK
        from psycopg2.extensions import POLL_WRITE
        from psycopg2.extensions import POLL_READ
        self.poll_ok = POLL_OK
        self.poll_write = POLL_WRITE
        self.poll_read = POLL_READ

    def __call__(self, conn):
        allow_switch = not conn._in_critical_phase
        # Even though ``conn.poll()`` would appear to do this, we need
        # to check at this level. Otherwise we can get strange hangs,
        # and one connection can monopolize the whole thing. Note that
        # this only applies when allow_switch is true.
        poll = True
        while 1:
            state = conn.poll()
            if state == self.poll_ok:
                return

            if state == self.poll_read:
                conn.gevent_generic_wait_read(allow_switch, poll)
            else:
                conn.gevent_generic_wait_write(allow_switch, poll)


def _gevent_did_patch(_event):
    try:
        from psycopg2.extensions import set_wait_callback
    except ImportError:
        pass
    else:
        set_wait_callback(_GeventPsycopg2WaitCallback())
