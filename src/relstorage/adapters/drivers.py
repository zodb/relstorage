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
Helpers for drivers
"""

from __future__ import print_function

import importlib
import sys
import os

from zope.interface import directlyProvides
from zope.interface import implementer

from .._compat import PYPY
from .._compat import PY3
from .._compat import casefold
from .._util import positive_integer
from .._util import consume

from .interfaces import IDBDriver
from .interfaces import IDBDriverFactory
from .interfaces import IDBDriverOptions
from .interfaces import DriverNotAvailableError
from .interfaces import NoDriversAvailableError
from .interfaces import ReplicaClosedException
from .interfaces import UnknownDriverError

logger = __import__('logging').getLogger(__name__)

def _select_driver(options, driver_options):
    driver = _select_driver_by_name(options.driver, driver_options)
    driver.configure_from_options(options)
    return driver

def _select_driver_by_name(driver_name, driver_options):
    driver_name = driver_name or 'auto'
    driver_name = casefold(driver_name)
    accept_any_driver = driver_name == 'auto'
    # XXX: For testing, we'd like to be able to prohibit the use of auto.
    for factory in driver_options.known_driver_factories():
        exact_match = casefold(factory.driver_name) == driver_name
        if accept_any_driver or exact_match:
            try:
                return factory()
            except DriverNotAvailableError as e:
                if not accept_any_driver:
                    e.driver_options = driver_options
                    raise

    # Well snap, no driver. Either we would take any driver,
    # and none were available, or we needed an exact driver that
    # wasn't found
    error = NoDriversAvailableError if accept_any_driver else UnknownDriverError
    raise error(driver_name, driver_options)

class DriverNotImportableError(DriverNotAvailableError,
                               ImportError):
    "When the module can't be imported."

class AbstractModuleDriver(object):
    """
    Base implementation of a driver, based on a module, as used in DBAPI.

    Subclasses must provide:

    - ``MODULE_NAME`` property.
    - ``__name__`` property
    - Implementation of ``get_driver_module``; this should import the
      module at runtime.
    """

    #: The name of the DB-API module to import.
    MODULE_NAME = None

    #: The name written in config files
    __name__ = None

    #: Can this module be used on PyPy?
    AVAILABLE_ON_PYPY = True

    #: Set this to false if your subclass can do static checks
    #: at import time to determine it should not be used.
    #: Helpful for things like Python version detection.
    STATIC_AVAILABLE = True

    #: Priority of this driver, when available. Lower is better.
    #: (That is, first choice should have value 1, and second choice value
    #: 2, and so on.)
    PRIORITY = 100

    #: Priority of this driver when running on PyPy. Lower is better.
    PRIORITY_PYPY = 100

    #: Class attribute. If set to a true value (not the default),
    #: ask the underlying driver to work in as strict a mode as possible
    #: when it comes to detecting programming errors.
    #:
    #: Typically set by tests. Most drivers do not have a stricter mode
    #: that can be enabled.
    STRICT = False

    # Can this driver work with gevent?
    _GEVENT_CAPABLE = False
    # Does this driver need the socket module patched?
    # Only checked if _GEVENT_CAPABLE is set to True.
    _GEVENT_NEEDS_SOCKET_PATCH = True

    #: The size we request cursor's from our :meth:`cursor` method
    #: to fetch from ``fetchmany`` and (hopefully) iteration (which is a
    #: DB-API extension. We default to 1024, but the environment variable
    #: RS_CURSOR_ARRAYSIZE can be set to an int to change this default.
    #: Individual drivers *might* choose a different default.
    cursor_arraysize = positive_integer(
        os.environ.get('RS_CURSOR_ARRAYSIZE', '1024')
    )

    DriverNotAvailableError = DriverNotAvailableError

    def __init__(self):
        if PYPY and not self.AVAILABLE_ON_PYPY:
            raise self.DriverNotAvailableError(self.__name__)
        if not self.STATIC_AVAILABLE:
            raise self.DriverNotAvailableError(self.__name__)
        try:
            self.driver_module = mod = self.get_driver_module()
        except ImportError:
            logger.debug("Unable to import driver", exc_info=True)
            raise DriverNotImportableError(self.__name__)


        self.disconnected_exceptions = (mod.OperationalError,
                                        mod.InterfaceError,
                                        ReplicaClosedException)
        self.close_exceptions = self.disconnected_exceptions + (mod.ProgrammingError,)
        self.lock_exceptions = (mod.DatabaseError,)
        # If we try to do something very wrong, a bug in our code,
        # we *should* get a ProgrammingError. Unfortunately, some drivers
        # raise ProgrammingError for other things, such as failing to get a lock.
        self.illegal_operation_exceptions = (mod.ProgrammingError,)
        self.use_replica_exceptions = (mod.OperationalError,)
        self.Binary = mod.Binary
        self._connect = mod.connect
        self.priority = self.PRIORITY if not PYPY else self.PRIORITY_PYPY

    def connect(self, *args, **kwargs):
        return self._connect(*args, **kwargs)

    def get_driver_module(self):
        """Import and return the driver module."""
        return importlib.import_module(self.MODULE_NAME)

    def gevent_cooperative(self):
        # Return whether this driver is cooperative with gevent.
        # This takes into account whether the system is
        # and/or needs to be monkey-patched
        if not self._GEVENT_CAPABLE:
            return False
        if self._GEVENT_NEEDS_SOCKET_PATCH:
            return self._sockets_gevent_monkey_patched()
        return True

    def configure_from_options(self, options): # pylint:disable=unused-argument
        """Default implementation; does nothing."""

    def _sockets_gevent_monkey_patched(self):
        # Return whether the socket module has been monkey-patched
        # by gevent
        try:
            from gevent import monkey
        except ImportError: # pragma: no cover
            return False
        else:
            # some versions of gevent have a bug where if we're monkey-patched
            # on the command line using python -m gevent.monkey /path/to/testrunner ...
            # it doesn't report being monkey-patched.
            import socket
            return monkey.is_module_patched('socket') or 'gevent' in repr(socket.socket)

    # Common compatibility shims, overriden as needed.

    def set_autocommit(self, conn, value):
        conn.autocommit(value)

    def cursor(self, conn, server_side=False): # pylint:disable=unused-argument
        cur = conn.cursor()
        cur.arraysize = self.cursor_arraysize
        return cur

    def debug_connection(self, conn, *extra): # pragma: no cover
        print(conn, *extra)

    def get_messages(self, conn): # pragma: no cover pylint:disable=unused-argument
        return ()

    def __transaction_boundary(self, conn, meth):
        meth()
        messages = self.get_messages(conn)
        for msg in messages:
            logger.debug(msg.strip())

    def commit(self, conn, cursor=None): #  pylint:disable=unused-argument
        self.__transaction_boundary(conn, conn.commit)

    def rollback(self, conn):
        self.__transaction_boundary(conn, conn.rollback)

    def connection_may_need_rollback(self, conn): # pylint:disable=unused-argument
        return True

    connection_may_need_commit = connection_may_need_rollback

    def synchronize_cursor_for_rollback(self, cursor):
        """Exceptions here are ignored, we don't know what state the cursor is in."""
        # psycopg2 raises ProgrammingError if we rollback when no results
        # are present on the cursor. mysql-connector-python raises
        # InterfaceError. OTOH, mysqlclient raises nothing and even wants
        # it in certain circumstances.

        if cursor is not None:
            try:
                consume(cursor)
            except Exception: # pylint:disable=broad-except
                pass

    # Things that can be recognized as a pickled state,
    # passed to an io.BytesIO reader, and unpickled.

    # Py MySQL Connector/Python returns a bytearray, whereas
    # C MySQL Connector/Python returns bytes.
    # sqlite uses buffer on Py2 and memoryview on Py3.

    # Keep these ordered with the most common at the front;
    # Python does a linear traversal of type checks.
    state_types = (bytes, bytearray)

    def binary_column_as_state_type(self, data):
        if isinstance(data, self.state_types) or data is None:
            return data
        __traceback_info__ = type(data), data
        raise TypeError("Unknown binary state column")


    def binary_column_as_bytes(self, data):
        # Take the same inputs as `as_state_type`, but turn them into
        # actual bytes. This includes None and empty bytes, which becomes
        # the literal b'';
        # XXX: TODO: We don't need all these checks up here. Just the common ones,
        # move everything else to specific drivers.
        if data is None or not data:
            return b''
        if isinstance(data, bytes):
            return data
        if isinstance(data, memoryview):
            return data.tobytes()
        # Everything left we convert with the bytes() construtor.
        # That would be buffer and bytearray
        __traceback_info__ = data, type(data)
        return bytes(data)

    def enter_critical_phase_until_transaction_end(self, connection, cursor):
        """Default implementation; does nothing."""

    def is_in_critical_phase(self, connection, cursor):
        """Default implementation; returns a false value."""

    def exit_critical_phase(self, connection, cursor):
        "Default implementation; does nothing."

class MemoryViewBlobDriverMixin(object):
    # psycopg2 is smart enough to return memoryview or buffer on
    # Py3/Py2, respectively, for BYTEa columns. sqlite3 does exactly
    # the same for BLOB columns (on Python 2; on Python 3 it returns
    # bytes instead of buffer), and defines ``Binary`` that way as
    # well.

    # memoryview can't be passed to bytes() on Py2 or Py3, but it can
    # be passed to cStringIO.StringIO() or io.BytesIO() ---
    # unfortunately, memoryviews, at least, don't like going to
    # io.BytesIO() on Python 3, and that's how we unpickle states. So
    # while ideally we'd like to keep it that way, to save a copy, we
    # are forced to make the copy. Plus there are tests that like to
    # directly compare bytes.

    if PY3:
        def binary_column_as_state_type(self, data):
            if data:
                # Calling 'bytes()' on a memoryview in Python 3 does
                # nothing useful.
                data = data.tobytes()
            return data
    else:
        def binary_column_as_state_type(self, data):
            if data:
                data = bytes(data)
            return data


@implementer(IDBDriverFactory)
class _ClassDriverFactory(object):

    def __init__(self, driver_type):
        self.driver_type = driver_type
        # Getting the name is tricky, the class wants to shadow it.
        self.driver_name = driver_type.__dict__.get('__name__') or driver_type.__name__

    def check_availability(self):
        try:
            self.driver_type()
        except DriverNotAvailableError:
            return False
        return True

    def __call__(self):
        return self.driver_type()

    def __eq__(self, other):
        return (casefold(self.driver_name), self.driver_type) == (
            casefold(other.driver_name), other.driver_type)

    def __hash__(self):
        return hash((casefold(self.driver_name), self.driver_type))

    def __getattr__(self, name):
        return getattr(self.driver_type, name)


def implement_db_driver_options(name, *driver_modules):
    """
    Helper function to be called at a module scope to
    make it implement ``IDBDriverOptions``.

    :param str name: The value of ``__name__``.
    :param driver_modules: Each of these names a module that has
        one or more implementations of ``IDBDriver`` in it,
        as named in their ``__all__`` attribute.

    """

    module = sys.modules[name]

    driver_factories = set()
    for driver_module in driver_modules:
        driver_module = importlib.import_module('.' + driver_module,
                                                name)
        for factory in driver_module.__all__:
            factory = getattr(driver_module, factory)
            if IDBDriver.implementedBy(factory): # pylint:disable=no-value-for-parameter
                driver_factories.add(_ClassDriverFactory(factory))

    module.known_driver_factories = lambda: sorted(
        driver_factories,
        key=lambda factory: factory.PRIORITY if not PYPY else factory.PRIORITY_PYPY,
    )

    directlyProvides(module, IDBDriverOptions)

    module.select_driver = lambda driver_name=None: _select_driver_by_name(driver_name,
                                                                           sys.modules[name])


class _NoGeventDriverMixin(object):
    import time as gevent

    def get_driver_module(self):
        raise ImportError("Could not import gevent")

class _NoGeventConnectionMixin(object):
    gevent_hub = None
    gevent_read_watcher = None
    gevent_write_watcher = None
    gevent_sleep = None

try:
    import gevent
except ImportError:
    GeventDriverMixin = _NoGeventDriverMixin
    GeventConnectionMixin = _NoGeventConnectionMixin
else:
    import select
    from gevent.socket import wait
    get_hub = gevent.get_hub

    class GeventDriverMixin(object):
        gevent = gevent

    class GeventConnectionMixin(_NoGeventConnectionMixin):
        """
        Helper for a connection that waits using gevent.

        Subclasses must provide a ``fileno()`` method. The usual
        pattern for executing a query would then be something like
        this::

            query = format_query_to_bytes(...)
            self.gevent_wait_write()
            self.send_query()

            self.gevent_wait_read()
            self.read_results()

        It is important that ``send_query`` do nothing but put bytes
        on the wire. It must not include any attempt to wait for a
        response from the database, especially if that response could
        take an arbitrary amount of time or block. (Of course, if
        ``send_query`` and ``read_results`` can arrange to use gevent
        waiting functions too, you'll have finer control. This example
        is all-or-nothing. Sometimes its easy to handle
        ``read_results`` in a looping function using a server-side
        cursor.)

        The ``gevent_wait_read`` and ``gevent_wait_write`` functions
        are implemented using :func:`gevent.socket.wait`. That
        function always takes a full iteration of the event loop to
        determine whether a file descriptor is ready; it always yields
        control to other greenlets immediately. gevent's own sockets
        don't work that way; instead they try to read/write and catch
        the resulting EAGAIN exception. Only after that do they yield
        to the event loop. This is for good reason: eliminating
        unnecessary switches can lead to higher throughput.

        Here, a pass through the event loop can be risky. If we send a
        request that establishes database locks that will require
        further action from the greenlet to relinquish, those will
        come into being (potentially blocking other greenlets in the
        same or different processes) sometime between when
        ``send_query`` is entered and when ``gevent_wait_read`` exits.
        If, for any reason, a different greenlet runs while we have
        yielded to the event loop and blocks on a resource we own that
        is not gevent cooperative (a non-monkey-patched lock, a
        different database) we'll never regain control. And thus we'll
        never be able to make forward progress and release those
        locks. Since they're shared locks, that could harm arbitrary
        machines in the cluster.

        Thus, we perform a similar optimization as gevent sockets: we
        first check to see if the file descriptor is ready and only
        yield to the event loop if it isn't. The cost is an extra
        system call to ``select``. For write requests, we could be
        able to assume that they are always ready (depending on the
        nature of the protocol); if that's so, override
        :meth:`gevent_check_write`. The same goes for
        :meth:`gevent_check_read`. This doesn't eliminate the problem,
        but it should substantially reduce the chances of it
        happening.
        """
        gevent_sleep = staticmethod(gevent.sleep)

        def close(self):
            self.__close_watchers()
            super(GeventConnectionMixin, self).close()

        def __check_watchers(self):
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

        def gevent_check_read(self,):
            if select.select([self], (), (), 0)[0]:
                return True
            return False

        def gevent_wait_read(self):
            if not self.gevent_check_read():
                self.__check_watchers()
                wait(self.gevent_read_watcher,
                     hub=self.gevent_hub)

        def gevent_check_write(self):
            if select.select((), [self], (), 0)[1]:
                return True
            return False

        def gevent_wait_write(self):
            if not self.gevent_check_write():
                self.__check_watchers()
                wait(self.gevent_write_watcher,
                     hub=self.gevent_hub)
