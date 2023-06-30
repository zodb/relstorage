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
from importlib import metadata

from packaging.version import parse as parse_version
from packaging.version import InvalidVersion
from packaging.requirements import Requirement


from zope.interface import directlyProvides
from zope.interface import implementer

from .._compat import PYPY
from .._compat import casefold
from .._util import get_positive_integer_from_environ
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
    ex_strs = {} # {driver_name: ex}
    for factory in driver_options.known_driver_factories():
        exact_match = casefold(factory.driver_name) == driver_name
        if accept_any_driver or exact_match:
            try:
                result = factory()
            except DriverNotAvailableError as e:
                if not accept_any_driver:
                    e.driver_options = driver_options
                    raise
                ex_strs[factory.driver_name] = str(e)
            else:
                logger.debug("Using driver %s for requested name %r", result, driver_name)
                return result

    # Well snap, no driver. Either we would take any driver,
    # and none were available, or we needed an exact driver that
    # wasn't found
    error = NoDriversAvailableError if accept_any_driver else UnknownDriverError
    raise error(driver_name, driver_options, ex_strs)

class DriverNotImportableError(DriverNotAvailableError,
                               ImportError):
    "When the module can't be imported."


class _FalseReason:
    def __init__(self, exc):
        if isinstance(exc, str):
            self.message = exc
        else:
            self.message = "%s: %s" % (type(exc).__name__, exc)

    def __bool__(self):
        return False

    def __str__(self):
        return self.message


def _has_requirement(requirement):
    # A requirement is a named distribution (the thing you install
    # from PyPI) and (an optional but important for this use)
    # a version specifier. This specifier lets you
    # specify ranges and exclusions.
    #
    # This, this checks that a distribution of the correct version
    # is installed.
    #
    # *requirement* is a ``packaging.requirements.Requirement`` object.
    dist_name = requirement.name
    try:
        installed_ver_str = metadata.version(dist_name)
    except ImportError as ex:
        return _FalseReason(ex)

    try:
        installed_ver = parse_version(installed_ver_str)
    except InvalidVersion as ex:
        return _FalseReason(ex)

    if installed_ver not in requirement.specifier:
        return _FalseReason('Requirement %s not met with package: %s' % (
            requirement, installed_ver
        ))
    return True


class AbstractModuleDriver(object):
    """
    Base implementation of a driver, based on a module, as used in DBAPI.

    Subclasses must provide:

    - ``MODULE_NAME`` property.
    - ``__name__`` property
    - Implementation of ``get_driver_module``; this should import the
      module at runtime.
    - Implementation of ``exception_is_deadlock``
    """

    #: The name of the DB-API module to import.
    MODULE_NAME = None

    #: The name written in config files
    __name__ = None

    #: Can this module be used on PyPy?
    AVAILABLE_ON_PYPY = True

    #: Set this to a false value if your subclass can do static checks
    #: at import time to determine it should not be used.
    #: Helpful for things like Python version detection.
    STATIC_AVAILABLE = True

    #: Set this to a sequence of strings of requirements ``("pg8000 >= 1.29",)``
    #: Creating an instance will validate that the requirements
    #: are met (packages with correct versions are installed).
    #:
    #: Do this only when a requirement cannot be specified in
    #: setup.py as an installation requirement.
    #:
    #: .. versionadded:: 4.0.0a1
    REQUIREMENTS = ()

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
    cursor_arraysize = get_positive_integer_from_environ(
        'RS_CURSOR_ARRAYSIZE', 1024,
        logger=logger,
    )

    DriverNotAvailableError = DriverNotAvailableError

    # Can the driver support the full range of a 64-bit unsigned ID for
    # OID and TID parameters?
    supports_64bit_unsigned_id = True

    def __init__(self):
        self.driver_module = mod = self._check_preconditions()

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
        # Binary must be able to handle None values to produce a None
        # parameter; not all of them can do this out of the box.
        self.Binary = lambda d, _Binary=mod.Binary: _Binary(d) if d is not None else None
        self._connect = mod.connect
        self.priority = self.PRIORITY if not PYPY else self.PRIORITY_PYPY

    def _check_preconditions(self):
        if PYPY and not self.AVAILABLE_ON_PYPY:
            raise self.DriverNotAvailableError(self.__name__, reason="Not available on PyPy")
        if not self.STATIC_AVAILABLE:
            raise self.DriverNotAvailableError(self.__name__, reason=self.STATIC_AVAILABLE)

        # Check that it can be imported. Cannnot do this with
        # a Requirement because the distro name may not match the importable name.
        try:
            mod = self.get_driver_module()
        except ImportError as ex:
            logger.debug(
                "Attempting to load driver named %r from %r failed; if no driver was specified, "
                "or the driver was set to 'auto', there may be more drivers to attempt.",
                self.__name__, self.MODULE_NAME,
                exc_info=True)
            raise DriverNotImportableError(self.__name__, reason=str(ex)) from ex

        # It can be imported. Verify versions.
        for req in self.REQUIREMENTS:
            has_it = _has_requirement(Requirement(req))
            if not has_it:
                raise self.DriverNotAvailableError(self.__name__, reason=has_it)
        return mod


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

    message_logger = logger

    def __transaction_boundary(self, conn, meth):
        meth()
        messages = self.get_messages(conn)
        for msg in messages:
            self.message_logger.debug("Message from the RDBMS: %s", msg.strip())

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

    def exception_is_deadlock(self, exc):
        __traceback_info__ = dir(exc), exc
        raise NotImplementedError(type(self))


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

    def binary_column_as_state_type(self, data):
        if data:
            # Calling 'bytes()' on a memoryview in Python 3 does
            # nothing useful.
            data = data.tobytes()
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

try: # pylint:disable=too-complex
    import gevent
except ImportError:
    GeventDriverMixin = _NoGeventDriverMixin
    GeventConnectionMixin = _NoGeventConnectionMixin
else:
    from gevent.select import select as gselect
    from gevent import monkey as gmonkey
    from gevent.socket import wait
    get_hub = gevent.get_hub

    blocking_select = gmonkey.get_original('select', 'select')
    del gmonkey

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

        Thus, by default we perform a similar optimization as gevent sockets: we
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

        # We don't have a fileno() method ourself,
        # because we're often mixed in at the first.

        def close(self):
            self.__close_watchers()
            super().close()

        def __check_watchers(self):
            # We can be used from more than one thread in a sequential
            # fashion.
            hub = get_hub()
            if hub is not self.gevent_hub:
                self.__close_watchers()

                # XXX: Any db drivers that silently reconnect? That would change the fileno.
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
            if gselect([self], (), (), 0)[0]:
                # gselect inserts a call to gevent.sleep() to spin
                # the event loop. If we don't do that, then one connection
                # can monopolize the event loop.
                return True
            return False

        def gevent_wait_read(self, poll=True):
            if poll and self.gevent_check_read():
                return

            self.__check_watchers()
            wait(self.gevent_read_watcher,
                 hub=self.gevent_hub)

        def gevent_blocking_wait_read(self, poll=True): # pylint:disable=unused-argument
            # poll is for compatibility with ``gevent_wait_read``.
            # Note that there's no timeout here, other than what might be on the
            # socket.
            # XXX: Should there be?
            blocking_select([self], (), ())

        def gevent_generic_wait_read(self, allow_switch=True, poll=True):
            """
            *poll* only applies if *allow_switch* is True.
            """
            waiter = self.gevent_wait_read if allow_switch else self.gevent_blocking_wait_read
            waiter(poll)

        def gevent_check_write(self):
            if gselect((), [self], (), 0)[1]:
                return True
            return False

        def gevent_wait_write(self, poll=True):
            if poll and self.gevent_check_write():
                return

            self.__check_watchers()
            wait(self.gevent_write_watcher,
                 hub=self.gevent_hub)

        def gevent_blocking_wait_write(self, poll=True): # pylint:disable=unused-argument
            blocking_select((), [self], ())

        def gevent_generic_wait_write(self, allow_switch=True, poll=True):
            waiter = self.gevent_wait_write if allow_switch else self.gevent_blocking_wait_write
            waiter(poll)
