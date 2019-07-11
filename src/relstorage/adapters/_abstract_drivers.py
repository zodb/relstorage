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

from zope.interface import directlyProvides
from zope.interface import implementer

from .._compat import ABC
from .._compat import PYPY
from .._compat import casefold

from .interfaces import IDBDriver
from .interfaces import IDBDriverFactory
from .interfaces import IDBDriverOptions
from .interfaces import DriverNotAvailableError
from .interfaces import NoDriversAvailableError
from .interfaces import ReplicaClosedException
from .interfaces import UnknownDriverError


def _select_driver(options, driver_options):
    return _select_driver_by_name(options.driver, driver_options)

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


class AbstractModuleDriver(ABC):
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

    #: Can this module be used on PyPy?
    AVAILABLE_ON_PYPY = True

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

    def __init__(self):
        if PYPY and not self.AVAILABLE_ON_PYPY:
            raise DriverNotAvailableError(self.__name__)
        try:
            self.driver_module = mod = self.get_driver_module()
        except ImportError:
            raise DriverNotAvailableError(self.__name__)


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

    def _sockets_gevent_monkey_patched(self):
        # Return whether the socket module has been monkey-patched
        # by gevent
        try:
            from gevent import monkey
        except ImportError: # pragma: no cover
            return False
        else:
            return monkey.is_module_patched('socket')

    # Common compatibility shims, overriden as needed.

    def set_autocommit(self, conn, value):
        conn.autocommit(value)

    def cursor(self, conn):
        return conn.cursor()

    # Things that can be recognized as a pickled state,
    # passed to an io.BytesIO reader, and unpickled.

    # Py MySQL Connector/Python returns a bytearray, whereas
    # C MySQL Connector/Python returns bytes.

    # Keep these ordered with the most common at the front;
    # Python does a linear traversal of type checks.
    state_types = (bytes, bytearray)

    def binary_column_as_state_type(self, data):
        if isinstance(data, self.state_types) or data is None:
            return data
        # Nothing we know about. cx_Oracle likes to give us an object
        # with .read(), look for that.
        # XXX: TODO: Move this to the oracle driver.
        return self.binary_column_as_state_type(data.read())

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
