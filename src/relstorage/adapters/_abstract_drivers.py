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
import traceback

from zope.interface import directlyProvides

from .._compat import ABC
from .._compat import PYPY
from .._compat import PY2
from .interfaces import IDBDriver
from .interfaces import IDBDriverOptions
from .interfaces import DriverNotAvailableError
from .interfaces import NoDriversAvailableError
from .interfaces import ReplicaClosedException
from .interfaces import UnknownDriverError


def _select_driver(options, driver_options):
    return _select_driver_by_name(options.driver, driver_options)

def _select_driver_by_name(driver_name, driver_options):
    driver_name = driver_name or 'auto'
    if driver_name == 'auto':
        # XXX: For testing, we'd like to be able to prohibit the use of auto.
        for driver in driver_options.driver_order:
            try:
                return driver()
            except DriverNotAvailableError:
                pass
        raise NoDriversAvailableError(driver_name, driver_options)

    try:
        factory = driver_options.driver_map[driver_name]
    except KeyError:
        raise UnknownDriverError(driver_name, driver_options)
    else:
        try:
            return factory()
        except DriverNotAvailableError as e:
            e.driver_options = driver_options
            raise e


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
        self.use_replica_exceptions = (mod.OperationalError,)
        self.Binary = mod.Binary
        self.connect = mod.connect

    def get_driver_module(self):
        """Import and return the driver module."""
        return importlib.import_module(self.MODULE_NAME)

    @classmethod
    def check_availability(cls):
        try:
            cls()
        except DriverNotAvailableError:
            return False
        return True

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
                driver_factories.add(factory)

    driver_map = module.driver_map = {
        # Getting the name is tricky, the class wants to shadow it.
        factory.__dict__['__name__']: factory
        for factory in driver_factories
    }

    module.driver_order = sorted(
        driver_factories,
        key=lambda factory: factory.PRIORITY if not PYPY else factory.PRIORITY_PYPY,
    )

    directlyProvides(module, IDBDriverOptions)

    module.select_driver = lambda driver_name=None: _select_driver_by_name(driver_name,
                                                                           sys.modules[name])

    module.known_driver_names = lambda: driver_map

class _ConnWrapper(object): # pragma: no cover
    def __init__(self, conn):
        self.__conn = conn
        self.__type = type(conn)
        self.__at = ''.join(traceback.format_stack())

    def __getattr__(self, name):
        return getattr(self.__conn, name)

    def __setattr__(self, name, value):
        if name in ('_ConnWrapper__conn', '_ConnWrapper__at', '_ConnWrapper__type'):
            object.__setattr__(self, name, value)
            return
        return setattr(self.__conn, name, value)

    def cursor(self, *args, **kwargs):
        return _ConnWrapper(self.__conn.cursor(*args, **kwargs))

    def execute(self, op, args=None):
        #print(op, args)
        self.__conn.connection.handle_unread_result()
        return self.__conn.execute(op, args)


    def __iter__(self):
        return self.__conn.__iter__()

    def close(self):
        if self.__conn is None:
            return
        try:
            self.__conn.close()
        finally:
            self.__conn = None

    def __del__(self):
        if self.__conn is not None:
            print("Failed to close", self, self.__type, " from:", self.__at, file=sys.stderr)
            print("Deleted at", ''.join(traceback.format_stack()))
