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

import abc
import sys
import traceback

from .._compat import ABC
from .._compat import PYPY
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
        raise NoDriversAvailableError

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


_base_disconnected_exceptions = (ReplicaClosedException,)

def _standard_exceptions(mod):
    # Returns disconnected_exceptions, close_exceptions
    # and lock_exceptions
    # for a standard driver
    disconnected_exceptions = (mod.OperationalError,
                               mod.InterfaceError)
    disconnected_exceptions += _base_disconnected_exceptions

    close_exceptions = disconnected_exceptions + (mod.ProgrammingError,)

    lock_exceptions = (mod.DatabaseError,)
    return disconnected_exceptions, close_exceptions, lock_exceptions


class AbstractModuleDriver(ABC):
    """
    Base implementation of a driver, based on a module, as used in DBAPI.

    Subclasses must provide:

    - ``__name__`` property
    - Implementation of ``get_driver_module``; this should import the
      module at runtime.
    """

    AVAILABLE_ON_PYPY = True

    def __init__(self):
        if PYPY and not self.AVAILABLE_ON_PYPY:
            raise DriverNotAvailableError(self.__name__)
        try:
            self.driver_module = mod = self.get_driver_module()
        except ImportError:
            raise DriverNotAvailableError(self.__name__)

        ex = _standard_exceptions(mod)
        self.disconnected_exceptions, self.close_exceptions, self.lock_exceptions = ex
        self.use_replica_exceptions = (mod.OperationalError,)
        self.Binary = mod.Binary
        self.connect = mod.connect

    @abc.abstractmethod
    def get_driver_module(self):
        """Import and return the driver module."""

    # Common compatibility shims, overriden as needed.

    def set_autocommit(self, conn, value):
        conn.autocommit(value)

    def cursor(self, conn):
        return conn.cursor()

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
