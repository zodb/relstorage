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

from .interfaces import ReplicaClosedException
import sys
import traceback

def _select_driver(options, driver_options):
    name = options.driver or 'auto'
    if name == 'auto':
        name = driver_options.preferred_driver_name

    try:
        return driver_options.driver_map[name]
    except KeyError:
        raise ImportError("Unable to use the driver '%s' for the database '%s'."
                          " Available drivers are: %s."
                          " Verify the driver name and that the right packages are installed."
                          % (name, driver_options.database_type, list(driver_options.driver_map.keys())))


_base_disconnected_exceptions = (ReplicaClosedException,)

def _standard_exceptions(mod):
    # Returns disconnected_exceptions, close_exceptions
    # and lock_exceptions
    # for a standard driver
    disconnected_exceptions = (getattr(mod, 'OperationalError'),
                               getattr(mod, 'InterfaceError'))
    disconnected_exceptions += _base_disconnected_exceptions

    close_exceptions = disconnected_exceptions + (getattr(mod, 'ProgrammingError'),)

    lock_exceptions = (getattr(mod, 'DatabaseError'),)
    return disconnected_exceptions, close_exceptions, lock_exceptions




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

    def cursor(self):
        return _ConnWrapper(self.__conn.cursor())

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
