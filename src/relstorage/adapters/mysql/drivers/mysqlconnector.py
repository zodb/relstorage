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
MySQL Connector/Python IDBDriver implementations.
"""
from __future__ import absolute_import
from __future__ import print_function

from zope.interface import implementer

from relstorage._compat import PYPY
from relstorage.adapters.interfaces import IDBDriver

from . import AbstractMySQLDriver

__all__ = [
    'PyMySQLConnectorDriver',
    'CMySQLConnectorDriver',
]

logger = __import__('logging').getLogger(__name__)

_base_name = 'MySQL Connector/Python'


@implementer(IDBDriver)
class PyMySQLConnectorDriver(AbstractMySQLDriver):
    # See https://github.com/zodb/relstorage/issues/155
    __name__ = 'Py ' + _base_name

    MODULE_NAME = 'mysql.connector'
    PRIORITY = 4
    PRIORITY_PYPY = 2
    REQUIREMENTS = (
        # 8.0.32 changes character set handling,
        # adds init_command

        'mysql-connector-python >= 8.0.32',
    )


    USE_PURE = True

    _CONVERTER_CLASS = None
    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = True

    def __init__(self):
        super().__init__()
        # This driver doesn't support ``init_command``, we have to run
        # it manually.
        #self.__init_command = self._init_command
        #del self._init_command

        # conn.close() -> InternalError: Unread result found
        # By the time we get to a close(), it's too late to do anything about it.
        self.close_exceptions += (self.driver_module.InternalError,)
        self.mysql_deadlock_exc = self.driver_module.DatabaseError

        if PYPY:
            # Patch to work around JIT bug found in (at least) 7.1.1
            # https://bitbucket.org/pypy/pypy/issues/3014/jit-issue-inlining-structunpack-hh
            #
            # In addition, we later discovered
            # https://github.com/zodb/relstorage/issues/283#issuecomment-516489791,
            # which was tracked down to be the same thing: protocol calls into utils
            # to parse "length code" for lastrowid in utils.read_lc_int, which uses
            # struct_unpack
            try:
                from mysql.connector import catch23
                from mysql.connector import protocol
                from mysql.connector import utils
            except ImportError: # pragma: no cover
                catch23 = protocol = utils = None

            if not hasattr(catch23, 'struct_unpack') or not hasattr(protocol, 'struct_unpack'):
                # pragma: no cover
                logger.debug("Unknown mysql.connector; not patching for PyPy JIT")
            else:
                logger.debug("Patching mysql.connector for PyPy JIT bug")
                from struct import unpack
                def struct_unpack(fmt, buf):
                    if isinstance(buf, bytearray):
                        buf = bytes(buf)
                    return unpack(fmt, buf)

                catch23.struct_unpack = struct_unpack
                protocol.struct_unpack = struct_unpack
                utils.struct_unpack = struct_unpack

    @classmethod
    def _get_converter_class(cls):
        if cls._CONVERTER_CLASS is None:
            import mysql.connector.conversion # pylint:disable=import-error

            # The results of things like 'SHOW TABLES' come in
            # and use _STRING_to_python (or _VAR_STRING_to_python).
            # This uses the value of self._use_unicode to decide whether to
            # decode bytes and bytearrays to unicode, unless the charset is
            # set to binary. Unfortunately, there's a bug and it doesn't
            # ever actually read 'use_unicode' out of the keyword arguments!
            # We want strings to be native strings on Python 2.

            # It likes to return BLOB columns as Unicode strings too! Clearly
            # that's not right. (The reason is because apparently JSON data is
            # stored with the same type as a BLOB...so for every BLOB it sees, it
            # first tries to parse it as JSON!)

            # The Python implementation calls row_to_python(), the C version
            # calls to_python()
            class BlobConverter(mysql.connector.conversion.MySQLConverter):
                # There are a few places we get into trouble on
                # Python 2/3 with bytearrays coming back: they
                # can't be hashed for the local_client compression
                # functions or sent to zlib.decompress(), they
                # can't be sent to pickle.loads(), etc, so it's
                # best to return them as bytes.
                def _BLOB_to_python(self, value, dsc=None): # pylint:disable=unused-argument
                    if isinstance(value, bytearray):
                        return bytes(value)
                    return value or b''

                _LONG_BLOB_to_python = _BLOB_to_python
                _MEDIUM_BLOB_to_python = _BLOB_to_python
                _TINY_BLOB_to_python = _BLOB_to_python


            cls._CONVERTER_CLASS = BlobConverter

        return cls._CONVERTER_CLASS

    def connect(self, *args, **kwargs):
        # It defaults to the (slower) pure-python version prior to 8.0.11.
        # NOTE: The C implementation doesn't support the prepared
        # operations.
        # NOTE: The C implementation returns bytes when the Py implementation
        # returns bytearray under Py2
        kwargs['use_pure'] = self.USE_PURE
        kwargs['converter_class'] = self._get_converter_class()

        # Prior to 8.0.32, we fetched and displayed warnings.
        #
        # But 8.0.32 is MESSED UP. First, it tries to use
        # ``warnings.warn(integer)``, which raises a TypeError. Second
        # and most importantly, there is no combination of
        # ``charecter_set_*`` settings that let it work without
        # warnings and/or errors. If we set those to ``utf8mb4``, then
        # when we try to send pickle values we get a warning "(1300)
        # Invalid utf8mb4 character string: '800363'" (and because of
        # bug number one, this becomes an Exception). And if we set
        # them to ``binary`` then when we try to send JSON values we
        # get a hard error from the DB: "(22032): Cannot create a JSON
        # value from a string with CHARACTER SET 'binary'" --- this is
        # despite installing the procs with that character set.
        #
        # PyMySQL handles this by using the ``_binary`` character set
        # introducer, but I don't find a way to insert that here. The
        # only workaround I've found is to just ignore the warnings...

        kwargs['get_warnings'] = False

        # By default, make it fetch all rows for the cursor, like most
        # drivers do. Unless we do this, cursors won't buffer, so we
        # don't know how many rows there are. That's fine and within
        # the DB-API spec. The Python implementation is much faster if
        # we don't ask it to. The C connection doesn't accept the
        # 'prepared' keyword. You can't have both a buffered and
        # prepared cursor, but the prepared cursor doesn't gain us
        # anything anyway.
        kwargs['buffered'] = True
        return super().connect(*args, **kwargs)

    def cursor(self, conn, server_side=False):
        if server_side:
            cursor = conn.cursor(buffered=False)
            cursor.arraysize = self.cursor_arraysize
        else:
            cursor = super().cursor(conn, server_side=server_side)
        cursor.connection = conn
        return cursor

    def set_autocommit(self, conn, value):
        # This implementation uses a property instead of a method.
        conn.autocommit = value

    def callproc_multi_result(self, cursor, proc, args=(), exit_critical_phase=False):
        # This driver is weird, wants multi=True, returns an iterator of cursors
        # instead of using nextset()
        resultsets = cursor.execute("CALL " + proc, args, multi=True)
        multi_results = []
        for resultset in resultsets:
            try:
                multi_results.append(resultset.fetchall())
            except self.driver_module.InterfaceError:
                # This gets raised on the empty set at the end, for some reason.
                # Ensure we put one there to be like the others
                multi_results.append(())
                break
        return multi_results

    def callproc_no_result(self, cursor, proc, args=()):
        # Again, weird. The call's empty result set seems to be consumed.
        cursor.execute("CALL " + proc, args)


class CMySQLConnectorDriver(PyMySQLConnectorDriver):
    __name__ = 'C ' + _base_name

    AVAILABLE_ON_PYPY = False
    # Values this high won't usually be automatically tested.
    # This driver (as of 8.0.16) fails to run under Python 3.7's development
    # mode, with assertion failures about doing things without the GIL
    # that crash the interpreter. That's pretty risky, so we don't currently
    # recommend it.
    # TODO: 8.0.17 claims to have fixed issues with Python 3. Verify this.
    PRIORITY = 1000
    PRIORITY_PYPY = 4000

    USE_PURE = False
    _GEVENT_CAPABLE = False

    def get_driver_module(self):
        mod = super().get_driver_module()
        if not mod.HAVE_CEXT:
            raise ImportError("No C extension")
        return mod

    _C_CONVERTER_CLASS = None

    @classmethod
    def _get_converter_class(cls):
        # The C implementation does *some* of its conversion down in the C
        # layer. If we try to pass that through the conversion process again,
        # we get bugs.
        # Notably, integer columns that have a value of 0 are interpreted as
        # NULL and end processing of the result set! This means, for example, that
        # we can't get changes for the root object, because it has zoid == 0.
        # We workaround that here.

        if cls._C_CONVERTER_CLASS is None:
            from mysql.connector.conversion import FieldType # pylint:disable=import-error
            # pylint:disable=inherit-non-class,protected-access
            class Converter(PyMySQLConnectorDriver._get_converter_class()):
                def to_python(self, vtype, value):
                    if value == 0 and vtype[1] == FieldType.LONGLONG:
                        return value
                    return super().to_python(vtype, value)

            cls._C_CONVERTER_CLASS = Converter

        return cls._C_CONVERTER_CLASS
