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

from relstorage._compat import PY2
from relstorage.adapters.interfaces import IDBDriver

from . import AbstractMySQLDriver

__all__ = [
    'PyMySQLConnectorDriver',
    'CMySQLConnectorDriver',
]

_base_name = 'MySQL Connector/Python'

@implementer(IDBDriver)
class PyMySQLConnectorDriver(AbstractMySQLDriver):
    # See https://github.com/zodb/relstorage/issues/155
    __name__ = 'Py ' + _base_name

    MODULE_NAME = 'mysql.connector'
    PRIORITY = 4
    PRIORITY_PYPY = 2

    USE_PURE = True

    _CONVERTER_CLASS = None
    _GEVENT_CAPABLE = True
    _GEVENT_NEEDS_SOCKET_PATCH = True

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

                def __init__(self, charset_name, use_unicode):
                    if PY2:
                        use_unicode = False
                    super(BlobConverter, self).__init__(charset_name, use_unicode)

                if PY2:
                    def _STRING_to_python(self, value, dsc=None):
                        # This is also supposed to handle SET, but we don't use that type
                        # anywhere
                        if isinstance(value, bytearray):
                            # The Python version tends to return bytearray values.
                            return bytes(value)
                        # The C extension tends to return byte (aka str) values.
                        return super(BlobConverter, self)._STRING_to_python(value, dsc)

                    _VAR_STRING_to_python = _STRING_to_python

                # There are a few places we get into trouble on
                # Python 2/3 with bytearrays comping back: they
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
        converter_class = self._get_converter_class()
        kwargs['converter_class'] = converter_class

        con = self.driver_module.connect(*args, **kwargs)

        # By default, cursors won't buffer, so we don't know
        # how many rows there are. That's fine and within the DB-API spec.
        # The Python implementation is much faster if we don't ask it to.
        # The C connection doesn't accept the 'prepared' keyword.
        # You can't have both a buffered and prepared cursor,
        # but the prepared cursor doesn't gain us anything anyway.
        return con

    def set_autocommit(self, conn, value):
        # This implementation uses a property instead of a method.
        conn.autocommit = value


class CMySQLConnectorDriver(PyMySQLConnectorDriver):
    __name__ = 'C ' + _base_name

    AVAILABLE_ON_PYPY = False
    PRIORITY = 3
    PRIORITY_PYPY = 4

    USE_PURE = False
    _GEVENT_CAPABLE = False

    def get_driver_module(self):
        mod = super(CMySQLConnectorDriver, self).get_driver_module()
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
            # pylint:disable=inherit-non-class
            class Converter(PyMySQLConnectorDriver._get_converter_class()):
                def to_python(self, vtype, value):
                    if value == 0 and vtype[1] == FieldType.LONGLONG:
                        return value
                    return super(Converter, self).to_python(vtype, value)

            cls._C_CONVERTER_CLASS = Converter

        return cls._C_CONVERTER_CLASS
