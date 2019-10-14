# -*- coding: utf-8 -*-
"""
SQL data types.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function



class Type(object):
    """
    A database type.
    """

    def to_sql_datatype(self, datatype_map):
        return datatype_map[type(self)]

class Unknown(Type):
    "Unspecified."

class Integer64(Type):
    """
    A 64-bit integer.
    """

class UnsignedInteger64(Integer64):
    """
    A 64-bit unsigned integer.
    """

class OID(UnsignedInteger64):
    """
    Type of an OID.
    """

class TID(UnsignedInteger64):
    """
    Type of a TID.
    """

class BinaryString(Type):
    """
    Arbitrary sized binary string.
    """

class State(Type):
    """
    Used for storing object state.
    """

class Boolean(Type):
    """
    A two-value column.
    """

class Char(Type):
    def __init__(self, char_count):
        self.char_count = char_count

    def to_sql_datatype(self, datatype_map):
        return 'CHAR(%s)' % (self.char_count,)
