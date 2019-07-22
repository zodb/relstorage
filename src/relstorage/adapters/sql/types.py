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

class Unknown(Type):
    "Unspecified."

class Integer64(Type):
    """
    A 64-bit integer.
    """

class OID(Integer64):
    """
    Type of an OID.
    """

class TID(Integer64):
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
