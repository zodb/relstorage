# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
Interfaces for RelStorage implementation components.

These interfaces aren't meant to be considered public, they exist to
serve as documentation and for validation of RelStorage internals.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import Interface
from zope.interface import Attribute

from transaction.interfaces import TransientError
from ZODB.POSException import StorageTransactionError

# pylint:disable=inherit-non-class,no-self-argument,no-method-argument
# pylint:disable=too-many-ancestors

class StorageDisconnectedDuringCommit(StorageTransactionError, TransientError):
    """
    We lost a connection to the database during a commit. Probably restarting
    the transaction will re-establish the connection.
    """

class IStaleAware(Interface):
    """
    An object that can transition between a state of normalcy and being stale.

    In the stale state, calling the object raises an exception.
    """

    def stale(stale_error):
        """
        Produce a new object that is the stale version of this object
        and return it.

        :return: Another `IStaleAware`.
        """

    def no_longer_stale():
        """
        Produce a new (or the original) object that is in the normal state
        and return it.

        :return: Another `IStaleAware`.
        """

class IManagedDBConnection(Interface):
    """
    A managed DB connection consists of a DB-API ``connection`` object
    and a single DB-API ``cursor`` from that connection.

    This encapsulates proper use of ``IConnectionManager``, including
    handling disconnections and re-connecting at appropriate times.

    It is not allowed to use multiple cursors from a connection at the
    same time; not all drivers properly support that.

    If the DB-API connection is not open and presumed to be good, this
    object has a false value.

    "Restarting" a connection means to bring it to a current view of
    the database. Typically this means a rollback so that a new
    transaction can begin with a new MVCC snapshot.
    """

    cursor = Attribute("The DB-API cursor to use. Read-only.")
    connection = Attribute("The DB-API connection to use. Read-only.")

    def __bool__():
        """
        Return true if the database connection is believed to be ready to use.
        """

    def __nonzero__():
        """
        Same as __bool__ for Python 2.
        """

    def drop():
        """
        Unconditionally drop (close) the database connection.
        """

    def rollback_quietly():
        """
        Rollback the connection and return a true value on success.

        When this completes, the connection will be in a neutral state,
        not idle in a transaction.

        If an error occurs during rollback, the connection is dropped
        and a false value is returned.
        """

    def isolated_connection():
        """
        Context manager that opens a new, distinct connection and
        returns its cursor.

        No matter what happens in the ``with`` block, the connection will be
        dropped afterwards.
        """

    def restart_and_call(f, *args, **kw):
        """
        Restart the connection (roll it back) and call a function
        after doing this.

        This may drop and re-connect the connection if necessary.

        :param callable f:
            The function to call: ``f(conn, cursor, *args, **kwargs)``.
            May be called up to twice if it raises a disconnected exception
            on the first try.

        :return: The return value of ``f``.
        """
