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
from ZODB.POSException import ReadConflictError

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

class VoteReadConflictError(ReadConflictError):
    """
    A read conflict (from Connection.readCurrent()) that wasn't
    detected until the storage voted.
    """

class ITPCState(Interface):
    """
    An object representing the current state (phase) of the two-phase commit protocol,
    and how to transition between it and other phases.

    The initial state is always :class:`ITPCStateNotInTransaction`
    """

    transaction = Attribute("The *transaction* object from ZODB.")

    initial_state = Attribute("The ITPCStateNotInTransaction that started the process.")

    def tpc_abort(transaction, force=False):
        """
        Clear any used resources, and return the object
        representing :class:`ITPCStateNotInTransaction`.

        :param transaction: The transaction object from ZODB.
           If this is not the current transaction, does nothing
           unless *force* is true.
        :keyword bool force: Whether to forcibly abort the transaction.
           If this is true, then it doesn't matter if the *transaction* parameter
           matches or not. Also, underlying RDBMS connections should also be closed
           and discarded.
        :return: The previous `ITPCStateNotInTransaction`.
        """


class ITPCStateNotInTransaction(ITPCState):
    """
    The "idle" state.

    In this state, no store connection is available,
    and the *transaction* is always `None`.

    Because ZODB tests this, this method has to define
    a bunch of methods that are also defined by various other states.
    These methods should raise ``StorageTransactionError``, or
    ``ReadOnlyError``, as appropriate.

    Implementations of this interface should be false in a boolean context
    to easily permit testing whether a TPC phase is active or not.
    """

    last_committed_tid_int = Attribute(
        """
        The TID of the last transaction committed to get us to this state.

        Initially, this is 0. In the value returned from :meth:`ITPCPhaseVoting.tpc_finish`,
        it is the TID just committed.
        """
    )

    def tpc_begin(storage, transaction):
        """
        Enter the two-phase commit protocol.

        :return: An implementation of :class:`ITPCStateBegan`.
        """

    def tpc_finish(*args, **kwargs):
        """
        Raises ``StorageTranasctionError``
        """

    def tpc_vote(*args, **kwargs):
        """
        As for `tpc_finish`.
        """

    def store(*args, **kwargs):
        """
        Raises ``ReadOnlyError`` or ``StorageTranasctionError``
        """

    restore = deleteObject = undo = restoreBlob = store


class ITPCStateDatabaseAvailable(ITPCState, IStaleAware):
    """
    A state where the writable database connection is available.
    """

    store_connection = Attribute("The IManagedStoreConnection in use.")


class ITPCStateBegan(ITPCStateDatabaseAvailable):
    """
    The first phase where the database is available for storage.

    Storing objects, restoring objects, storing blobs, deleting objects,
    all happen in this phase.
    """

    def tpc_vote(storage, transaction):
        """
        Enter the voting state.

        :return: An implementation of :class:`ITPCStateVoting`
        """

class ITPCStateBeganHP(ITPCStateBegan):
    """
    The extra methods that are available for history-preserving
    storages.
    """

class ITPCStateVoting(ITPCStateDatabaseAvailable):
    """
    The phase where voting happens. This follows the beginning phase.
    """

    invalidated_oids = Attribute(
        """An iterable of OID bytes, returned from the storage's ``tpc_vote`` method.

        The Connection will ghost all cached objects in this iterable. This includes
        things things during conflict resolution or undo.
        """
    )

    def tpc_finish(storage, transaction, f=None):
        """
        Finish the transaction.

        :return: The next implementation of :class:`ITPCPhaseNotInTransaction`
        """
