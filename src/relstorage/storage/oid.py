# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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
Implementation of the oid allocation algorithm.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ZODB.POSException import ReadOnlyError
from ZODB.utils import p64 as int64_to_8bytes

from zope.interface import implementer

from .interfaces import IStaleAware

logger = __import__('logging').getLogger(__name__)

class AbstractOIDs(object):

    __slots__ = (
    )

    def stale(self, ex):
        return StaleOIDs(ex, self)

    def no_longer_stale(self):
        return self

    def new_oid(self, commit_in_progress):
        raise NotImplementedError

    def set_min_oid(self, max_observed_oid):
        raise NotImplementedError

class OIDs(AbstractOIDs):

    def __init__(self, oidallocator, store_connection):
        # From largest to smallest: [16, 15, 14, ..., 1]
        self.preallocated_oids = [] # type: list
        # The maximum OID we've handed out (or that has been observed)
        # A value of 0 is not legal for the oidallocator to produce.
        self.max_allocated_oid = 0 # type: int
        self.oidallocator = oidallocator
        self.store_connection = store_connection # type: StoreConnection
        if hasattr(oidallocator, 'new_oids_no_cursor'):
            self.__preallocate_oids = self.__preallocate_oids_no_cursor

    def set_min_oid(self, max_observed_oid):
        """
        Ensure that the next oid we produce is greater than *max_observed_oid*.

        Must be done in a transaction while the store connection is usable.
        """
        if max_observed_oid > self.max_allocated_oid:
            # They saw one from outside of us that's greater than what
            # we've allocated. We could be a brand new object that's
            # never allocated an OID before (i.e., we're unused, or
            # we've only done loads); or, we could be copying
            # transactions from an external storage.

            # Set it in the database for everyone.
            self.oidallocator.set_min_oid(self.store_connection.cursor,
                                          max_observed_oid)
            # Then, set it in the storage for this thread
            # so we don't have to keep doing this if it only ever
            # updates existing objects.
            # NOTE: This is a non-transactional change to the our state.
            # That's OK, though, as the underlying sequence for OIDs we allocate
            # is also non-transactional.
            self.max_allocated_oid = max_observed_oid
            # Discard any preallocated oids that are less than this; they're not
            # safe to use in the most general case. (In the typical case they probably
            # are.)
            preallocated_oids = self.preallocated_oids
            while preallocated_oids and preallocated_oids[-1] < max_observed_oid:
                preallocated_oids.pop()

    def new_oid(self, commit_in_progress):
        # Prior to ZODB 5.1.2, this method was actually called on the
        # storage object of the DB, not the instance storage object of
        # a Connection. This meant that this method (and the oid
        # cache) was shared among all connections using a database and
        # was called outside of a transaction (starting its own
        # long-running transaction).

        # The DB.new_oid() method still exists, but shouldn't be used;
        # if it is, we'll open a database connection and transaction that's
        # going to sit there idle, possibly holding row locks. That's bad.
        # But we don't take any counter measures.

        # Connection.new_oid() can be called at just about any time
        # thanks to the Connection.add() API, which clients can use
        # at any time (typically before commit begins, but it's possible to
        # add() objects from a ``__getstate__`` method).
        #
        # Thus we may or may not have a store connection already open;
        # if we do, we can't restart it or drop it.
        if not self.preallocated_oids:
            self.__preallocate_oids(commit_in_progress)
            # OIDs are monotonic, always increasing. It should never
            # go down or return equal to what we've already seen.
            self.max_allocated_oid = max(self.preallocated_oids[0], self.max_allocated_oid)

        oid_int = self.preallocated_oids.pop()
        return int64_to_8bytes(oid_int)

    def __preallocate_oids(self, commit_in_progress): # pylint:disable=method-hidden
        self.preallocated_oids = self.store_connection.call(
            self.__new_oid_callback,
            can_reconnect=not commit_in_progress
        )

    def __preallocate_oids_no_cursor(self, commit_in_progress):# pylint:disable=unused-argument
        self.preallocated_oids = self.oidallocator.new_oids_no_cursor()

    def __new_oid_callback(self, _store_conn, store_cursor, _fresh_connection):
        return self.oidallocator.new_oids(store_cursor)


@implementer(IStaleAware)
class ReadOnlyOIDs(AbstractOIDs):

    __slots__ = (
    )

    def new_oid(self, commit_in_progress):
        raise ReadOnlyError

    def set_min_oid(self, max_observed_oid):
        raise ReadOnlyError

@implementer(IStaleAware)
class StaleOIDs(AbstractOIDs):

    __slots__ = (
        'stale_error',
        'previous',
    )

    def __init__(self, stale_error, previous):
        self.stale_error = stale_error
        self.previous = previous

    def no_longer_stale(self):
        return self.previous

    def stale(self, ex):
        return self

    def new_oid(self, commit_in_progress):
        raise self.stale_error

    def set_min_oid(self, max_observed_oid):
        raise self.stale_error
