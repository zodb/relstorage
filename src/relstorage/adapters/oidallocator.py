##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
IOIDAllocator implementations.
"""

from __future__ import absolute_import

import abc
from contextlib import contextmanager

from relstorage._util import metricmethod
from relstorage._compat import ABC

from .schema import Schema

logger = __import__('logging').getLogger(__name__)

class AbstractOIDAllocator(ABC):
    """
    These objects are intended to be stateless.

    They consult an underlying database sequence than increases, and
    then derive additional OIDs from that sequence. ``new_oids``
    returns a list of those OIDs, with the largest such OID in
    position 0.
    """
    __slots__ = ()

    def new_instance(self):
        return self

    close = release = lambda self: None

    def reset_oid(self, cursor):
        raise NotImplementedError

    @abc.abstractmethod
    def set_min_oid(self, cursor, oid_int):
        raise NotImplementedError

    @abc.abstractmethod
    def new_oids(self, cursor):
        raise NotImplementedError

# All of these allocators allocate 16 OIDs at a time. In the sequence
# or table, value (n) represents (n * 16 - 15) through (n * 16). So,
# value 1 represents OID block 1-16, 2 represents OID block 17-32, and
# so on. The _oid_range_around helper method returns a list around
# this number sorted in the proper way (largest to smallest, so that
# list.pop() returns the smallest remaining number, and so on:
# [16, 15, ..., 1] when given 1)
#
# Given:
#    num_oids = 16
#    highest_inclusive = n * num_oids2
#    highest_exclusive = highest_inclusive - 1
#    lowest_inclusive = highest_inclusive - (num_oids - 1)
#    lowest_exclusive = lowest_inclusive - 1

# Note that
#    range(lowest_inclusive, highest_inclusive + 1).sort(reverse=True)
#
# is the same as
#     range(highest_inclusive, lowest_exclusive, -1)

_OID_RANGE_SIZE = 16
def _oid_range_around_assume_list(n, _s=_OID_RANGE_SIZE):
    return range(n * _s, n * _s - _s, -1)

def _oid_range_around_iterable(n, _s=_OID_RANGE_SIZE, _range=_oid_range_around_assume_list):
    return list(_range(n))


class AbstractRangedOIDAllocator(AbstractOIDAllocator):
    __slots__ = ()

    def set_min_oid(self, cursor, oid_int):
        # This takes a user-space OID and turns it into the internal
        # range number.
        n = (oid_int + 15) // 16
        self._set_min_oid_from_range(cursor, n)

    @abc.abstractmethod
    def _set_min_oid_from_range(self, cursor, n):
        raise NotImplementedError

    if isinstance(range(1), list):
        # Py2
        _oid_range_around = staticmethod(_oid_range_around_assume_list)
    else:
        _oid_range_around = staticmethod(_oid_range_around_iterable)


class AbstractTableOIDAllocator(AbstractRangedOIDAllocator):
    """
    Implements OID allocation using a table, new_oid, with an incrementing
    column.
    """
    # pylint:disable=abstract-method

    #: To use bound queries, we need to know whether we're keeping history.
    #: It doesn't change anything, though.
    keep_history = None

    #: After this many allocated OIDs should the (unlucky) thread that
    #: allocated the one evenly divisible by this number attempt to remove
    #: old OIDs. Set to None to disable periodic garbage collection.
    garbage_collect_interval = 100001

    # How many OIDs to attempt to delete at any one request. Keeping
    # this on the small side relative to the interval limits the
    # chances of deadlocks (by observation).
    garbage_collect_interactive_size = 1000

    # How many OIDs to attempt to delete if we're not allocating, only
    # garbage collecting.
    garbage_collect_batch_size = 3000

    # How long to wait in seconds before timing out when batch deleting locks.
    # Note that we don't do this interactively because our connection
    # already has a custom timeout.
    garbage_collect_batch_timeout = 5

    def __init__(self, driver):
        # Take the driver so we can bind to our queries
        self.driver = driver

    _insert_stmt = Schema.new_oid.insert()

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        self._insert_stmt.execute(cursor)

        # This is a DB-API extension. Fortunately, all
        # supported drivers implement it.
        n = cursor.lastrowid

        if self.garbage_collect_interval and n % self.garbage_collect_interval == 0:
            self.garbage_collect_oids(cursor, n)
        return self._oid_range_around(n)

    @contextmanager
    def _timeout(self, cursor, batch_timeout):
        yield (cursor, batch_timeout)

    def garbage_collect_oids(self, cursor, max_value=None):
        # Clean out previously generated OIDs.
        batch_size = self.garbage_collect_interactive_size
        batch_timeout = None
        if not max_value:
            batch_size = self.garbage_collect_batch_size
            batch_timeout = self.garbage_collect_batch_timeout
            cursor.execute('SELECT MAX(zoid) FROM new_oid')
            row = cursor.fetchone()
            if row:
                max_value = row[0]
        if not max_value:
            return
        # Delete old OIDs, starting with the oldest.
        stmt = """
        DELETE IGNORE
        FROM new_oid
        WHERE zoid < %s
        ORDER BY zoid ASC
        LIMIT %s
        """
        params = (max_value, batch_size)
        __traceback_info__ = stmt, params
        rowcount = True
        with self._timeout(cursor, batch_timeout):
            while rowcount:
                try:
                    cursor.execute(stmt, params)
                except Exception: # pylint:disable=broad-except
                    # Luckily, a deadlock only rolls back the previous
                    # statement, not the whole transaction.
                    #
                    # XXX: No, that's not true. A general error, like
                    # a lock timeout, will roll back the previous
                    # statement. A deadlock rolls back the whole
                    # transaction. We're lucky the difference here
                    # doesn't make any difference: we don't actually
                    # write anything to the database temp tables, etc,
                    # until the storage enters commit(), at which
                    # point we shouldn't need to allocate any more new
                    # oids.
                    #
                    # TODO: We'd prefer to only do this for errcode 1213: Deadlock.
                    # MySQLdb raises this as an OperationalError; what do all the other
                    # drivers do?
                    logger.debug("Failed to garbage collect allocated OIDs", exc_info=True)
                    break
                else:
                    rowcount = cursor.rowcount
                    logger.debug("Garbage collected %s old OIDs less than %s", rowcount, max_value)

    _reset_oid_query = Schema.new_oid.truncate()

    def reset_oid(self, cursor):
        self._reset_oid_query.execute(cursor)
