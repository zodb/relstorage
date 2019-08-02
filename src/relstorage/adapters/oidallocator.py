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
#    highest_inclusive = n * num_oids
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

class AbstractOIDAllocator(object):
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

    @abc.abstractmethod
    def set_min_oid(self, cursor, oid_int):
        raise NotImplementedError

    @abc.abstractmethod
    def new_oids(self, cursor):
        raise NotImplementedError

    if isinstance(range(1), list):
        # Py2
        _oid_range_around = staticmethod(_oid_range_around_assume_list)
    else:
        _oid_range_around = staticmethod(_oid_range_around_iterable)
