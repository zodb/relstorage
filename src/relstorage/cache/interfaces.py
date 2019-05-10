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
from __future__ import absolute_import, print_function, division

from zope.interface import Interface
from zope.interface import Attribute

#pylint: disable=inherit-non-class,no-method-argument,no-self-argument

class IPersistentCache(Interface):
    """
    A cache that can be persisted to a file (or more generally, a stream)
    and later re-populated from that same stream.
    """

    size = Attribute("The byte-size of the entries in the cache.")
    limit = Attribute("The upper bound of the byte-size that this cache should hold.")

    def read_from_stream(stream):
        """
        Populate the cache from the stream.

        This method may be called multiple times to populate the cache from
        different streams, so long as ``size`` is less than ``limit``.

        The stream will be a stream opened for reading in binary mode that was
        originally written by :meth:`write_to_stream`.

        :return: A two-tuple ``(count, stored)``, where ``stored`` is the number of new
            entries added to the cache from this stream. If that number is zero,
            no more streams should be used to populate the cache. ``count`` is an informational
            number showing how many total entries were in the stream.
        """

    def write_to_stream(stream):
        """
        Store the information the cache needs to repopulate itself into the stream.
        """

    def get_cache_modification_time_for_stream():
        """
        Return the timestamp as a number that represents the most recent
        modification time of this cache.

        If there is metadata associated with a stream (as in, when the
        stream is a file), then this number can be stored and later
        used to determine which cache stream has the most recent data.

        This method is optional. If it is not implemented, or returns zero or None,
        the time at which the stream is written will be used.
        """
