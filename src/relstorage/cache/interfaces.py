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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import Attribute
from zope.interface import Interface

# pylint: disable=inherit-non-class,no-method-argument,no-self-argument
# pylint:disable=unexpected-special-method-signature
# pylint:disable=signature-differs


class IStateCache(Interface):
    """
    The methods we use to store state information.

    This interface is defined in terms of OID and TID *integers*;
    implementations (such as memcache) that only support string
    keys will need to convert.

    All return values for states return (state_bytes, tid_int).

    We use special methods where possible because those are slightly
    faster to invoke.
    """

    def __getitem__(oid_tid):
        """
        Given an (oid, tid) pair, return the cache data (state_bytes,
        tid_int) for that object.

        The returned *tid_int* must match the tid in the key.

        If the (oid, tid) pair isn't in the cache, return None.
        """

    def __call__(oid, tid1, tid2):
        """
        The same as invoking `__getitem__((oid, tid1))` followed by
        `__getitem__((oid, tid2))` if no result was found for the first one.

        If no result is found for *tid1*, but a result is found for *tid2*,
        then this method should cache the result at (oid, tid1) before returning.
        """

    def __setitem__(oid_tid, state_bytes_tid):
        """
        Store the *state_bytes* for the (oid, tid) pair.

        Note that it does not necessarily mean that the key tid
        matches the value tid.
        """

    def set_multi(keys_and_values):
        """
        Given a mapping from keys to values, set them all.
        """

    def store_checkpoints(cp0_tid, cp1_tid):
        """
        Store the suggested pair of checkpoints.
        """

    def get_checkpoints():
        """
        Return the current checkpoints as (cp0_tid, cp1_tid).

        If not found, return None.
        """

    def close():
        """
        Release external resources held by this object.
        """

    def flush_all():
        """
        Clear cached data.
        """

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
