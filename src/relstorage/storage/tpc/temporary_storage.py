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
Temporary local storage for pickles that
will be uploaded to the database and the local
and memcache.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tempfile import SpooledTemporaryFile

from relstorage._compat import OID_OBJECT_MAP_TYPE as OidObjectMap
from relstorage._compat import OidObjectMap_max_key
from relstorage._compat import iteroiditems
from relstorage._compat import NStringIO


class _AbstractTPCTemporaryStorage(object):
    __slots__ = (
        '_buffer',
        '_buffer_index',
    )

    def __init__(self):
        # start with a fresh in-memory buffer instead of reusing one that might
        # already be spooled to disk.
        # TODO: An alternate idea would be a temporary sqlite database.
        # pylint:disable-next=consider-using-with
        self._buffer = SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        # {oid: (startpos, endpos, prev_tid_int)}
        # OR {oid: (None, None, prev_tid_int)} to indicate a deletion.
        self._buffer_index = OidObjectMap()

    def reset(self):
        self._buffer_index.clear()
        self._buffer.seek(0)

    def delete_object(self, oid_int, prev_tid_int=0):
        raise NotImplementedError

    def store_temp(self, oid_int, state, prev_tid_int=0):
        """
        Queue an object for storage.

        You may queue an object for storage more than once.
        You may queue an object for storage and also delete it.
        Only the most recent of those actions has any effect.
        """
        queue = self._buffer
        queue.seek(0, 2)  # seek to end
        startpos = queue.tell()
        queue.write(state)
        endpos = queue.tell()
        self._buffer_index[oid_int] = (startpos, endpos, prev_tid_int)

    def __len__(self):
        """
        Answer how many distinct OIDs have been stored plus
        how many have been deleted.

        If we are closed, this is always 0 (letting us be false once
        closed).
        """
        return len(self._buffer_index)

    @property
    def stored_oids(self):
        return self._buffer_index

    @property
    def max_stored_oid(self):
        return OidObjectMap_max_key(self._buffer_index)

    def _read_temp_state(self, startpos, endpos):
        self._buffer.seek(startpos)
        length = endpos - startpos
        state = self._buffer.read(length)
        if len(state) != length:
            raise AssertionError("Queued cache data is truncated")
        return state

    def read_temp(self, oid_int):
        """
        Return the bytes for a previously stored temporary item.
        """
        startpos, endpos, _ = self._buffer_index[oid_int]
        return self._read_temp_state(startpos, endpos)

    def __iter__(self):
        """
        Iterating this object iterates across all the stored
        objects: ``state, oid_int, prev_tid_int``.
        """
        return self.iter_for_oids(None)

    def iter_for_oids(self, oids):
        read_temp_state = self._read_temp_state
        for startpos, endpos, oid_int, prev_tid_int in self.items(oids):
            state = read_temp_state(startpos, endpos)
            yield state, oid_int, prev_tid_int

    def items(self, oids=None):
        """
        If *oids* is given, it is a container of OID integers;
        only objects present in that container are returned.

        Returns an iterable of ``(startpos, endpos, oid_int, prev_tid_int)``
        tuples.
        """
        # Order the queue by file position, which should help
        # if the file is large and needs to be read
        # sequentially from disk.
        items = [
            (startpos, endpos, oid_int, prev_tid_int)
            for (oid_int, (startpos, endpos, prev_tid_int))
            in iteroiditems(self._buffer_index)
            if oids is None or oid_int in oids
        ]
        items.sort()
        return items

    def close(self):
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
            self._buffer_index = () # Not None so len() keeps working

    def __repr__(self):
        approx_size = 0
        if self._buffer is not None:
            self._buffer.seek(0, 2)  # seek to end
            # The number of bytes we stored isn't necessarily the
            # number of bytes we send to the server, if there are duplicates
            approx_size = self._buffer.tell()
        return "<%s at 0x%x count=%d bytes=%d>" % (
            type(self).__name__,
            id(self),
            len(self),
            approx_size
        )

    def __str__(self):
        base = repr(self)
        if not self:
            return base

        out = NStringIO()

        div = '=' * len(base)
        headings = ['OID', 'Length', 'Previous TID']
        col_width = (len(base) - 5) // len(headings)

        print(base, file=out)
        print(div, file=out)
        print('| ', file=out, end='')
        for heading in headings:
            print('%-*s' % (col_width, heading), end='', file=out)
            print('| ', end='', file=out)
        out.seek(out.tell() - 3)
        print('|', file=out)
        print(div, file=out)

        items = sorted(
            (oid_int, endpos - startpos, prev_tid_int)
            for (startpos, endpos, oid_int, prev_tid_int)
            in self.items()
        )

        for oid_int, length, prev_tid_int in items:
            print('%*d  |%*d |%*d' % (
                col_width, oid_int,
                col_width, length,
                col_width, prev_tid_int
            ), file=out)


        return out.getvalue()

_DELETED_STATE = b'<this revision has been deleted>'

class HPTPCTemporaryStorage(_AbstractTPCTemporaryStorage):
    def delete_object(self, oid_int, prev_tid_int=0):
        """
        This version actually writes the deleted marker.
        """
        # We use a real byte string for the state, because it turns out
        # this simplifies some things, as opposed to using None.
        self._buffer_index.pop(oid_int, None)

        # Internally, we store a state string for these. This
        # helps keep everything consistent.
        self.store_temp(oid_int, _DELETED_STATE, prev_tid_int)

    def _read_temp_state(self, startpos, endpos):
        # Specialized to return None for the deleted state.
        state = super()._read_temp_state(startpos, endpos)
        return state if state != _DELETED_STATE else None

    def has_deleted_and_active_objects(self):
        # Rather than two separate iterations with APIs
        # has_deleted_objects() and has_active_objects(),
        # we do both in one iteration.
        deleted = False
        active = False
        for state, _, _ in self:
            if state == _DELETED_STATE:
                deleted = True
            else:
                active = True
            if deleted and active:
                break
        return deleted and active


class HFTPCTemporaryStorage(_AbstractTPCTemporaryStorage):
    def delete_object(self, oid_int, prev_tid_int=0):
        """
        There is nothing todo for histery-free temp storage.
        """
