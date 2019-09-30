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
An implementation of ``IStateCache`` using a memcache client.

Keys and values are transformed into the (byte)string based
keys and values that memcache accepts.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib

from ZODB.utils import p64
from ZODB.utils import u64
from zope import interface

from relstorage._compat import string_types
from relstorage._compat import iteritems
from relstorage.cache.interfaces import IStateCache


@interface.implementer(IStateCache)
class MemcacheStateCache(object):

    # send_limit: approximate limit on the bytes to buffer before
    # sending to the cache.
    send_limit = 1024 * 1024

    @classmethod
    def from_options(cls, options, prefix=''):
        """
        Create and return a MemcacheStateCache from the options,
        if they so request.
        """
        if not options.cache_servers:
            return
        module_name = options.cache_module_name
        module = importlib.import_module(module_name)
        servers = options.cache_servers
        if isinstance(servers, string_types):
            servers = servers.split()

        return cls(
            lambda: module.Client(servers),
            prefix
        )

    def __init__(self, constructor, prefix):
        self._constructor = constructor
        self.prefix = prefix
        self.client = constructor()
        # checkpoints_key holds the current checkpoints.
        self.checkpoints_key = ck = '%s:checkpoints' % self.prefix
        # no unicode on Py2
        assert isinstance(ck, str), (ck, type(ck))

    def __oid_tid_to_key(self, oid, tid):
        return '%s:state:%d:%d' % (self.prefix, tid, oid)

    def __getitem__(self, oid_tid, peek=False):
        oid, tid = oid_tid
        if tid is None:
            # We don't support frozen keys, only those in the index
            return None

        cachekeys = [self.__oid_tid_to_key(oid, tid)]
        response = self.client.get_multi(cachekeys) or {}
        for key in cachekeys:
            data = response.get(key)
            if data and len(data) >= 8:
                actual_tid_int = u64(data[:8])
                return data[8:], actual_tid_int

    get = __getitem__

    def __contains__(self, oid_tid):
        return self[oid_tid] is not None

    def __setitem__(self, oid_tid, state_bytes_tid):
        oid, tid = oid_tid
        key = self.__oid_tid_to_key(oid, tid)
        state_bytes, actual_tid = state_bytes_tid
        cache_data = p64(actual_tid) + (state_bytes or b'')
        self.client.set(key, cache_data)

    def __delitem__(self, oid_tid):
        self.client.delete(self.__oid_tid_to_key(*oid_tid))

    def invalidate_all(self, oids): # pylint:disable=unused-argument
        """
        Implemented by flushing everything.
        """
        self.flush_all()

    def _set_multi(self, keys_and_values):
        formatted = {
            '%s:state:%d:%d' % (self.prefix, tid, oid): (p64(actual_tid) + (state or b''))
            for (oid, tid), (state, actual_tid) in iteritems(keys_and_values)
        }
        self.client.set_multi(formatted)

    def set_all_for_tid(self, tid_int, state_oid_iter):
        send_size = 0
        to_send = {}
        for state, oid_int, _ in state_oid_iter:
            length = len(state)
            cachekey = (oid_int, tid_int)
            item_size = length + len(cachekey)
            if send_size and send_size + item_size >= self.send_limit:
                self._set_multi(to_send)
                to_send.clear()
                send_size = 0
            to_send[cachekey] = (state, tid_int)
            send_size += item_size

        if to_send:
            self._set_multi(to_send)

    def store_checkpoints(self, cp0_tid, cp1_tid):
        checkpoint_data = '%d %d' % (cp0_tid, cp1_tid)
        checkpoint_data = checkpoint_data.encode('ascii')
        self.client.set(self.checkpoints_key, checkpoint_data)

    def get_checkpoints(self):
        s = self.client.get(self.checkpoints_key)
        if s:
            try:
                c0, c1 = s.split()
                c0 = int(c0)
                c1 = int(c1)
            except ValueError:
                # Invalid checkpoint cache value; ignore it.
                pass
            else:
                # More validation
                return (c0, c1) if c0 >= c1 else None

    def replace_checkpoints(self, expected, change_to):
        cp = self.get_checkpoints()
        if cp is not None and cp != expected:
            return None
        self.store_checkpoints(*change_to)
        return change_to

    def close(self):
        if self.client is not None:
            self.client.disconnect_all()
            self.client = None

    release = close

    def flush_all(self):
        self.client.flush_all()

    def updating_delta_map(self, deltas):
        return deltas

    def new_instance(self):
        return type(self)(self._constructor, self.prefix)
