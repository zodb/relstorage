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
from __future__ import absolute_import, print_function, division

import bz2
import threading
import zlib

from zope import interface

from relstorage._compat import iteritems

from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache import persistence as _Loader
from relstorage.cache.mapping import SizedLRUMapping as LocalClientBucket

@interface.implementer(IPersistentCache)
class LocalClient(object):
    """A memcache-like object that stores in Python dictionaries."""

    # Use the same markers as zc.zlibstorage (well, one marker)
    # to automatically avoid double-compression
    _compression_markers = {
        'zlib': (b'.z', zlib.compress),
        'bz2': (b'.b', bz2.compress),
        'none': (None, None)
    }
    _decompression_functions = {
        b'.z': zlib.decompress,
        b'.b': bz2.decompress
    }

    _bucket_type = LocalClientBucket

    def __init__(self, options, prefix=None):
        self._lock = threading.Lock()
        self.options = options
        self.prefix = prefix or ''
        # XXX: The calc for limit is substantially smaller
        # The real MB value is 1024 * 1024 = 1048576
        self.limit = int(1000000 * options.cache_local_mb)
        self._value_limit = options.cache_local_object_max
        self.__bucket = None
        self.flush_all()

        compression_module = options.cache_local_compression
        try:
            compression_markers = self._compression_markers[compression_module]
        except KeyError:
            raise ValueError("Unknown compression module")
        else:
            self.__compression_marker = compression_markers[0]
            self.__compress = compression_markers[1]
            if self.__compress is None:
                self._compress = None

    @property
    def size(self):
        return self.__bucket.size

    def __len__(self):
        return len(self.__bucket)

    def __iter__(self):
        return iter(self.__bucket)

    def _decompress(self, data):
        pfx = data[:2]
        if pfx not in self._decompression_functions:
            return data
        return self._decompression_functions[pfx](data[2:])

    def _compress(self, data): # pylint:disable=method-hidden
        # We override this if we're disabling compression
        # altogether.
        # Use the same basic rule as zc.zlibstorage, but bump the object size up from 20;
        # many smaller object (under 100 bytes) like you get with small btrees,
        # tend not to compress well, so don't bother.
        if data and (len(data) > 100) and data[:2] not in self._decompression_functions:
            compressed = self.__compression_marker + self.__compress(data)
            if len(compressed) < len(data):
                return compressed
        return data

    def save(self):
        options = self.options
        if options.cache_local_dir and self.__bucket.size:
            _Loader.save_local_cache(options, self.prefix, self)

    def write_to_stream(self, stream):
        with self._lock:
            options = self.options
            return self.__bucket.write_to_stream(stream, options.cache_local_dir_write_max_size)

    def restore(self):
        options = self.options
        if options.cache_local_dir:
            _Loader.load_local_cache(options, self.prefix, self)

    def read_from_stream(self, stream):
        with self._lock:
            return self.__bucket.read_from_stream(stream)

    @property
    def _bucket0(self):
        # For testing only.
        return self.__bucket

    def flush_all(self):
        with self._lock:
            self.__bucket = self._bucket_type(self.limit)

    def reset_stats(self):
        self.__bucket.reset_stats()

    def stats(self):
        return self.__bucket.stats()

    def get(self, key):
        return self.get_multi([key]).get(key)

    def get_multi(self, keys):
        res = {}
        decompress = self._decompress
        get = self.__bucket.get_and_bubble_all

        with self._lock:
            res = get(keys)

        # Finally, while not holding the lock, decompress if needed
        res = {k: decompress(v)
               for k, v in iteritems(res)}

        return res

    def set(self, key, value):
        self.set_multi({key: value})

    def set_multi(self, d):
        if not self.limit:
            # don't bother
            return

        compress = self._compress
        items = [] # [(key, value)]
        for key, value in iteritems(d):
            # This used to allow non-byte values, but that's confusing
            # on Py3 and wasn't used outside of tests, so we enforce it.
            assert isinstance(key, str), (type(key), key)
            assert isinstance(value, bytes)

            cvalue = compress(value) if compress else value # pylint:disable=not-callable

            if len(cvalue) >= self._value_limit:
                # This value is too big, so don't cache it.
                continue
            items.append((key, cvalue))

        bucket0 = self.__bucket
        set_key = bucket0.__setitem__

        with self._lock:
            for key, cvalue in items:
                set_key(key, cvalue)

    def add(self, key, value):
        # We don't use this method. We used to implement it by adding
        # an extra parameter to set_multi and checking for each key
        # before we set it if the param was true, but that was an
        # extra boolean check we don't need to make because we don't use
        # this method.
        raise NotImplementedError()

    def disconnect_all(self):
        # Compatibility with memcache.
        pass
