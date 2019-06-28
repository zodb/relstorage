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
"""A wrapper around pylibmc to make it not raise memcache errors.

One way to use this is to add 'cache-module-name relstorage.pylibmc_wrapper'
to zope.conf and set the 'cache-servers' parameter as well.
"""
import logging
from functools import wraps

import pylibmc  # pylint:disable=import-error
from pylibmc import Error as MemcachedError  # pylint:disable=no-name-in-module,import-error

log = logging.getLogger(__name__)

def _catching(func):
    name = func.__name__

    @wraps(func)
    def wrapper(*args):
        try:
            return func(*args)
        except MemcachedError as e: # pragma: no cover
            log.debug("%s failed: %s", name, e)
            return None
    return wrapper

class Client(object):
    behaviors = {
        "tcp_nodelay": True,
        "ketama": True,
    }

    min_compress_len = 0

    def __init__(self, servers):
        self._client = pylibmc.Client(servers, binary=True)
        self._client.set_behaviors(self.behaviors)
        if pylibmc.support_compression: # pylint:disable=no-member
            self.min_compress_len = 1000

    @_catching
    def delete(self, key):
        return self._client.delete(key)

    @_catching
    def get(self, key):
        return self._client.get(key)

    @_catching
    def get_multi(self, keys):
        return self._client.get_multi(keys)

    @_catching
    def set(self, key, value):
        return self._client.set(
            key, value, min_compress_len=self.min_compress_len)

    @_catching
    def set_multi(self, d):
        return self._client.set_multi(
            d, min_compress_len=self.min_compress_len)

    @_catching
    def add(self, key, value):
        return self._client.add(
            key, value, min_compress_len=self.min_compress_len)

    @_catching
    def flush_all(self):
        return self._client.flush_all()

    @_catching
    def disconnect_all(self):
        return self._client.disconnect_all()
