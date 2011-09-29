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

import pylibmc
from _pylibmc import MemcachedError  # pylibmc >= 0.9
import logging

log = logging.getLogger(__name__)


class Client(object):
    behaviors = {
        "tcp_nodelay": True,
        "ketama": True,
    }

    def __init__(self, servers):
        self._client = pylibmc.Client(servers, binary=True)
        self._client.set_behaviors(self.behaviors)
        if pylibmc.support_compression:
            self.min_compress_len = 1000
        else:
            self.min_compress_len = 0

    def get(self, key):
        try:
            return self._client.get(key)
        except MemcachedError, e:
            log.warning('get failed: %s', e)
            return None

    def get_multi(self, keys):
        try:
            return self._client.get_multi(keys)
        except MemcachedError, e:
            log.warning('get_multi failed: %s', e)
            return None

    def set(self, key, value):
        try:
            return self._client.set(
                key, value, min_compress_len=self.min_compress_len)
        except MemcachedError, e:
            log.warning('set failed: %s', e)
            return None

    def set_multi(self, d):
        try:
            return self._client.set_multi(
                d, min_compress_len=self.min_compress_len)
        except MemcachedError, e:
            log.warning('set_multi failed: %s', e)
            return None

    def add(self, key, value):
        try:
            return self._client.add(
                key, value, min_compress_len=self.min_compress_len)
        except MemcachedError, e:
            log.warning('add failed: %s', e)
            return None

    def incr(self, key):
        try:
            return self._client.incr(key)
        except MemcachedError, e:
            log.warning('incr failed: %s', e)
            return None

    def flush_all(self):
        try:
            self._client.flush_all()
        except MemcachedError, e:
            log.warning('flush_all failed: %s', e)
            return None
