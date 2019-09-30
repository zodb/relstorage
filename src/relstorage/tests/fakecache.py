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
"""A memcache-like module sufficient for testing without an actual memcache.
"""

data = {}

class Client(object):

    def __init__(self, servers):
        self.servers = servers

    def delete(self, key):
        data.pop(key, None)

    def get(self, key):
        return data.get(key)

    def get_multi(self, keys):
        return dict((key, data.get(key)) for key in keys)

    def set(self, key, value):
        data[key] = value

    def set_multi(self, d):
        data.update(d)

    def add(self, key, value):
        if key not in data:
            data[key] = value

    def incr(self, key):
        value = data.get(key)
        if value is None:
            return None
        value = int(value) + 1
        data[key] = value
        return value

    def flush_all(self, **_kwargs):
        data.clear()

    def disconnect_all(self):
        # no-op
        pass
