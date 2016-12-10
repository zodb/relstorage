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

import six
import abc

@six.add_metaclass(abc.ABCMeta)
class AbstractOIDAllocator(object):
    # All of these allocators allocate 16 OIDs at a time.  In the sequence
    # or table, value (n) represents (n * 16 - 15) through (n * 16).  So,
    # value 1 represents OID block 1-16, 2 represents OID block 17-32,
    # and so on.

    @abc.abstractmethod
    def set_min_oid(self, cursor, oid):
        raise NotImplementedError()

    @abc.abstractmethod
    def new_oids(self, cursor):
        raise NotImplementedError()
