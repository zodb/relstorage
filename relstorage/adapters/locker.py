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
Locker implementations.
"""

from __future__ import absolute_import

import six
import abc

@six.add_metaclass(abc.ABCMeta)
class AbstractLocker(object):

    def __init__(self, options, lock_exceptions):
        self.keep_history = options.keep_history
        self.commit_lock_timeout = options.commit_lock_timeout
        self.commit_lock_id = options.commit_lock_id
        self.lock_exceptions = lock_exceptions

    @abc.abstractmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def release_commit_lock(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def hold_pack_lock(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def release_pack_lock(self, cursor):
        raise NotImplementedError()
