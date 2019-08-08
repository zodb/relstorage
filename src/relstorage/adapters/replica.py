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
from __future__ import absolute_import

import os
import time

from zope.interface import implementer

from .._compat import metricmethod
from .interfaces import IReplicaSelector


@implementer(IReplicaSelector)
class ReplicaSelector(object):

    # The time at which we checked the config
    _config_checked = 0

    def __init__(self, fn, replica_timeout):
        self.replica_conf = fn
        self.replica_timeout = replica_timeout
        self._read_config()
        self._select(0)
        self._iterating = False
        self._skip_index = None

    def _read_config(self):
        self._config_modified = os.path.getmtime(self.replica_conf)
        self._config_checked = time.time()
        f = open(self.replica_conf, 'r')
        try:
            lines = f.readlines()
        finally:
            f.close()
        replicas = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            replicas.append(line)
        if not replicas:
            raise IndexError(
                "No replicas specified in %s" % self.replica_conf)
        self._replicas = replicas

    def _is_config_modified(self):
        now = time.time()
        if now < self._config_checked + 1:
            # don't check the last mod time more often than once per second
            return False
        self._config_checked = now
        t = os.path.getmtime(self.replica_conf)
        return t != self._config_modified

    def _select(self, index):
        self._current_replica = self._replicas[index]
        self._current_index = index
        if index > 0 and self.replica_timeout:
            self._expiration = time.time() + self.replica_timeout
        else:
            self._expiration = None

    def current(self):
        """Get the current replica."""
        self._iterating = False
        if self._is_config_modified():
            self._read_config()
            self._select(0)
        elif self._expiration is not None and time.time() >= self._expiration:
            self._select(0)
        return self._current_replica

    @metricmethod
    def next(self):
        """Return the next replica to try.

        Return None if there are no more replicas defined.
        """
        if self._is_config_modified():
            # Start over even if iteration was already in progress.
            self._read_config()
            self._select(0)
            self._skip_index = None
            self._iterating = True
        elif not self._iterating:
            # Start iterating.
            self._skip_index = self._current_index
            i = 0
            if i == self._skip_index:
                i = 1
                if i >= len(self._replicas):
                    # There are no more replicas to try.
                    self._select(0)
                    return None
            self._select(i)
            self._iterating = True
        else:
            # Continue iterating.
            i = self._current_index + 1
            if i == self._skip_index:
                i += 1
            if i >= len(self._replicas):
                # There are no more replicas to try.
                self._select(0)
                return None
            self._select(i)

        return self._current_replica
