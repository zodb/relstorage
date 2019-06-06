# -*- coding: utf-8 -*-
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
from __future__ import division
from __future__ import print_function

import functools
import time

logger = __import__('logging').getLogger(__name__)


class timer(object):
    begin = None
    end = None
    duration = None

    try:
        from pyperf import perf_counter as counter
    except ImportError: # pragma: no cover
        counter = time.time

    def __enter__(self):
        self.begin = self.counter()
        return self

    def __exit__(self, t, v, tb):
        self.end = self.counter()
        self.duration = self.end - self.begin

def log_timed(func):
    @functools.wraps(func)
    def f(*args, **kwargs):
        t = timer()
        with t:
            result = func(*args, **kwargs)
        logger.debug("Function %s took %s", func.__name__, t.duration)
        return result
    return f
