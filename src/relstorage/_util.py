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

def spawn(func, args=()):
    """Execute func in a different (real) thread"""
    import threading
    submit = lambda func, args: threading.Thread(target=func, args=args).start()
    try:
        import gevent.monkey
        import gevent
    except ImportError:
        pass
    else:
        if gevent.monkey.is_module_patched('threading'):
            submit = lambda func, args: gevent.get_hub().threadpool.spawn(func, *args)
    submit(func, args)

def get_this_psutil_process():
    # Returns a freshly queried object each time.
    try:
        from psutil import Process, AccessDenied
        # Make sure it works (why would we be denied access to our own process?)
        try:
            proc = Process()
            proc.memory_full_info()
        except AccessDenied: # pragma: no cover
            proc = None
    except ImportError:
        proc = None
    return proc

def get_memory_usage():
    """
    Get a number representing the current memory usage.

    Returns 0 if this is not available.
    """
    proc = get_this_psutil_process()
    if proc is None:
        return 0

    rusage = proc.memory_full_info()
    # uss only documented available on Windows, Linux, and OS X.
    # If not available, fall back to rss as an aproximation.
    mem_usage = getattr(rusage, 'uss', 0) or rusage.rss
    return mem_usage

def byte_display(size):
    """
    Returns a size with the correct unit (KB, MB), given the size in bytes.
    The output should be given to zope.i18n.translate()
    """
    if size == 0:
        return '0 KB'
    if size <= 1024:
        return '1 KB'
    if size > 1048576:
        return '%0.02f MB' % (size / 1048576.0)
    return '%d KB' % (size / 1024.0)
