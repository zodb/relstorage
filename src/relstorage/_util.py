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

import collections
import functools
import itertools
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

def _thread_spawn(func, args):
    import threading
    t = threading.Thread(target=func, args=args)
    t.name = t.name + '-spawn-' + func.__name__
    t.start()
    return t

def _gevent_pool_spawn(func, args):
    import gevent
    return gevent.get_hub().threadpool.spawn(func, *args)

def spawn(func, args=()):
    """Execute func in a different (real) thread"""

    submit = _thread_spawn
    try:
        import gevent.monkey
        import gevent
    except ImportError:
        pass
    else:
        if gevent.monkey.is_module_patched('threading'):
            submit = _gevent_pool_spawn
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
    """
    if size == 0:
        return '0 KB'
    if size <= 1024:
        return '1 KB'
    if size > 1048576:
        return '%0.02f MB' % (size / 1048576.0)
    return '%d KB' % (size / 1024.0)


class Lazy(object):
    "Property-like descriptor that calls func only once per instance."

    # Derived from zope.cachedescriptors.property.Lazy

    def __init__(self, func, name=None):
        if name is None:
            name = func.__name__
        self.data = (func, name)
        functools.update_wrapper(self, func)

    def __get__(self, inst, class_):
        if inst is None:
            return self

        func, name = self.data
        value = func(inst)
        inst.__dict__[name] = value
        return value

def to_utf8(data):
    if data is None or isinstance(data, bytes):
        return data
    return data.encode("utf-8")

def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is none, consume entirely."
    # From the itertools documentation.
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(itertools.islice(iterator, n, n), None)
