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
import sys
import time
import traceback

from persistent.timestamp import TimeStamp

logger = __import__('logging').getLogger(__name__)


def timestamp_at_unixtime(now):
    """
    Return a :class:`persistent.timestamp.TimeStamp` for the moment
    given by *now* (a float giving seconds since the epoch).
    """

    # ``now`` is a float (double precision.). We extract the seconds
    # value using ``now % 60.0``. But performing a modulus operation
    # on it tends to invent precision that we do not really have.
    #
    # For example, given the timestamp ``1564178539.353595``, the
    # correct value for seconds is obviously ``19.353595``. But Python
    # tends to answer ``19.35359501838684``. When the TimeStamp
    # constructor takes that double precision value and applies *more*
    # arithmetic to it to set the bias, the results get worse: we get
    # ``1385384294.4`` when we should get ``1385384293.0838187``.
    #
    # If we're comparing with a system that doesn't use exactly IEEE double
    # arithmetic to calculate the seconds value, this can lead to wrong answers.

    gmtime = time.gmtime(now)
    seconds = now % 60.0
    return TimeStamp(*(gmtime[:5] + (seconds,)))


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

class CachedIn(object):
    """Cached method with given cache attribute."""

    def __init__(self, attribute_name, factory=dict):
        self.attribute_name = attribute_name
        self.factory = factory

    def __call__(self, func):

        @functools.wraps(func)
        def decorated(instance):
            cache = self.cache(instance)
            key = () # We don't support arguments right now, so only one key.
            try:
                v = cache[key]
            except KeyError:
                v = cache[key] = func(instance)
            return v

        decorated.invalidate = self.invalidate
        return decorated

    def invalidate(self, instance):
        cache = self.cache(instance)
        key = ()
        try:
            del cache[key]
        except KeyError:
            pass

    def cache(self, instance):
        try:
            cache = getattr(instance, self.attribute_name)
        except AttributeError:
            cache = self.factory()
            setattr(instance, self.attribute_name, cache)
        return cache


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

class CloseTracker(object): # pragma: no cover
    __slots__ = ('_tracked', '_type', '_at')

    def __init__(self, conn):
        self._tracked = conn
        self._type = type(conn)
        self._at = ''.join(traceback.format_stack())

    def __getattr__(self, name):
        return getattr(self._tracked, name)

    def __setattr__(self, name, value):
        if name in CloseTracker.__slots__:
            object.__setattr__(self, name, value)
            return
        return setattr(self._tracked, name, value)

    def close(self):
        if self._tracked is None:
            return
        try:
            self._tracked.close()
        finally:
            self._tracked = None

    def __del__(self):
        if self._tracked is not None:
            print("Failed to close", self, self._type, " from:", self._at, file=sys.stderr)
            print("Deleted at", ''.join(traceback.format_stack()))


class CloseTrackedConnection(CloseTracker): # pragma: no cover
    __slots__ = ()

    def cursor(self, *args, **kwargs):
        return CloseTrackerCursor(self._tracked.cursor(*args, **kwargs))


class CloseTrackerCursor(CloseTracker):
    __slots__ = ()

    def execute(self, stmt, args=None):
        return self._tracked.execute(stmt, args)

    def __iter__(self):
        return self._tracked.__iter__()


class NeedsFetchallBeforeCloseCursor(CloseTrackerCursor): # pragma: no cover
    needs_fetchall = False
    verbose = False
    last_stmt = None

    def close(self):
        if self.needs_fetchall:
            raise AssertionError("Forgot to fetchall")
        super(NeedsFetchallBeforeCloseCursor, self).close()

    def execute(self, stmt, args=None):
        if self.needs_fetchall:
            raise AssertionError("Forgot to fetchall")
        self.last_stmt = stmt
        if stmt.strip().startswith('SELECT'):
            self.needs_fetchall = True
        if self.verbose:
            print(stmt, end=' -> ')
            result = self._tracked.execute(stmt, args)
            print(result)
            return result
        return self._tracked.execute(stmt, args)

    def fetchone(self):
        r = self._tracked.fetchone()
        if r is None:
            self.needs_fetchall = False
        return r

    def fetchall(self):
        self.needs_fetchall = False
        return self._tracked.fetchall()

    def __repr__(self):
        return "<%r need_fetch=%s last=%r>" % (
            self._tracked,
            self.needs_fetchall,
            self.last_stmt,
        )
