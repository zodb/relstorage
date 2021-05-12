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
import itertools
import sys
import time
import traceback
import os

import logging
from logging import DEBUG
from logging import INFO
from logging import WARN
from logging import ERROR

from persistent.timestamp import TimeStamp

from ZConfig.datatypes import asBoolean
from ZConfig.datatypes import integer
from ZConfig.datatypes import RangeCheckedConversion
from ZConfig.datatypes import stock_datatypes

from ZODB.utils import p64
from ZODB.utils import u64
# Trace is beneath (less important than) DEBUG.
# It is "extremely verbose"
from ZODB.loglevels import TRACE

from perfmetrics import metricmethod
from perfmetrics import Metric

from relstorage._compat import wraps
from relstorage._compat import update_wrapper
from relstorage._compat import perf_counter
from relstorage._compat import IN_TESTRUNNER

_logger = logging.getLogger('relstorage')
perf_logger = _logger.getChild('timing')

int64_to_8bytes = p64
bytes8_to_int64 = u64

__all__ = [
    'int64_to_8bytes',
    'bytes8_to_int64',

    'CachedIn',
    'Lazy',
    'TRACE',
    'byte_display',
    'consume',

    'get_duration_from_environ',
    'get_positive_integer_from_environ',
    'get_non_negative_float_from_environ',
    'get_boolean_from_environ',

    'get_memory_usage',
    'log_timed',
    'log_timed_only_self',
    'metricmethod',
    'metricmethod_sampled',
    'parse_boolean',
    'parse_byte_size',
    'positive_integer',
    'spawn',
    'thread_spawn',
    'timer',
    'timestamp_at_unixtime',
    'to_utf8',
]

positive_integer = RangeCheckedConversion(integer, min=1)
non_negative_float = RangeCheckedConversion(float, min=0)

def _setting_from_environ(converter, environ_name, default, logger):
    result = default
    env_val = os.environ.get(environ_name, default)
    if env_val is not default:
        try:
            result = converter(env_val)
        except (ValueError, TypeError):
            logger.exception("Failed to parse environment value %r for key %r",
                             env_val, environ_name)
            result = default

    logger.debug('Using value %s from environ %r=%r (default=%r)',
                 result, environ_name, env_val, default)
    return result


def get_positive_integer_from_environ(environ_name, default, logger=_logger):
    return _setting_from_environ(positive_integer, environ_name, default, logger)

def get_non_negative_float_from_environ(environ_name, default, logger=_logger):
    return _setting_from_environ(non_negative_float, environ_name, default, logger)

def parse_boolean(val):
    if val == '0':
        return False
    if val == '1':
        return True
    return asBoolean(val)

parse_byte_size = stock_datatypes['byte-size']

def get_boolean_from_environ(environ_name, default, logger=_logger):
    return _setting_from_environ(parse_boolean, environ_name, default, logger)

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
    __begin = None
    __end = None
    duration = None

    counter = perf_counter

    def __enter__(self):
        self.__begin = self.counter()
        return self

    def __exit__(self, t, v, tb):
        self.__end = self.counter()
        self.duration = self.__end - self.__begin

def get_duration_from_environ(environ_name, default, logger=_logger):
    """
    Return a floating-point number of seconds from the environment *environ_name*,
    or *default*.

    Examples: ``1.24s``, ``3m``, ``1m 3.6s``::

        >>> import os
        >>> os.environ['RS_TEST_VAL'] = '2.3'
        >>> get_duration_from_environ('RS_TEST_VAL', None)
        2.3
        >>> os.environ['RS_TEST_VAL'] = '5.4s'
        >>> get_duration_from_environ('RS_TEST_VAL', None)
        5.4
        >>> os.environ['RS_TEST_VAL'] = '1m 3.2s'
        >>> get_duration_from_environ('RS_TEST_VAL', None)
        63.2
        >>> os.environ['RS_TEST_VAL'] = 'Invalid' # No time specifier
        >>> get_duration_from_environ('RS_TEST_VAL', 42)
        42
        >>> os.environ['RS_TEST_VAL'] = 'Invalids' # The 's' time specifier
        >>> get_duration_from_environ('RS_TEST_VAL', 42)
        42
    """

    def convert(val):
        # The default time-interval accepts only integers; that's not fine
        # grained enough for these durations.
        if any(c in val for c in ' wdhms'):
            delta = stock_datatypes['timedelta'](val)
            return delta.total_seconds()
        return float(val)

    return _setting_from_environ(convert, environ_name, default, logger)

def _get_log_time_level(level_int, default):
    level_name = logging.getLevelName(level_int)
    val = get_duration_from_environ('RS_PERF_LOG_%s_MIN' % level_name, default, logger=perf_logger)
    return (level_int, float(val))

# A list of tuples (level_int, min_duration), ordered by increasing
# min_duration. If you modify this list at runtime, be sure to keep it
# sorted. A quick way to completely disable logging is to clear this
# list. Modify this list in place to apply to all functions; make a
# copy and place in `func.__wrapped__.log_levels` or
# `Class.meth.__wrapped__.log_levels` to change for an individual
# function.
_LOG_TIMED_DEFAULT_DURATIONS = [
    _get_log_time_level(TRACE, 0.31),
    _get_log_time_level(DEBUG, 1.24),
    _get_log_time_level(INFO, 3.03),
    _get_log_time_level(WARN, 9.24),
    _get_log_time_level(ERROR, 20.10)
]

_LOG_TIMED_DEFAULT_DURATIONS.sort(key=lambda x: x[1])

# timed events above this threshold will include detail information.
# The 'log_details_threshold' property of the function can be
# assigned to make it different than the default.
_LOG_TIMED_DEFAULT_DETAILS_THRESHOLD = logging.getLevelName(
    _setting_from_environ(str, 'RS_PERF_LOG_DETAILS_LEVEL', 'WARN', logger=perf_logger)
)


# If this is true when a module is imported, timer decorations
# are omitted.
_LOG_TIMED_COMPILETIME_ENABLE = get_boolean_from_environ(
    'RS_PERF_LOG_ENABLE',
    'on',
    logger=perf_logger,
)

def do_log_duration_info(basic_msg, func,
                         args, kwargs,
                         actual_duration,
                         log=perf_logger):
    if func is None:
        # Timing was disabled at compile time
        return

    log_level = 0
    # Defer capturing the name; it might get changed at
    # runtime.
    log_msg = basic_msg
    log_args = (func.__name__, actual_duration)
    for level, duration in func.log_levels:
        if actual_duration < duration:
            break
        log_level = level

    if not log.isEnabledFor(log_level):
        return

    if log_level >= func.log_details_threshold:
        # This will capture 'self' as the first argument,
        # so you can put useful things into that repr if you'd
        # like.
        try:
            load = os.getloadavg()
        except (OSError, AttributeError):
            load = "<unknown load>"
        mem = byte_display(get_memory_usage())

        if args and kwargs:
            log_msg += " (load=%s) (memory=%s) (args=%r kwargs=%r)"
            log_args += (load, mem, args, kwargs)
        elif args:
            if getattr(func, 'log_args_only_self', None):
                args = args[:1]
            log_msg += " (load=%s) (memory=%s) (args=%r)"
            log_args += (load, mem, args)

    log.log(log_level, log_msg, *log_args)

def log_timed(func):
    # Store these on each individual function so they can be
    # tweaked later: Class.func.__wrapped__.min_duration_to_log = X
    # Do this even if we've disabled the wrappers for a consistent
    # interface.
    func.log_levels = _LOG_TIMED_DEFAULT_DURATIONS
    func.log_details_threshold = _LOG_TIMED_DEFAULT_DETAILS_THRESHOLD
    if not _LOG_TIMED_COMPILETIME_ENABLE:
        # One more we're possibly missing
        if getattr(func, '__wrapped__', func) is func:
            func.__wrapped__ = None
        return func

    counter = perf_counter
    log = do_log_duration_info
    func_logger = logging.getLogger(func.__module__).getChild('timing')


    @wraps(func)
    def f(*args, **kwargs):
        begin = counter()
        try:
            result = func(*args, **kwargs)
        finally:
            end = counter()
            duration = end - begin

            log_msg = "Function %s took %.3fs."
            log(log_msg, func, args, kwargs, duration,
                log=func_logger)

        return result

    return f


def log_timed_only_self(func):
    func.log_args_only_self = 1
    return log_timed(func)

_ThreadWithReady = None

METRIC_SAMPLE_RATE = get_non_negative_float_from_environ('RS_PERF_STATSD_SAMPLE_RATE', 0.1,
                                                         logger=perf_logger)

metricmethod_sampled = Metric(method=True, rate=METRIC_SAMPLE_RATE)

if IN_TESTRUNNER and os.environ.get('RS_TEST_DISABLE_METRICS'):
    # If we're running under the testrunner,
    # don't apply the metricmethod stuff. It makes
    # backtraces ugly and makes stepping in the
    # debugger annoying.
    metricmethod = metricmethod_sampled = lambda f: f


def thread_spawn(func, args=(), daemon=False):
    global _ThreadWithReady
    if _ThreadWithReady is None:
        import threading

        class T(threading.Thread):
            # Note that this is going to be a bit racy
            # between threads; it may not appear in a reading
            # thread right away.
            __ready = False

            def ready(self):
                return self.__ready

            def run(self):
                try:
                    super(T, self).run()
                finally:
                    self.__ready = True

            def wait(self, timeout=None):
                return self.join(timeout)

        _ThreadWithReady = T

    t = _ThreadWithReady(target=func, args=args)
    t.name = t.name + '-spawn-' + func.__name__
    if daemon:
        t.setDaemon(daemon)
    t.start()
    return t

def _gevent_pool_spawn(func, args=()):
    import gevent
    return gevent.get_hub().threadpool.spawn(func, *args)

def spawn(func, args=()):
    """
    Execute func in a different (real) thread.

    Returns an object with a ``ready()`` method that can be called to
    determine if the *func* has finished running, and a
    ``wait([timeout])`` method that blocks until the function has
    finished running. (This abstracts differences between the gevent
    threadpool and direct use of threads.)
    """

    submit = thread_spawn
    try:
        import gevent.monkey
    except ImportError: # pragma: no cover
        pass
    else:
        if gevent.monkey.is_module_patched('threading'):
            submit = _gevent_pool_spawn

    _logger.log(TRACE, "Using %s to run %s", submit, func)
    return submit(func, args)

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
    except ImportError: # pragma: no cover
        proc = None
    return proc

def get_memory_usage():
    """
    Get a number representing the current memory usage.

    Returns 0 if this is not available.
    """
    proc = get_this_psutil_process()
    if proc is None: # pragma: no cover
        return 0

    rusage = proc.memory_full_info()
    # uss only documented available on Windows, Linux, and OS X.
    # If not available, fall back to rss as an aproximation.
    mem_usage = getattr(rusage, 'uss', 0) or rusage.rss
    return mem_usage

def byte_display(size):
    """
    Returns a string with the correct unit (KB, MB), given the size in bytes.
    """
    if size == 0:
        return '0 KB'
    if size <= 1024:
        return '%s bytes' % size
    if size > 1048576:
        return '%0.02f MB' % (size / 1048576.0)
    return '%0.02f KB' % (size / 1024.0)

class Lazy(object):
    "Property-like descriptor that calls func only once per instance."

    # Derived from zope.cachedescriptors.property.Lazy

    def __init__(self, func, name=None):
        if name is None:
            name = func.__name__
        self.data = (func, name)
        update_wrapper(self, func)

    def __get__(self, inst, class_):
        if inst is None:
            return self

        func, name = self.data
        value = func(inst)
        inst.__dict__[name] = value
        self._stored_value_for_name_in_inst(value, name, inst)
        return value

    @staticmethod
    def _stored_value_for_name_in_inst(value, name, inst):
        """
        Hook for subclasses.
        """

class CachedIn(object):
    """Cached method with given cache attribute."""

    def __init__(self, attribute_name, factory=dict):
        self.attribute_name = attribute_name
        self.factory = factory

    def __call__(self, func):

        @wraps(func)
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
