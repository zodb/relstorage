# -*- coding: utf-8 -*-
"""
Supporting code.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools

def storage_method(meth):
    """
    Decorator to mark the decorated method as being a storage method,
    to be copied to the storage instance.
    """
    meth.storage_method = True
    return meth

def phase_dependent(meth):
    """
    Decorator to mark the method as requiring the current
    transaction phase as its first argument.

    These are methods that can only be called after ``tpc_begin``.
    """
    meth.phase_dependent = True
    return meth

class _StaleMethodWrapper(object):

    __slots__ = (
        'orig_method',
        'stale_error',
    )

    def __init__(self, orig_method, stale_error):
        self.orig_method = orig_method
        self.stale_error = stale_error

    def __call__(self, *args, **kwargs):
        raise self.stale_error

    def no_longer_stale(self):
        return self.orig_method

class stale_aware(object):

    __slots__ = (
        '_func',
    )

    def __init__(self, func):
        self._func = func

    def __get__(self, inst, klass):
        if inst is None:
            return self

        bound = self._func.__get__(inst, klass)
        # Because we cannot add attributes to a bound method,
        # and because we want to limit the overhead of additional
        # function calls (these methods are commonly used),
        # we derive a new class whose __call__ is exactly the bound
        # method's __call__.  For this reason, its important to access
        # these methods only once and cache them (which is what copy_storage_methods
        # does).
        class Callable(object):

            __call__ = bound.__call__

            def __getattr__(self, name):
                return getattr(bound, name)

            def no_longer_stale(self):
                return self

            def stale(self, stale_error):
                return _StaleMethodWrapper(self, stale_error)

        return Callable()

def _make_phase_dependent(storage, method):
    @functools.wraps(method)
    def state(*args, **kwargs):
        return method(storage._tpc_phase, *args, **kwargs)
    return state

def copy_storage_methods(storage, delegate):
    # type: (RelStorage, Any) -> None

    for var_name in dir(delegate):
        __traceabck_info__ = var_name
        if var_name.startswith('_'):
            continue
        attr = getattr(delegate, var_name)
        if callable(attr) and getattr(attr, 'storage_method', None):
            if getattr(attr, 'phase_dependent', None):
                attr = _make_phase_dependent(storage, attr)
            setattr(storage, var_name, attr)
