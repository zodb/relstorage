# -*- coding: utf-8 -*-
"""
Supporting code.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools

from ZODB.POSException import ReadOnlyError
from zope.interface import implementer

from .interfaces import IStaleAware

def storage_method(meth):
    """
    Decorator to mark the decorated method as being a storage method,
    to be copied to the storage instance.
    """
    meth.storage_method = True
    return meth

def writable_storage_method(meth):
    """
    Decorator to mark the decorated method as being a storage method,
    to be copied to the storage instance when it is *not* read only.

    If the storage is read only, then calling this method will
    immediately raise a ``ReadOnlyError``.
    """
    meth = storage_method(meth)
    meth.write_method = True
    return meth

def phase_dependent(meth):
    """
    Decorator to mark the method as requiring the current
    transaction phase as its first argument.

    These are methods that can only be called after ``tpc_begin``.
    """
    meth.phase_dependent = True
    return meth


class _StaleMethodWrapperType(type):
    # Make the _StaleMethodWrapper class also a descriptor
    # so that when we put it in the class dictionary, it
    # gets a chance to bind to the instance.
    def __get__(cls, inst, klass):
        return functools.partial(cls, inst)

@implementer(IStaleAware)
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

    def stale(self, stale_error): # pylint:disable=unused-argument
        return self

_StaleMethodWrapper = _StaleMethodWrapperType(
    '_StaleMethodWrapper',
    (object,),
    {
        k: v
        for k, v in _StaleMethodWrapper.__dict__.items()
        if k not in _StaleMethodWrapper.__slots__
    }
)

class _StaleAwareMethodTemplate(object):

    __slots__ = (
        '_wrapped',
    )

    def __init__(self, wrapped):
        self._wrapped = wrapped

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def no_longer_stale(self):
        return self

    stale = _StaleMethodWrapper

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

        stale_aware_class = type(
            'StaleAware_' + bound.__name__,
            (_StaleAwareMethodTemplate,),
            {
                '__slots__': (),
                '__call__': bound.__call__,
            }
        )

        stale_aware_class = implementer(IStaleAware)(stale_aware_class)
        # update_wrapper() doesn't work on a type.
        # stale_aware_class = functools.wraps(bound)(stale_aware_class)
        return stale_aware_class(bound)

def _make_phase_dependent(storage, method):
    @functools.wraps(method)
    def state(*args, **kwargs):
        return method(storage._tpc_phase, *args, **kwargs)
    return state

def _make_cannot_write(method):
    @functools.wraps(method)
    def read_only(*args, **kwargs):
        raise ReadOnlyError
    return read_only

def copy_storage_methods(storage, delegate):
    # type: (RelStorage, Any) -> None

    read_only = storage.isReadOnly()

    for var_name in dir(delegate):
        __traceabck_info__ = var_name
        if var_name.startswith('_'):
            continue
        attr = getattr(delegate, var_name)
        if callable(attr) and getattr(attr, 'storage_method', None):
            if read_only and getattr(attr, 'write_method', False):
                attr = _make_cannot_write(attr)
            elif getattr(attr, 'phase_dependent', None):
                attr = _make_phase_dependent(storage, attr)
            setattr(storage, var_name, attr)
