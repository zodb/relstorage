# -*- coding: utf-8 -*-
"""
Supporting code.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import partial

from zope.interface import implementer

from relstorage._compat import wraps

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

    These are methods that can only be called after ``tpc_begin``
    and delegate to the ``tpc_phase`` object.
    """
    meth.phase_dependent = True
    return meth

def phase_dependent_aborts_early(meth):
    """
    Like :func:`phase_dependent`, but if any exception
    were to propagate out of the body of the method,
    the TPC will be aborted  immediately.
    """
    meth = phase_dependent(meth)
    meth.aborts_early = True
    return meth

class _StaleMethodWrapperType(type):
    # Make the _StaleMethodWrapper class also a descriptor
    # so that when we put it in the class dictionary, it
    # gets a chance to bind to the instance.
    def __get__(cls, inst, klass):
        return partial(cls, inst)

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
        # method's __call__.  For this reason, it's important to access
        # these methods only once and cache them (which is what copy_storage_methods
        # does).

        # Unfortunately, when a method from inside the defining class
        # calls another method decorated like this, we derive a new
        # type each time. e.g., ``Loader.loadBefore`` calls
        # ``self.load`` and generates a new type. Therefore we must
        # also cache this on the instance. This creates a reference
        # cycle.

        # Many of the instances that use this actually define __slots__
        # so we can't put arbitrary stuff in their missing __dict__ anyway,
        # so they must also define __dict__.

        stale_aware_class = type(
            'StaleAware_' + bound.__name__,
            (_StaleAwareMethodTemplate,),
            {
                '__slots__': (),
                '__call__': bound.__call__,
                '__name__': bound.__name__,
                '__module__': bound.__module__,
            }
        )

        stale_aware_class = implementer(IStaleAware)(stale_aware_class)
        # Defining
        meth = stale_aware_class(bound)
        inst.__dict__[bound.__name__] = meth
        return meth


def _make_phase_dependent(storage, method):
    aborts_early = getattr(method, 'aborts_early', False)
    if aborts_early:
        @wraps(method)
        def state(*args, **kwargs):
            try:
                return method(storage._tpc_phase, *args, **kwargs)
            except:
                storage.tpc_abort(None, _force=True)
                raise
    else:
        @wraps(method)
        def state(*args, **kwargs):
            return method(storage._tpc_phase, *args, **kwargs)
    return state

def make_cannot_write(storage, bound_method):
    @wraps(bound_method)
    def read_only(*args, **kwargs):
        raise storage._read_only_error
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
            storage_meth = attr

            method_writes_to_db = getattr(storage_meth, 'write_method', False)
            method_is_phase_dependent = getattr(storage_meth, 'phase_dependent', False)

            if read_only and method_writes_to_db:
                storage_meth = make_cannot_write(storage, storage_meth)
            elif method_is_phase_dependent:
                storage_meth = _make_phase_dependent(storage, storage_meth)
            setattr(storage, var_name, storage_meth)
