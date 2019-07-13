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

def _make_state_dependent(storage, method):
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
                attr = _make_state_dependent(storage, attr)
            setattr(storage, var_name, attr)
