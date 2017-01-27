##############################################################################
#
# Copyright (c) 2017 Zope Foundation and Contributors.
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
"Internal helper utilities."

from functools import partial
from functools import update_wrapper
from functools import wraps

from relstorage._compat import intern

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
        return value

def query_property(base_name,
                   extension='',
                   formatted=False):
    """
    Defines a property that adapts to preserving or dropping history.

    To use, define a property ending in `_queries` that is a
    two-tuple, where the preserving query comes first and the dropping
    query comes second. This indirection lets subclasses override these
    queries.

    Then define a property, passing the base name (without _queries) to
    this function.

    The correct query will be lazily picked at runtime. The instance must have the
    ``keep_history`` attribute.

    If the chosen query is an exception instance, it will be raised instead
    of returned. This allows defining a query that is only supported in one of the
    two modes.

    :keyword str extension: This string will be appended to whatever query
      is chosen before it is formatted and before it is returned.
    :keyword bool formatted: If True (*not* the default), then the chosen query
      will be formatted using the ``self.runner.script_vars``.
    """

    def prop(inst):
        queries = getattr(inst, base_name + '_queries')
        query = queries[0] if inst.keep_history else queries[1]
        if isinstance(query, Exception):
            raise query

        if extension:
            query = query + extension
        if formatted:
            query = intern(query % inst.runner.script_vars)

        return query

    prop.__doc__ = "Query for " + base_name

    return Lazy(prop, base_name + '_query')

formatted_query_property = partial(query_property, formatted=True)


def noop_when_history_free(meth):
    """
    Decorator for *meth* that causes it to do nothing when
    ``self.keep_history`` is False.

    *meth* must have no return value (returns None) when it is
    history free. When history is preserved it can return anything.

    This requires a bit more memory to use the instance dict, but at
    runtime it has minimal time overhead (after the first call).
    """

    # Python 3.4 (via timeit)
    # calling a trivial method ('def t(self, arg): return arg') takes 118ns
    # calling a method that does 'if not self.keep_history: return; return arg'
    #   takes 142 ns
    # calling a functools.partial bound to self wrapped around t
    #   takes 298ns
    # calling a generic python function
    #     def wrap(self, *args, **kwargs):
    #       if not self.keep_history: return
    #       return self.t(*args, **kwargs)
    #   takes 429ns
    # So a partial function set into the __dict__ is the fastest way to
    # do this.

    meth_name = meth.__name__

    @wraps(meth)
    def no_op(*_args, **_kwargs):
        return

    @wraps(meth)
    def swizzler(self, *args, **kwargs):
        if not self.keep_history:
            setattr(self, meth_name, no_op)
        else:
            # NOTE: This creates a reference cycle
            bound = partial(meth, self)
            update_wrapper(bound, meth)
            if not hasattr(bound, '__wrapped__'):
                bound.__wrapped__ = meth
            setattr(self, meth_name, bound)

        return getattr(self, meth_name)(*args, **kwargs)

    if not hasattr(swizzler, '__wrapped__'):
        # Py2: this was added in 3.2
        swizzler.__wrapped__ = meth
        no_op.__wrapped__ = meth

    return swizzler
