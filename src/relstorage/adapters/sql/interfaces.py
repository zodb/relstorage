# -*- coding: utf-8 -*-
"""
Interfaces, mostly internal, for the sql module.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint:disable=inherit-non-class,no-method-argument

from zope.interface import Interface

class ITypedParams(Interface):
    """
    Something that accepts parameters, and knows
    what their types should be.
    """

    def datatypes_for_parameters():
        """
        Returns a sequence of datatypes.

        XXX: This only works for ordered params; make this work
        for named parameters. Probably want to treat the two the same,
        with ordered parameters using indexes as their name.
        """


class IBindParam(Interface):
    """
    A parameter to a query.
    """

class INamedBindParam(IBindParam):
    """
    A named parameter.
    """

class IOrderedBindParam(IBindParam):
    """
    A anonymous parameter, identified only by order.
    """
