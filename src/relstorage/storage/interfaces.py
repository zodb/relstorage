# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
"""
Interfaces for RelStorage implementation components.

These interfaces aren't meant to be considered public, they exist to
serve as documentation and for validation of RelStorage internals.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import Interface

# pylint:disable=inherit-non-class,no-self-argument,no-method-argument

class IStaleAware(Interface):
    """
    An object that can transition between a state of normalcy and being stale.

    In the stale state, calling the object raises an exception.
    """

    def stale(stale_error):
        """
        Produce a new object that is the stale version of this object
        and return it.

        :return: Another `IStaleAware`.
        """

    def no_longer_stale():
        """
        Produce a new (or the original) object that is in the normal state
        and return it.

        :return: Another `IStaleAware`.
        """