# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
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
Segmented LRU implementations.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import itertools

from zope import interface

from .interfaces import IGenerationalLRUCache
from .interfaces import IGeneration
from .interfaces import ILRUEntry
from .interfaces import NoSuchGeneration

from . import cache
from . import cache_values

try:
    izip = itertools.izip
except AttributeError:
    # Python 3
    izip = zip

interface.classImplements(cache.PyCache, IGenerationalLRUCache)
interface.classImplements(cache.PyGeneration, IGeneration)
interface.classImplements(cache_values.SingleValue, ILRUEntry)
interface.classImplements(cache_values.MultipleValues, ILRUEntry)


class Cache(cache.PyCache):
    # Percentage of our byte limit that should be dedicated
    # to the main "protected" generation
    _gen_protected_pct = 0.8
    # Percentage of our byte limit that should be dedicated
    # to the initial "eden" generation
    _gen_eden_pct = 0.1
    # Percentage of our byte limit that should be dedicated
    # to the "probationary"generation
    _gen_probation_pct = 0.1
    # By default these numbers add up to 1.0, but it would be possible to
    # overcommit by making them sum to more than 1.0. (For very small
    # limits, the rounding will also make them overcommit).

    __slots__ = ()

    def __new__(cls, byte_limit):
        return super(Cache, cls).__new__(
            cls,
            byte_limit * cls._gen_eden_pct,
            byte_limit * cls._gen_protected_pct,
            byte_limit * cls._gen_probation_pct
        )


    @property
    def generations(self):
        return [NoSuchGeneration(0),
                self.eden,
                self.protected,
                self.probation,]

    # mapping operations, operating on user-level key/value pairs.

    @property
    def size(self): # Also, rename to weight to be consistent.
        return self.weight


    def stats(self):
        return {
        }



# BWC
CFFICache = Cache
