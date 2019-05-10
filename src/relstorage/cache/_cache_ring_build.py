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
from __future__ import absolute_import, print_function, division

import os
from cffi import FFI

this_dir = os.path.dirname(os.path.abspath(__file__))

ffi = FFI()
with open(os.path.join(this_dir, 'cache_ring.h')) as f:
    ffi.cdef(f.read())

ffi.set_source('relstorage.cache._cache_ring',
               '#include "cache_ring.c"',
               include_dirs=[this_dir])

if __name__ == '__main__':
    ffi.compile()
