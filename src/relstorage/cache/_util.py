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
Helpers.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

class InvalidationMixin(object):

    def _invalidate(self, oid_int, tid_int):
        # TODO: Perhaps this should invalidate anything <= the tid?
        if self.delta_after0.get(oid_int) == tid_int:
            del self.delta_after0[oid_int]

    def _invalidate_all(self, oids):
        pop0 = self.delta_after0.pop
        pop1 = self.delta_after1.pop
        for oid in oids:
            pop0(oid, None)
            pop1(oid, None)
