# -*- coding: utf-8 -*-
"""
Compatibility shims.

"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

import sys
PY3 = sys.version_info[0] == 3

# Dict support

if PY3:
    def list_keys(d):
        return list(d.keys())
    def list_items(d):
        return list(d.items())
    def list_values(d):
        return list(d.values())
    iteritems = dict.items
    iterkeys = dict.keys
else:
    list_keys = dict.keys
    list_items = dict.items
    list_values = dict.values
    iteritems = dict.iteritems
    iterkeys = dict.iterkeys
