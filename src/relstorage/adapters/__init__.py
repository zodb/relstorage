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
Abstractions for using particular RDBMS implementations.

The interfaces in this package define what operations RelStorage needs
to work with a particular database. Most modules then have an Abstract
(partial) implementation of one such interface. The central
``IRelStorageAdapter`` interface ties them all together.

To use a new RDBMS, create a new package named for it, containing a
module named like each module found here to store the
implementation-specific class that subclasses the abstract version
found here.

Decisions that need to be made include:

- How to allocate OIDs.
- How to handle locking (commit and pack)
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
