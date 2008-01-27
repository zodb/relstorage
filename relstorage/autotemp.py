##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""A temporary file that switches from StringIO to TemporaryFile if needed.

This could be a useful addition to Python's tempfile module.
"""

from cStringIO import StringIO
import tempfile

class AutoTemporaryFile:
    """Initially a StringIO, but becomes a TemporaryFile if it grows too big"""
    def __init__(self, threshold=10485760):
        self._threshold = threshold
        self._f = f = StringIO()
        # delegate most methods
        for m in ('read', 'readline', 'seek', 'tell', 'close'):
            setattr(self, m, getattr(f, m))

    def write(self, data):
        threshold = self._threshold
        if threshold > 0 and self.tell() + len(data) >= threshold:
            # convert to TemporaryFile
            f = tempfile.TemporaryFile()
            f.write(self._f.getvalue())
            f.seek(self.tell())
            self._f = f
            self._threshold = 0
            # delegate all important methods
            for m in ('write', 'read', 'readline', 'seek', 'tell', 'close'):
                setattr(self, m, getattr(f, m))
        self._f.write(data)
