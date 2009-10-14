##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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

from cStringIO import StringIO
import tempfile

class AutoTemporaryFile(object):
    """Initially a StringIO, but becomes a TemporaryFile if it grows large.

    Not thread safe.
    """

    def __init__(self, threshold=10*1024*1024):
        self._threshold = threshold
        self._f = StringIO()

    def read(self, n=None):
        if n is not None:
            return self._f.read(n)
        else:
            return self._f.read()

    def seek(self, pos, mode=0):
        self._f.seek(pos, mode)

    def tell(self):
        return self._f.tell()

    def close(self):
        self._f.close()

    def write(self, data):
        threshold = self._threshold
        if threshold and self._f.tell() + len(data) >= threshold:
            # convert to TemporaryFile
            self._threshold = 0
            f = tempfile.TemporaryFile()
            f.write(self._f.getvalue())
            f.seek(self._f.tell())
            self._f = f
        self._f.write(data)
