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

from tempfile import SpooledTemporaryFile

class AutoTemporaryFile(SpooledTemporaryFile):
    # Exists for BWC and to preserve the default threshold

    def __init__(self, threshold=10 * 1024 * 1024, **kw):
        # STF uses >, the old ATF used >= for the max_size check
        SpooledTemporaryFile.__init__(self, max_size=threshold - 1, **kw)

    @property
    def _threshold(self):
        return self._max_size + 1 if not self._rolled else 0
