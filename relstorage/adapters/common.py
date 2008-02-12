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
"""Code common to most adapters."""

import logging

log = logging.getLogger("relstorage.adapters.common")


class Adapter(object):

    def _run_script(self, cursor, script, params=()):
        """Execute a series of statements in the database."""
        lines = []
        for line in script.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            if line.endswith(';'):
                line = line[:-1]
                lines.append(line)
                stmt = '\n'.join(lines)
                try:
                    cursor.execute(stmt, params)
                except:
                    log.warning("script statement failed: %s", stmt)
                    raise
                lines = []
            else:
                lines.append(line)
        if lines:
            try:
                stmt = '\n'.join(lines)
                cursor.execute(stmt, params)
            except:
                log.warning("script statement failed: %s", stmt)
                raise

