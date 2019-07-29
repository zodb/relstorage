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
"""Stats implementations
"""

import abc

from .._compat import ABC
from ._util import query_property
from ._util import DatabaseHelpersMixin


class AbstractStats(DatabaseHelpersMixin, ABC):

    def __init__(self, connmanager, keep_history):
        self.connmanager = connmanager
        self.keep_history = keep_history

    _get_object_count_queries = (
        "SELECT COUNT(*) FROM current_object",
        "SELECT COUNT(*) FROM object_state"
    )

    _get_object_count_query = query_property('_get_object_count')

    def get_object_count(self):
        """Returns the approximate number of objects in the database"""
        conn, cursor = self.connmanager.open()
        try:
            cursor.execute(self._get_object_count_query)
            return cursor.fetchone()[0]
        finally:
            self.connmanager.close(conn, cursor)

    @abc.abstractmethod
    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        raise NotImplementedError()


    def large_database_change(self):
        """
        Call this when the database has changed substantially,
        and it would be a good time to perform any updates or
        optimizations.
        """
