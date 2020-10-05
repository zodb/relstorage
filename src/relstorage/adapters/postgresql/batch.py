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
Batch table row insert/delete support.
"""

from __future__ import absolute_import


from ..batch import RowBatcher

class PostgreSQLRowBatcher(RowBatcher):
    """
    Applies array operations to DELETE and SELECT
    for single column filters.
    """

    def _make_single_column_query(self, command, table,
                                  filter_column, filter_value,
                                  rows_need_flattened):
        if not command.startswith("UPDATE"):
            stmt = "%s FROM %s WHERE %s = ANY (%s)" % (
                command, table, filter_column, self.delete_placeholder
            )
        else:
            stmt = '%s WHERE %s = ANY (%s)' % (
                command, filter_column, self.delete_placeholder
            )
        # We only pass a single parameter, and it doesn't need further
        # flattening.
        # It does have to be an actual list, though
        params = filter_value
        if rows_need_flattened:
            # This is guaranteed to return a list
            params = self._flatten_params(params)
        elif not isinstance(params, list):
            params = list(params)
        return stmt, (params,), False
