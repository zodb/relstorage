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
"""TransactionControl implementations"""

from __future__ import absolute_import
from __future__ import print_function

from ..txncontrol import GenericTransactionControl


class MySQLTransactionControl(GenericTransactionControl):
    # A temporary magic variable as we move TID allocation into some
    # databases; with an external clock, we *do* need to sleep waiting for
    # TIDs to change in a manner we can exploit; that or we need to be very
    # careful about choosing pack times.
    RS_TEST_TXN_PACK_NEEDS_SLEEP = 1

    def lock_database_and_choose_next_tid(self, cursor, locker,
                                          username,
                                          description,
                                          extension):
        proc = 'lock_and_choose_tid'
        args = ()
        if self.keep_history:
            proc = proc + '(%s, %s, %s, %s)'
            args = (False, username, description, extension)

        multi_results = self.driver.callproc_multi_result(cursor, proc, args)
        tid, = multi_results[0][0]
        return tid
