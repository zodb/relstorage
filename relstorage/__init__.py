##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""relstorage package"""

from ZODB.Connection import Connection

if not hasattr(Connection, 'new_oid'):
    # Monkey patch to make new_oid use the connection's
    # storage. This has to be a data descriptor because
    # connection will try to reset this.
    # See https://github.com/zopefoundation/ZODB/issues/139
    # Merged in https://github.com/zopefoundation/ZODB/pull/140,
    # expected release in 5.1.2

    class NewOid(object):

        __name__ = 'new_oid'

        def __get__(self, inst, klass=None):
            if klass is None:
                return self
            return inst._storage.new_oid

        def __set__(self, inst, value):
            pass

    Connection.new_oid = NewOid()
