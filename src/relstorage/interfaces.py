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
Interfaces for top-level RelStorage components.

These interfaces aren't meant to be considered public, they exist to
serve as documentation and for validation of RelStorage internals.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import Interface
from zope.interface import Attribute

import ZODB.interfaces

# pylint:disable=inherit-non-class, no-self-argument, no-method-argument
# pylint:disable=too-many-ancestors
try:
    from zope.schema import Tuple
    from zope.schema import Object
    from zope.schema import Bool
    from zope.schema import Int
    from zope.interface.common.interfaces import IException
except ImportError: # pragma: no cover
    # We have nti.testing -> zope.schema as a test dependency; but we
    # don't have it as a hard-coded runtime dependency because we
    # don't want to force a version on consumers of RelStorage.
    class _Field(Attribute):
        __allowed_kw__ = ()
        def __init__(self, description, required=False, **kwargs):
            description = "%s (required? %s)" % (description, required)
            for k in self.__allowed_kw__:
                kwargs.pop(k, None)
            if kwargs:
                raise TypeError("Unexpected keyword arguments: %r" % (kwargs,))
            Attribute.__init__(self, description)

    class Tuple(_Field):
        __allowed_kw__ = ('value_type', )

    class Object(_Field):
        def __init__(self, schema, description=''):
            description = "%s (Must provide %s)" % (description, schema)
            super(Object, self).__init__(description)

    Bool = _Field
    Int = _Field

    class Factory(_Field):
        def __init__(self, schema, description='', **kwargs):
            description = "%s (Must implement %s)" % (description, schema)
            super(Factory, self).__init__(description, **kwargs)


    IException = Interface
else:
    from zope.schema.interfaces import SchemaNotProvided as _SchemaNotProvided
    from zope.schema import Field as _Field

    class Factory(_Field):
        def __init__(self, schema, **kw):
            self.schema = schema
            _Field.__init__(self, **kw)

        def _validate(self, value):
            super(Factory, self)._validate(value)
            if not self.schema.implementedBy(value):
                raise _SchemaNotProvided(self.schema, value).with_field_and_value(self, value)

__all__ = [
    'Tuple',
    'Object',
    'Bool',
    'TID',
    'OID',
    'IException',
    'Factory',
    'IMVCCDatabaseCoordinator',
    'IMVCCDatabaseViewer'
]


class OID(Int):
    """
    A ZODB object identifier, represented as a 64-bit integer.
    """

class TID(Int):
    """
    A ZODB transaction identifier, represented as a 64-bit integer.

    Traditionally, ZODB TIDs are created and derived using
    `persistent.timestamp.TimeStamp`, which is a reference to the current
    `time.time` value.
    """

###
# Efficiently handling multiple views of a database
# within a process.
###

class IMVCCDatabaseViewer(Interface):
    """
    A component that has a consistent, point-in-time view of a
    database.

    This is implemented using an RDBMS connection (session) with
    ``REPEATABLE READ`` or higher isolation level.

    In the context of ZODB, this means that this view contains all the
    data for a particular transaction identifier (TID, also "revision"
    or "revid"; the contents of a particular persistent object's
    ``_p_serial``) and the transactions that come before it (lower
    TIDs), but not any newer (higher numbered) transactions that may
    exist.

    The highest available TID is updated between transactions.
    """

    highest_visible_tid = TID(
        description=(
            u"""
            The identifier of the most recent transaction viewable to
            this component. A value of ``None`` means that we have no
            idea what transactions even exist.
            """),
        required=False)


class IMVCCDatabaseCoordinator(Interface):
    """
    A component that handles tracking multiple `IMVCCDatabaseViewer`
    components fulfilling the same role.

    These components would be created by calling
    :meth:`ZODB.interfaces.IMVCCStorage.new_instance` on the
    `relstorage.interfaces.IRelStorage` owned by the
    `ZODB.interfaces.IDatabase` object. There will be one for each
    `ZODB.interfaces.IConnection` object in a pool.

    By tracking all existing components for a database within the same
    process, we can know what the maximum and minimum visible TIDs are
    within the process. When the minimum visible TID is incremented,
    we have an opportunity to take actions such as freeing data no
    longer needed (because it has been updated in a subsequent
    transaction and we now know the old states aren't visible to any
    current connections.)
    """

    def register(viewer):
        """
        Register the *viewer* to be tracked by this object.

        A matching call to :meth:`unregister` is expected.
        """

    def unregister(viewer):
        """
        Stop tracking the *viewer*.
        """

    maximum_highest_visible_tid = TID(
        description=(
            u"""
             Across all tracked components, report the current highest
             visible tid. This is the most recent transaction that can
             be seen in this process.
             """),
        required=False)

    minimum_highest_visible_tid = TID(
        description=(
            u"""
             Across all tracked components, report the current minimum
             highest visible tid. This is the oldest transaction being
             viewed in this process.
             """),
        required=False)



class IRelStorage(
        ZODB.interfaces.IMVCCAfterCompletionStorage, # IMVCCStorage <- IStorage
        ZODB.interfaces.IMultiCommitStorage,  # mandatory in ZODB5, returns tid from tpc_finish.
        ZODB.interfaces.IStorageRestoreable,  # tpc_begin(tid=) and restore()
        ZODB.interfaces.IStorageIteration,    # iterator()
        ZODB.interfaces.ReadVerifyingStorage, # checkCurrentSerialInTransaction()
        IMVCCDatabaseViewer,
):
    """
    The relational storage backend.

    These objects are not thread-safe.

    Instances may optionally implement some other interfaces,
    depending on their configuration. These include:

    - :class:`ZODB.interfaces.IBlobStorage` and :class:`ZODB.interfaces.IBlobStorage`
      if a ``blob-dir`` is configured.
    - :class:`ZODB.interfaces.IStorageUndoable` if ``keep-history`` is true.

    """
