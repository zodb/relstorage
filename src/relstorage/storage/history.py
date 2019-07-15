# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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
Support for methods that require access to
an object's history. These only do useful things
when the database is history preserving.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from persistent.timestamp import TimeStamp

from ZODB.UndoLogCompatible import UndoLogCompatible
from ZODB.POSException import POSKeyError
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage._compat import base64_encodebytes
from relstorage._compat import loads

from .util import phase_dependent
from .util import storage_method
from .util import stale_aware

class History(object):
    """
    Provides the implementation of ``history``
    defined in :class:`ZODB.interfaces.IStorage`.

    This is available for all database types.
    """

    __slots__ = (
        'adapter',
        'load_connection',
    )

    def __init__(self, adapter, load_connection):
        self.adapter = adapter
        self.load_connection = load_connection

    @stale_aware
    @storage_method
    def history(self, oid, version=None, size=1, filter=None):
        # pylint:disable=unused-argument,too-many-locals
        cursor = self.load_connection.cursor
        oid_int = bytes8_to_int64(oid)
        try:
            rows = self.adapter.dbiter.iter_object_history(
                cursor, oid_int)
        except KeyError:
            raise POSKeyError(oid)

        res = []
        for tid_int, username, description, extension, length in rows:
            tid = int64_to_8bytes(tid_int)
            if extension:
                d = loads(extension)
            else:
                d = {}
            d.update({
                "time": TimeStamp(tid).timeTime(),
                "user_name": username or b'',
                "description": description or b'',
                "tid": tid,
                "version": '',
                "size": length,
                "rs_tid_int": tid_int,
                "rs_oid_int": oid_int,
            })
            if filter is None or filter(d):
                res.append(d)
                if size is not None and len(res) >= size:
                    break
        return res


class UndoableHistory(History):
    """
    Provides the implementation of the unique methods
    defined in :class:`ZODB.interfaces.IStorageUndoable`.
    """

    __slots__ = ()

    @storage_method
    def undoInfo(self, *args, **kwargs):
        # UndoLogCompatible provides the
        # implementation of undoInfo using self.undoLog
        log = UndoLogCompatible()
        log.undoLog = self.undoLog
        return log.undoInfo(*args, **kwargs)

    @storage_method
    def supportsUndo(self):
        return self.adapter.keep_history

    supportsTransactionalUndo = supportsUndo

    @stale_aware
    @storage_method
    def undoLog(self, first=0, last=-20, filter=None):
        # pylint:disable=too-many-locals
        if last < 0:
            last = first - last

        # use a private connection to ensure the most current results
        adapter = self.adapter
        conn, cursor = adapter.connmanager.open()
        try:
            rows = adapter.dbiter.iter_transactions(cursor)
            i = 0
            res = []
            for tid_int, user, desc, ext in rows:
                tid = int64_to_8bytes(tid_int)
                # Note that user and desc are schizophrenic. The transaction
                # interface specifies that they are a Python str, *probably*
                # meaning bytes. But code in the wild and the ZODB test suite
                # sets them as native strings, meaning unicode on Py3. OTOH, the
                # test suite checks that this method *returns* them as bytes!
                # This is largely cleaned up with transaction 2.0/ZODB 5, where the storage
                # interface is defined in terms of bytes only.
                d = {
                    'id': base64_encodebytes(tid)[:-1],  # pylint:disable=deprecated-method
                    'time': TimeStamp(tid).timeTime(),
                    'user_name':  user or b'',
                    'description': desc or b'',
                }
                if ext:
                    d.update(loads(ext))

                if filter is None or filter(d):
                    if i >= first:
                        res.append(d)
                    i += 1
                    if i >= last:
                        break
            return res

        finally:
            adapter.connmanager.close(conn, cursor)

    @phase_dependent
    @storage_method
    def undo(self, tpc_phase, transaction_id, transaction):
        """
        Undo a transaction identified by transaction_id.

        transaction_id is the base 64 encoding of an 8 byte tid. Undo
        by writing new data that reverses the action taken by the
        transaction.
        """
        # This is called directly from code in DB.py on a new instance
        # (created either by new_instance() or a special
        # undo_instance()). That new instance is never asked to load
        # anything, or poll invalidations, so our storage cache is ineffective
        # (unless we had loaded persistent state files)
        #
        # TODO: Implement 'undo_instance' to make this clear.
        #
        # A regular Connection going through two-phase commit will
        # call tpc_begin(), do a bunch of store() from its commit(),
        # then tpc_vote(), tpc_finish().
        #
        # During undo, we get a tpc_begin(), then a bunch of undo() from
        # ZODB.DB.TransactionalUndo.commit(), then tpc_vote() and tpc_finish().
        tpc_phase.undo(transaction_id, transaction)
