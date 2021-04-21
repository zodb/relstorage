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
Implementation of load methods.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64
from ZODB.utils import maxtid

from relstorage.interfaces import POSKeyError
from relstorage.cache.interfaces import CacheConsistencyError
from .._util import metricmethod
from .._util import metricmethod_sampled
from .util import storage_method
from .util import stale_aware


logger = __import__('logging').getLogger(__name__)

def _make_pke_data(cursor, adapter, oid_int, reason):
    """
    Add extra info to the POSKeyError from load.

    KeyErrors in load() are generally not supposed to happen,
    so this is a good place to gather information.
    """
    extra = {'reason': reason}

    if adapter.keep_history:
        tid = adapter.txncontrol.get_tid(cursor)
        extra['current_txn'] = tid

        tids = []
        try:
            history = adapter.dbiter.iter_object_history(cursor, oid_int)
        except KeyError as e:
            # The object has no history, at least from the point of view
            # of the current database load connection.
            tids = str(e)
            del e
        else:
            for entry in history:
                tids.append(entry.tid_int)
                if len(tids) >= 10:
                    break
        extra['recent_tids'] = tids
    return extra

class Loader(object):

    __slots__ = (
        'adapter',
        'load_connection',
        'cache',
        '__dict__',
    )

    def __init__(self, adapter, load_connection, cache):
        self.adapter = adapter
        self.load_connection = load_connection
        self.cache = cache

    def __load_using_method(self, load_cursor, meth, argument):
        try:
            return meth(load_cursor, argument)
        except CacheConsistencyError:
            logger.exception("Cache consistency error; restarting load")
            self.load_connection.drop()
            raise

    def __pke(self, oid_bytes, **extra):
        defs = {
            'cache': self.cache,
            'load': self.load_connection,
            'adapter': self.adapter,
        }
        defs.update(extra)
        return POSKeyError(oid_bytes, defs)

    @stale_aware
    @storage_method
    @metricmethod_sampled
    def load(self, oid, version=''):
        # pylint:disable=unused-argument
        oid_int = bytes8_to_int64(oid)
        # TODO: Here, and in prefetch, should we check bool(load_connection)?
        # If it's not active and had polled, we don't really want to do that, do we?
        load_cursor = self.load_connection.cursor
        state, tid_int = self.__load_using_method(load_cursor, self.cache.load, oid_int)
        if tid_int is None:
            raise self.__pke(oid,
                             **_make_pke_data(load_cursor,
                                              self.adapter,
                                              oid_int,
                                              "no tid found"))

        if not state:
            # This can happen if something attempts to load
            # an object whose creation has been undone or which was deleted
            # by IExternalGC.deleteObject().
            raise self.__pke(oid,
                             **_make_pke_data(load_cursor,
                                              self.adapter,
                                              oid_int,
                                              "creation undone"))
        return state, int64_to_8bytes(tid_int)

    @storage_method
    def getTid(self, oid):
        """
        Return the transaction TID bytes for the OID, as
        seen from the load connection.
        """
        _state, serial = self.load(oid)
        return serial

    @stale_aware
    @storage_method
    def prefetch(self, oids):
        prefetch = self.cache.prefetch
        oid_ints = [bytes8_to_int64(oid) for oid in oids]
        load_cursor = self.load_connection.cursor
        try:
            self.__load_using_method(load_cursor, prefetch, oid_ints)
        except (AttributeError, TypeError, ValueError):
            raise
        except Exception: # pylint:disable=broad-except
            # This could raise self._stale_error, or
            # CacheConsistencyError. Both of those mean that regular loads
            # may fail too, but we don't know what our transaction state is
            # at this time, so we don't want to raise it to the caller.
            logger.exception("Failed to prefetch")

    # This is *NOT* stale aware for some reason (why?)
    @storage_method
    @metricmethod_sampled
    def loadSerial(self, oid, serial):
        """Load a specific revision of an object"""
        oid_int = bytes8_to_int64(oid)
        tid_int = bytes8_to_int64(serial)

        # If we've got this state cached exactly,
        # use it. No need to poll or anything like that first;
        # polling is unlikely to get us the state we want.
        # If the data happens to have been removed from the database,
        # due to a pack, this won't detect it if it was already cached
        # and the pack happened somewhere else. This method is
        # only used for conflict resolution, though, and we
        # shouldn't be able to get to that point if the root revision
        # went missing, right? Packing periodically takes the same locks we
        # want to take for committing.
        state = self.cache.loadSerial(oid_int, tid_int)
        if state:
            return state

        # Allow loading data from later transactions for conflict
        # resolution. There are three states involved in conflict resolution:
        # the original state (our load connection should be able to see that),
        # the committed state (our store connection can see that, and we returned it
        # when we detected the conflict) and the state we're trying to commit
        # (stored in the temporary cache data). So if we get here, mostly we
        # should need to use the load connection.

        state = self.adapter.mover.load_revision(
            self.load_connection.cursor,
            oid_int,
            tid_int)

        if state:
            return state

        # Actually using the store_connection to pull into the future was
        # removed as part of the pooling of store_connection. The above comments
        # indicate that we really shouldn't need to get here, and no tests break
        # with this commented out. What's a legitimate need for pulling into the future?
        # state = self.adapter.mover.load_revision(
        #     self.store_connection.cursor,
        #     oid_int,
        #     tid_int)
        # if state:
        #     return state

        raise self.__pke(oid, tid_int=tid_int, state=state)


    @stale_aware
    @storage_method
    @metricmethod_sampled
    def loadBefore(self, oid, tid):
        """
        Return the most recent revision of oid before tid committed.
        """
        if tid is maxtid or tid == maxtid:
            # This is probably from ZODB.utils.load_current(), which
            # is really trying to just get the current state of the
            # object. This is almost entirely just from test cases; ZODB 5's mvccadapter
            # doesn't even expose it, so ZODB.Connection doesn't use it.
            #
            # Shortcut the logic below by using load() (current),
            # formatted in the way this method returns it:
            #
            #     ``(state, tid # of state, tid_after_state)``
            #
            # where tid_after_state will naturally be None
            return self.load(oid) + (None,)
        oid_int = bytes8_to_int64(oid)

        # TODO: This makes three separate queries, and also bypasses the cache.
        # We should be able to fix at least the multiple queries.

        # In the past, we would use the store connection (only if it was already open)
        # to "allow leading dato from later transactions for conflict resolution".
        # However, this doesn't seem to be used in conflict
        # resolution. ZODB.ConflictResolution.tryToResolveConflict
        # calls loadSerial(); About the only call in ZODB to
        # loadBefore() is from BlobStorage.undo() (which
        # RelStorage does not extend). Mixing and matching calls
        # between connections using different isolation levels
        # isn't great.
        #
        # We had it as a todo for a long time to stop doing that, and
        # pooling store connections was a great time to try it.
        cursor = self.load_connection.cursor
        if not self.adapter.mover.exists(cursor, oid_int):
            raise self.__pke(oid, exists=False)

        state, start_tid = self.adapter.mover.load_before(
            cursor, oid_int, bytes8_to_int64(tid))

        if start_tid is None:
            return None

        if state is None:
            # This can happen if something attempts to load
            # an object whose creation has been undone, see load()
            # This change fixes the test in
            # TransactionalUndoStorage.checkUndoCreationBranch1
            # self._log_keyerror doesn't work here, only in certain states.
            self.__pke(oid, undone=True)

        end_int = self.adapter.mover.get_object_tid_after(
            cursor, oid_int, start_tid)
        if end_int is not None:
            end = int64_to_8bytes(end_int)
        else:
            end = None

        return state, int64_to_8bytes(start_tid), end


class BlobLoader(object):

    __slots__ = (
        'load_connection',
        'blobhelper',
    )

    def __init__(self, load_connection, blobhelper):
        self.load_connection = load_connection
        self.blobhelper = blobhelper

    @storage_method
    @metricmethod
    def loadBlob(self, oid, serial):
        """Return the filename of the Blob data for this OID and serial.

        Returns a filename.

        Raises POSKeyError if the blobfile cannot be found.
        """
        cursor = self.load_connection.cursor
        return self.blobhelper.loadBlob(cursor, oid, serial)

    @storage_method
    @metricmethod
    def openCommittedBlobFile(self, oid, serial, blob=None):
        """
        Return a file for committed data for the given object id and serial

        If a blob is provided, then a BlobFile object is returned,
        otherwise, an ordinary file is returned. In either case, the
        file is opened for binary reading.

        This method is used to allow storages that cache blob data to
        make sure that data are available at least long enough for the
        file to be opened.
        """
        cursor = self.load_connection.cursor
        return self.blobhelper.openCommittedBlobFile(
            cursor, oid, serial, blob=blob)
