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
"""The core of RelStorage, a ZODB storage for relational databases.

Stores pickles in the database.
"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import sys
import weakref


import ZODB.interfaces

from perfmetrics import metricmethod

from ZODB import ConflictResolution

from ZODB.mvccadapter import HistoricalStorageAdapter

from ZODB.POSException import ReadConflictError

from ZODB.utils import z64
from zope import interface
from zope.interface import implementer

from ..blobhelper import BlobHelper
from ..blobhelper.interfaces import IBlobHelper
from ..blobhelper.interfaces import INoBlobHelper

from ..cache import StorageCache
from ..options import Options
from ..interfaces import IRelStorage
from ..adapters.connections import LoadConnection
from ..adapters.connections import StoreConnection
from ..adapters.connections import ClosedConnection
from .._compat import clear_frames
from .._util import int64_to_8bytes
from .._util import bytes8_to_int64


from .transaction_iterator import TransactionIterator

from .copy import Copy
from .history import History
from .history import UndoableHistory
from .legacy import LegacyMethodsMixin
from .load import Loader
from .load import BlobLoader
from .oid import OIDs
from .oid import ReadOnlyOIDs
from .pack import Pack
from .store import Storer
from .store import BlobStorer

from .tpc import NotInTransaction
from .tpc.begin import HistoryFree
from .tpc.begin import HistoryPreserving
from .tpc.restore import Restore

from .util import copy_storage_methods
from .interfaces import StorageDisconnectedDuringCommit

__all__ = [
    'RelStorage',
]

log = logging.getLogger("relstorage")


class _ClosedCache(object):
    __slots__ = ()

    def close(self):
        "does nothing"

    release = close

@implementer(IRelStorage)
class RelStorage(LegacyMethodsMixin,
                 ConflictResolution.ConflictResolvingStorage):

    """
    Storage to a relational database, based on invalidation polling.
    """

    # pylint:disable=too-many-instance-attributes,too-many-public-methods

    _adapter = None
    _options = None
    _is_read_only = False
    # _ltid is the ID of the last transaction committed by this instance.
    _ltid = z64

    # _closed is True after self.close() is called.
    _closed = False

    # _cache if set is a StorageCache object.
    _cache = None

    # _prev_polled_tid contains the tid at the previous poll
    _prev_polled_tid = None

    # If the blob directory is set, blobhelper is a BlobHelper.
    blobhelper = BlobHelper(None, None)

    # The state of committing that were in. Certain operations are
    # only available in certain states; certain information is only needed
    # in certain states.
    _tpc_phase = None

    __name__ = None

    # _instances is a list of weak references to storage instances bound
    # to the same database.
    _instances = ()

    _load_connection = None
    _store_connection = None
    _tpc_begin_factory = None

    _oids = ReadOnlyOIDs()

    # Attributes in our dictionary that shouldn't have stale()/no_longer_stale()
    # called on them. At this writing, it's just the type object.
    _STALE_IGNORED_ATTRS = (
        '_tpc_begin_factory',
    )

    def __init__(self, adapter, name=None, create=None,
                 options=None, cache=None, blobhelper=None,
                 **kwoptions):
        # pylint:disable=too-many-branches, too-many-statements
        if options and kwoptions:
            raise TypeError("The RelStorage constructor accepts either "
                            "an options parameter or keyword arguments, not both")

        self._adapter = adapter

        if options is None:
            options = Options(**kwoptions)
        self._options = options

        if not name:
            name = options.name
            if not name:
                name = 'RelStorage: %s' % adapter
        self.__name__ = name

        self._is_read_only = options.read_only

        need_check_compat = create is None
        if create is None:
            create = options.create_schema

        if create:
            self._adapter.schema.prepare()
        elif need_check_compat:
            # At the top level, not new_instance(), and not asked to create.
            self._adapter.schema.verify()


        # A ZODB Connection is documented as not being thread-safe and
        # must be used only by a single thread at a time. In IMVCC,
        # each Connection has a unique storage object. So the storage
        # object is used only by a single thread. So there's really
        # not much point in RelStorage keeping thread locks around its
        # methods.

        # There are a small handful of ZODB tests that assume the
        # storage is thread safe (but they know nothing about IMVCC);
        # for those tests we use a custom wrapper storage that makes
        # a single instance appear thread safe.

        self._instances = []

        self._load_connection = LoadConnection(self._adapter.connmanager)
        # This creates a reference cycle that won't go away until we release()
        # or close()
        self._load_connection.on_first_use = self.__on_load_first_use
        self._store_connection = StoreConnection(self._adapter.connmanager)

        if cache is not None:
            self._cache = cache
        else:
            prefix = options.cache_prefix
            if not prefix:
                # Use the database name as the cache prefix.
                with self._load_connection.isolated_connection() as cur:
                    prefix = adapter.schema.get_database_name(cur)

                prefix = prefix.replace(' ', '_')
                options.cache_prefix = prefix
            self._cache = StorageCache(adapter, options, prefix)

        # Creating the storage cache may have loaded cache files, and if so,
        # we have a previous tid state.
        if self._cache.current_tid is not None:
            self._prev_polled_tid = self._cache.current_tid

        if blobhelper is not None:
            self.blobhelper = blobhelper
        else:
            self.blobhelper = BlobHelper(options=options, adapter=adapter)

        self._tpc_begin_factory = HistoryPreserving if self._options.keep_history else HistoryFree

        if hasattr(self._adapter.packundo, 'deleteObject'):
            interface.alsoProvides(self, ZODB.interfaces.IExternalGC)

        self._tpc_phase = NotInTransaction.from_storage(self)
        if not self._is_read_only:
            self._oids = OIDs(self._adapter.oidallocator, self._store_connection)

        # Now copy in a bunch of methods from our component objects.
        # Many of these are 'stale_aware', meaning that we can ask
        # them for a version of themselves that does something
        # different when we go stale. When that happens, we *replace*
        # the object in our __dict__ with the new one; and then
        # reverse that when we're no longer stale.
        #
        # The storage wrapper zc.zlibstorage also copies methods into
        # itself when it is created, from the storage it is wrapping.
        # Because of this, stale aware methods like history() do not
        # do the right thing when we're wrapped by zc.zlibstorage.
        loader = Loader(self._adapter, self._load_connection, self._store_connection, self._cache)
        copy_storage_methods(self, loader)
        storer = Storer()
        copy_storage_methods(self, storer)

        if options.keep_history:
            interface.alsoProvides(self, ZODB.interfaces.IStorageUndoable)
            history = UndoableHistory(self._adapter, self._load_connection)
        else:
            history = History(self._adapter, self._load_connection)
        copy_storage_methods(self, history)

        assert IBlobHelper.providedBy(self.blobhelper)
        if not INoBlobHelper.providedBy(self.blobhelper):
            interface.alsoProvides(self, ZODB.interfaces.IBlobStorageRestoreable)

            loader = BlobLoader(self._load_connection, self.blobhelper)
            copy_storage_methods(self, loader)

            storer = BlobStorer(self.blobhelper, self._store_connection)
            copy_storage_methods(self, storer)

    def __repr__(self):
        return "<%s at %x keep_history=%s phase=%r cache=%r>" % (
            self.__class__.__name__,
            id(self),
            self._options.keep_history,
            self._tpc_phase,
            self._cache
        )

    def new_instance(self):
        """Creates and returns another storage instance.

        See ZODB.interfaces.IMVCCStorage.
        """
        adapter = self._adapter.new_instance()
        cache = self._cache.new_instance()
        blobhelper = self.blobhelper.new_instance(adapter=adapter)
        other = type(self)(adapter=adapter, name=self.__name__,
                           create=False, options=self._options, cache=cache,
                           blobhelper=blobhelper)
        # NOTE: We're depending on the GIL (or list implementation)
        # for thread safety here.
        self._instances.append(weakref.ref(other, self._instances.remove))

        if self._crs_transform_record_data is not type(self)._crs_transform_record_data:
            # registerDB has been called on us but isn't called on
            # our children. Make sure any wrapper that needs to transform
            # records can do so.
            # See https://github.com/zodb/relstorage/issues/71
            other._crs_transform_record_data = self._crs_transform_record_data
            other._crs_untransform_record_data = self._crs_untransform_record_data
        return other

    def before_instance(self, before):
        # Implement this method of MVCCAdapterInstance
        # (possibly destined for IMVCCStorage) as a small optimization
        # in ZODB5 that can eventually simplify ZODB.Connection.Connection
        # XXX: 5.0a2 doesn't forward the release method, so we leak
        # open connections.
        i = self.new_instance()
        x = HistoricalStorageAdapter(i, before)
        x.release = i.release
        return x

    def zap_all(self, **kwargs):
        """Clear all objects and transactions out of the database.

        Used by the test suite and the ZODBConvert script.
        """
        self._adapter.schema.zap_all(**kwargs)
        self._load_connection.drop()
        self._store_connection.drop()
        self._cache.zap_all()

    def release(self):
        """
        Release external resources used by this storage instance.

        This includes the database sessions (connections) and any memcache
        connections.

        Does *not* affect any other instances created by this instance. This object
        should still be :meth:`close` (but note that might have global affects
        on other instances of the same base object).
        """
        self._load_connection.drop()
        self._store_connection.drop()

        self._cache.release()
        self._cache = _ClosedCache()
        self._tpc_phase = None
        self._oids = None
        self._load_connection = ClosedConnection()
        self._store_connection = ClosedConnection()

    def close(self):
        """Close the storage and all instances."""
        if self._closed:
            return

        self._closed = True
        self._load_connection.drop()
        self._store_connection.drop()
        self._load_connection = ClosedConnection()
        self._store_connection = ClosedConnection()

        self.blobhelper.close()
        for wref in self._instances:
            instance = wref()
            if instance is not None:
                instance.close()
        self._instances = ()

        self._cache.close()
        self._cache = _ClosedCache()

        self._tpc_phase = None
        self._oids = None

    def __len__(self):
        return self._adapter.stats.get_object_count()

    def sortKey(self):
        """Return a string that can be used to sort storage instances.

        The key must uniquely identify a storage and must be the same
        across multiple instantiations of the same storage.
        """
        return self.__name__

    def getName(self):
        return self.__name__

    def getSize(self):
        """Return database size in bytes"""
        return self._adapter.stats.get_db_size()

    def registerDB(self, wrapper):
        if (ZODB.interfaces.IStorageWrapper.providedBy(wrapper)
                # Prior to ZODB 5, this would be called by the database itself.
                # (I wish it would still do that.)
                and not ZODB.interfaces.IDatabase.providedBy(wrapper)
                and not hasattr(type(wrapper), 'new_instance')):
            # Fixes for https://github.com/zopefoundation/zc.zlibstorage/issues/2
            # We special-case zlibstorage for speed
            if hasattr(wrapper, 'base') and hasattr(wrapper, 'copied_methods'):
                type(wrapper).new_instance = _zlibstorage_new_instance
                # NOTE that zlibstorage has a custom copyTransactionsFrom that overrides
                # our own implementation.
            else:
                wrapper.new_instance = lambda s: type(wrapper)(self.new_instance())

        # Prior to ZODB 4.3.1, ConflictResolvingStorage would raise an AttributeError
        super(RelStorage, self).registerDB(wrapper)

    def isReadOnly(self):
        return self._is_read_only

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self._tpc_phase.checkCurrentSerialInTransaction(oid, serial, transaction)

    # For the time between tpc_begin and tpc_abort, if anything we do
    # would trigger an exception to propagate up (store(), tpc_vote(),
    # whatever) we should immediately abort the transaction at the
    # database level and release locks. This will allow other
    # transactions to make progress faster. It also can help in the ZODB test suite,
    # since some places manually call the tpc_* methods and aren't careful to
    # tpc_abort() in the event of an exception like `transaction` is.
    #
    # Some of this is manual (in the tpc_* methods), but most of it is
    # done through the @phase_dependent_aborts_early decorator.

    @metricmethod
    def tpc_begin(self, transaction, tid=None, status=' '):
        try:
            self._tpc_phase = self._tpc_phase.tpc_begin(transaction, self._tpc_begin_factory)
        except:
            # Could be a database (connection) error, could be a programming
            # bug. Either way, we're fine to roll everything back and hope
            # for the best on a retry. Perhaps we need to raise a TransientError?
            self._load_connection.drop()
            self._store_connection.drop()
            raise

        if tid is not None:
            # tid is a committed transaction we will restore.
            # The allowed actions are carefully prescribed.
            # This argument is specified by IStorageRestoreable
            try:
                self._tpc_phase = Restore(self._tpc_phase, tid, status)
            except:
                self.tpc_abort(transaction, _force=True)
                raise

    @metricmethod
    def tpc_vote(self, transaction):
        # Returns an iterable of OIDs; the storage will ghost all
        # cached objects in that list. This is invalidation because
        # the object has changed during the commit process, due to
        # conflict resolution or undo.
        try:
            next_phase = self._tpc_phase.tpc_vote(transaction, self)
        except:
            self.tpc_abort(transaction, _force=True)
            raise
        else:
            self._tpc_phase = next_phase
            return self._tpc_phase.invalidated_oids

    @metricmethod
    def tpc_finish(self, transaction, f=None):
        try:
            next_phase, committed_tid = self._tpc_phase.tpc_finish(transaction, f)
        except:
            # OH NO! This isn't supposed to happen!
            # It's unlikely tpc_abort will get called...
            self.tpc_abort(transaction, _force=True)
            raise
        self._tpc_phase = next_phase
        self._ltid = committed_tid
        return committed_tid

    @metricmethod
    def tpc_abort(self, transaction, _force=False):
        # _force is not a public argument, it is an internal
        # implementation detail.
        try:
            self._tpc_phase = self._tpc_phase.tpc_abort(transaction, _force)
        except BaseException:
            if _force:
                # We're here under unexpected circumstances. It's possible something
                # might go wrong rolling back.
                self._tpc_phase = NotInTransaction.from_storage(self)
            raise

    def afterCompletion(self):
        # Note that this method exists mainly to deal with read-only
        # transactions that don't go through 2-phase commit (although
        # it's called for all transactions). For this reason, we only
        # have to roll back the load connection. The store connection
        # is completed during normal write-transaction commit or
        # abort.

        # The next time we use the load connection, it will need to poll
        # and will call our __on_first_use.
        # Typically our next call from the ZODB Connection will be from its
        # `newTransaction` method, a forced `sync` followed by `poll_invalidations`.

        # This doesn't use restart_and_call() or even just restart() because we don't
        # need to do those checks yet, we just want to quietly rollback.
        # They both rollback; the difference is that restart_load checks for replicas,
        # and calls any hooks needed.
        self._load_connection.rollback_quietly()

    def lastTransaction(self):
        if self._ltid == z64 and self._prev_polled_tid is None:
            # We haven't committed *or* polled for transactions,
            # so our MVCC state is "floating".
            # Read directly from the database to get the latest value,
            return int64_to_8bytes(self._adapter.txncontrol.get_tid(self._load_connection.cursor))

        return max(self._ltid, int64_to_8bytes(self._prev_polled_tid or 0))

    def lastTransactionInt(self):
        return bytes8_to_int64(self.lastTransaction())

    def new_oid(self):
        # If we're committing, we can't restart the connection.
        return self._oids.new_oid(bool(self._tpc_phase))

    def iterator(self, start=None, stop=None):
        # XXX: This is broken for purposes of copyTransactionsFrom() because
        # it can only be iterated over once. zodbconvert works around this.
        return TransactionIterator(self._adapter, start, stop)

    def sync(self, force=True):
        """
        Updates to a current view of the database.

        This is implemented by rolling back the relational database
        transaction.

        .. versionchanged:: 3.0a6
           This method now pays attention to the *force* argument. If it is
           ``False``, this method does nothing. This prevents extra rollbacks
           and network traffic in the common case of using the default implicit
           transactions when this object is used by a ZODB ``Connection``.

        """
        if not force:
            # When we get a force=False, it's from Connection.afterCompletion()
            # calling Connection.newTransaction() because the transaction manager is
            # in implicit mode. Immediately after it does that, it will call
            # poll_invalidations()...and in our implementation, that implies a sync.
            # So we can avoid the overhead of the extra rollback.
            return

        rolled_back = self._load_connection.rollback_quietly()
        if not rolled_back:
            # Disconnected. Well, the rollback happens automatically
            # in that case. No big deal, ignore it.
            #
            # However, if we happened to be in the middle of committing and were
            # asked to sync (XXX: why would we?) then that's probably a problem that
            # needs to be handled and the commit rolled back too.
            if self._tpc_phase:
                raise StorageDisconnectedDuringCommit()

    def __stale(self, stale_error):
        # Allow GC to do its thing with the locals
        clear_frames(sys.exc_info()[2])
        replacements = {}
        my_ns = vars(self)
        for k, v in my_ns.items():
            if k in self._STALE_IGNORED_ATTRS:
                continue
            if callable(getattr(v, 'stale', None)):
                new_v = v.stale(stale_error)
                replacements[k] = new_v

        my_ns.update(replacements)

        # The next time we rollback or reopen, we're no longer stale
        # Note that this creates a reference cycle, but it should only be
        # temporary.
        self._load_connection.on_rolledback = self.__no_longer_stale
        self._load_connection.on_opened = self.__no_longer_stale

    def __no_longer_stale(self, _conn, _cursor):
        # This is called at the end of every transaction in
        # afterCompletion(), and at the beginning of every transaction
        # in sync() (called from Connection.newTransaction(), which is
        # called automatically from Connection.afterCompletion when
        # transactions are not explicit) Most of the time we won't be
        # stale, which is why we only install these hooks when we are,
        # and are careful to clean them up when we're not.
        #
        # Both _conn and _cursor will be None if the rollback detected
        # a disconnect from the database.

        my_ns = vars(self)
        replacements = {
            k: v.no_longer_stale()
            for k, v in my_ns.items()
            if k not in self._STALE_IGNORED_ATTRS
            and callable(getattr(v, 'no_longer_stale', None))
        }

        my_ns.update(replacements)

        del self._load_connection.on_rolledback
        del self._load_connection.on_opened

    def __on_load_first_use(self, conn, cursor):
        """
        Poll for invalidations, update our cache.

        Move the component objects of this object into or out of the
        stale state as appropriate.
        """
        # Ignore changes made by the last transaction committed
        # by this connection.
        if self._ltid is not None:
            ignore_tid = bytes8_to_int64(self._ltid)
        else:
            ignore_tid = None
        prev = self._prev_polled_tid

        # get a list of changed OIDs and the most recent tid
        try:
            changes, new_polled_tid = self._adapter.poller.poll_invalidations(
                conn, cursor,
                prev, ignore_tid
            )
        except ReadConflictError as e:
            # The database connection is stale, but postpone this
            # error until the application tries to read or write something.
            # XXX: We probably need to drop our pickle cache? At least the local
            # delta_after* maps/current_tid/checkpoints?
            log.error("ReadConflictError from polling invalidations; %s", e)
            self.__stale(e)
            return (), prev

        # Inform the cache of the changes.
        #
        # It's not good if this raises a CacheConsistencyError, that should
        # be a fresh connection. It's not clear that we can take any steps to
        # recover that the cache hasn't already taken.
        self._cache.after_poll(
            cursor,
            prev,
            new_polled_tid, changes
        )

        return changes, new_polled_tid

    def poll_invalidations(self):
        """Look for OIDs of objects that changed since _prev_polled_tid.

        Returns {oid: 1}, or None if all objects need to be invalidated
        because prev_polled_tid is not in the database (presumably it
        has been packed).
        """
        if self._closed:
            return {}

        changes, new_polled_tid = self._load_connection.restart_and_call(
            self.__on_load_first_use
        )
        # Now we're in a fully synced state, meaning
        # we don't need to do anything fancy with the load connection
        # anymore. Let it know that.
        self._load_connection.active = True

        self._prev_polled_tid = new_polled_tid

        if changes is None:
            oids = None
        else:
            # The value is ignored, only key matters
            oids = {int64_to_8bytes(oid_int): 1 for oid_int, _tid_int in changes}
        return oids

    def temporaryDirectory(self):
        """Return a directory that should be used for uncommitted blob data.

        If Blobs use this, then commits can be performed with a simple rename.
        """
        return self.blobhelper.temporaryDirectory()

    def copyTransactionsFrom(self, other):
        Copy(self.blobhelper, self, self).copyTransactionsFrom(other)
        self._adapter.stats.large_database_change()

    def pack(self, t, referencesf, prepack_only=False, skip_prepack=False):
        pack = Pack(self._options, self._adapter, self.blobhelper, self._cache)
        pack.pack(t, referencesf, prepack_only, skip_prepack)
        self.sync()

        self._pack_finished()

    def _pack_finished(self):
        "Hook for testing."


def _zlibstorage_new_instance(self):
    new_self = type(self).__new__(type(self))
    # Preserve _transform, etc
    new_self.__dict__ = self.__dict__.copy()
    new_self.base = self.base.new_instance()
    # Because these are bound methods, we must re-copy
    # them or ivars might be wrong, like _transaction
    for name in self.copied_methods:
        v = getattr(new_self.base, name, None)
        if v is not None:
            setattr(new_self, name, v)
    return new_self
