# -*- coding: utf-8 -*-
# distutils: language = c++
# cython: auto_pickle=False,embedsignature=True,always_allow_keywords=False,infer_types=True
"""
Cython implementation of the object indices used by :mod:`mvcc`.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

cimport cython
from cython.operator cimport dereference as deref
from cython.operator cimport preincrement as preincr
from cpython cimport PyObject

from libcpp.vector cimport vector
from libcpp.algorithm cimport copy
from libcpp.iterator cimport back_inserter

from relstorage._rs_types cimport OID_t
from relstorage._rs_types cimport TID_t
from relstorage._rs_types cimport PythonAllocator
from relstorage._inthashmap cimport OidTidMap
from relstorage._inthashmap cimport MapType
from relstorage._inthashmap cimport MapSizeType
from relstorage._inthashmap cimport VectorOidType
from relstorage._inthashmap cimport multiunion

from zope.interface import classImplements

from relstorage._compat import IN_TESTRUNNER

from relstorage.interfaces import IMVCCDatabaseViewer

cdef bint DEBUG = __debug__ and IN_TESTRUNNER

ctypedef PyObject* PyObjectPtr
ctypedef PythonAllocator[PyObjectPtr] PyObjectPtrAlloc

@cython.freelist(1000)
@cython.final
cdef class _TransactionRangeObjectIndex:
    """
    Holds the portion of the object index visible to transactions <=
    ``highest_visible_tid``.

    Initialized to be either empty, or with the *complete* index of
    objects <= ``highest_visible_tid`` and greater than
    ``complete_since_tid``. (That is, you cannot pass an 'ignore_txn'
    value to the poller. TODO: Workon that. Maybe there's a way.)

    ``highest_visible_tid`` must always be given, but
    ``complete_since_tid`` may be null.

    These attribute, once set, will never change. We may add data
    prior to and including ``complete_since_tid`` as we access it, but we have no
    guarantee that it is complete.
    """

    cdef readonly TID_t highest_visible_tid
    # We store -1 for "not complete" and expose a property
    # without the leading underscore that maps that back to None
    cdef TID_t _complete_since_tid
    cdef public bint accepts_writes
    cdef OidTidMap bucket

    def __init__(self, highest_visible_tid=0, complete_since_tid=None, data=()):
        assert complete_since_tid is None or highest_visible_tid >= complete_since_tid
        self.highest_visible_tid = highest_visible_tid
        self._complete_since_tid = complete_since_tid if complete_since_tid is not None else -1
        self.accepts_writes = True

        self.bucket = OidTidMap(data)

        if self.size():
            # Verify the data matches what they told us.
            # If we were constructed with data, we must be complete.
            # Otherwise we get built up bit by bit.
            if DEBUG:
                assert self._complete_since_tid
                self.verify()
        else:
            # If we had no changes, then either we polled for the same tid
            # as we got, or we didn't try to poll for changes at all.
            assert complete_since_tid is None or complete_since_tid == highest_visible_tid, (
                complete_since_tid, highest_visible_tid
            )

    @property
    def raw_data(self):
        """
        Returns the underlying map. Only use this in testing.
        """
        return self.bucket

    @property
    def complete_since_tid(self):
        if self._complete_since_tid == -1:
            return None
        return self._complete_since_tid

    cpdef verify(self, bint initial=True):
        # Check that our constraints are met
        if not self.size() or not __debug__:
            return

        max_stored_tid = self.max_stored_tid()
        min_stored_tid = self.min_stored_tid()
        hvt = self.highest_visible_tid
        if max_stored_tid > hvt:
            raise TypeError("max_stored_tid should be <= hvt")
        if min_stored_tid < 0:
            raise TypeError("Underflow error: min TID must be >= 0")

        if initial:
            # This is only true at startup. Over time we can add older entries.
            assert self._complete_since_tid == -1 or min_stored_tid > self._complete_since_tid, (
                min_stored_tid, self._complete_since_tid)

    cpdef complete_to(self, _TransactionRangeObjectIndex newer_bucket):
        """
        Given an incomplete bucket (this object) and a possibly-complete bucket for the
        same or a later TID, merge this one to hold the same data and be complete
        for the same transaction range.

        This bucket will be complete for the given bucket's completion, *if* the
        given bucket actually had a different tid than this one. If the given
        bucket was the same tid, then nothing changed and we can't presume
        to be complete.
        """
        assert self._complete_since_tid == -1
        assert newer_bucket.highest_visible_tid >= self.highest_visible_tid
        self.bucket.update(newer_bucket.bucket)
        if newer_bucket.highest_visible_tid > self.highest_visible_tid:
            self.highest_visible_tid = newer_bucket.highest_visible_tid
            self._complete_since_tid = newer_bucket._complete_since_tid

    cpdef merge_same_tid(self, _TransactionRangeObjectIndex bucket):
        """
        Given an incoming complete bucket for the same highest tid as this bucket,
        merge the two into this object.
        """
        assert bucket.highest_visible_tid == self.highest_visible_tid
        self.bucket.update(bucket.bucket)
        if bucket._complete_since_tid < self._complete_since_tid:
            self._complete_since_tid = bucket._complete_since_tid

    cpdef merge_older_tid(self, _TransactionRangeObjectIndex bucket):
        """
        Given an incoming complete bucket for an older tid than this bucket,
        merge the two into this object.

        Because we're newer, entries in this bucket supercede objects
        in the incoming data.

        If the *bucket* was complete to a transaction earlier than the transaction
        we're complete to, we become complete to that earlier transaction.

        Does not modify the *bucket*.
        """
        assert bucket.highest_visible_tid <= self.highest_visible_tid
        #debug('Diff between self', dict(self), "and", dict(bucket), items_not_in_self)
        # bring missing data into ourself, being careful not to overwrite
        # things we do have.
        # XXX: This could be one C call instead of two.
        self.bucket.update(bucket.items_not_in(self))
        if bucket._complete_since_tid != -1 \
           and bucket._complete_since_tid < self._complete_since_tid:
            self._complete_since_tid = bucket._complete_since_tid

    # These raise ValueError if the map is empty
    cpdef TID_t max_stored_tid(self) except -1:
        return self.bucket.maxValue()

    cpdef TID_t min_stored_tid(self) except -1:
        return self.bucket.minValue()


    ###
    # Mapping-like things.
    # NOTE: We deliberately do not implement __getitem__ to avoid
    # attempting to iterate that way.
    ###

    cdef MapSizeType size(self):
        return self.bucket.size()

    def __len__(self):
        return self.bucket.size()

    cpdef update(self, data):
        self.bucket.update(data)

    def __setitem__(self, OID_t key, TID_t value):
        self.set(key, value)

    cdef int set(self, OID_t key, TID_t value) except -1:
        return self.bucket.set(key, value)

    def __contains__(self, OID_t key):
        return self.contains(key)

    cdef bint contains(self, OID_t key) except -1:
        return self.bucket.contains(key)

    cpdef items(self):
        return self.bucket.items()

    cdef TID_t get(self, OID_t key) except -1:
        if self.bucket.contains(key):
            return self.bucket[key]
        raise KeyError(key)

    cpdef items_not_in(self, _TransactionRangeObjectIndex other):
        """
        Return the ``(k, v)`` pairs in self whose ``k`` is not found in *other*
        """
        return self.bucket.difference(other.bucket)

    def __repr__(self):
        return '<%s at 0x%x hvt=%s complete_after=%s len=%s readonly=%s>' % (
            self.__class__.__name__,
            id(self),
            self.highest_visible_tid,
            self._complete_since_tid,
            len(self),
            not self.accepts_writes,
        )

@cython.final
@cython.freelist(1000)
cdef class _ObjectIndex:
    """
    Collectively holds all the object index visible to transactions <=
    ``highest_visible_tid``.

    The entries in ``maps`` attribute are instances of
    :class:`_TransactionRangeObjectIndex`, and our
    ``highest_visible_tid`` is exactly that of the frontmost map. For
    convenience, we also define this to be our
    ``maximum_highest_visible_tid``. The ``highest_visible_tid`` of
    the final map defines our ``minimum_highest_visible_tid``.

    The following describes the general evolution of this index and
    its MVCC strategy. Some of this is implemented here, but much of
    it is implemented in the :class:`MVCCDatabaseCoordinator`.

    .. rubric:: Initial State

    In the beginning, the frontmost map will be empty and incomplete;
    it will have a ``highest_visible_tid`` but no
    ``complete_since_tid``. (Alternately, if we restored from cache,
    we may have data, but we are definitely still incomplete.) We may
    load new object indices into this map, but only if they're less
    than what our ``highest_visible_tid`` is (this is just a special
    version of the general rule for updates laid out below).

    When a new poll takes place and produces a new change list, in the
    form of a :class:`_TransactionRangeObjectIndex`, it is merged with
    the previous map and replaced into this object. This map does have
    a ``complete_since_tid.``

    After this first map is added, moving forward there should be no
    gaps in ``complete_since_tid`` coverage. There may be overlaps,
    but there must be no gaps. Thus, we have complete coverage from
    ``highest_visible_tid`` in the first map all the way back to
    ``complete_since_tid`` in the second-to-last map.

    If the front of this instance already has data matching that same
    ``highest_visible_tid``, the two maps are merged together
    preserving the lowest ``complete_since_tid`` value. In this case,
    a new instance is *not* returned; the same instance is returned so
    that this map can be shared.

    If the front of this instance has a ``highest_visible_tid`` *less*
    than the new polled tid (that is, there's a new TID in the
    database) all data in the new map with a TID less than the current
    ``highest_visible_tid`` could be discarded (but since we walk the
    maps from front to back to find entries, the more data up front
    the better). In fact, we could limit the poll to just that range
    (use ``highest_visible_tid`` as our ``prev_polled_tid``
    parameter), if we're willing to walk through the current map and
    find all objects whose TID is greater than what we would have used
    for ``prev_polled_tid`` (assuming it's >= the current
    ``complete_since_tid`` value) --- but it's probably better to just
    let the database do that.

    On the chance that the value we wish to poll from
    (``prev_polled_tid``) is <= our current ``complete_since_tid``,
    the new map can completely replace the current front of the map
    (merging in any extra data).

    Instead of updating the frontmost map when new a new entry is
    presented, we update the oldest map that can see the new entry.
    The older (farther to the right, or towards the end of the
    ``maps`` list), the more likely it is to be shared by the most
    transactions. (That does have the unfortunate effect of making it
    take longer to get a hit since we have to probe further back in
    the map; maybe we want to update both frontmost and backmost?)

    When a transaction ends and polls, such that the
    ``MVCCDatabaseCoordinator.minimum_highest_visible_tid`` increments
    (which also corresponds to the last reference to that
    ``TransactionRangeObjectIndex`` going away), that map is removed
    from the list (where it was the end), and any unique data in it is
    added to the map at the new back of the list. (The object that was
    removed is *cleared* and marked as *inactive*: other transactions
    will still have that same object on their list, but they shouldn't
    use it anymore to store new index entries.)

    This last map will keep growing. We shrink it in one way:

        - When we merge the back two maps, we examine the map being
          removed. This is a snapshot of the database no longer in
          use. We first remove any entries that have newer data in a
          more recent snapshot. Then, any remaining unique entries
          (which by definition are the current state of the affected
          objects) are simply removed (note that we don't search for
          duplicates). Crucially, to obtain decent hit rates, those
          dropped index entries are re-cached with a special key
          indicating that they haven't changed in a long time;
          operations that update the cache with new data must
          invalidate this special key. If an object is not found in
          this index, it can then be looked for under this special
          key. Even though the index data is not complete, this
          "frozen" object can still be found.

    When merging this last map, we have an excellent time to purge
    entries from the cache: Any OIDs stored in the last map (the one
    being removed) who have entries in other maps has their stored
    OID/TID from the last map removed. These have changed, and by
    definition their old state is not visible to any other connection.

    TODO: The merging and recaching might be expensive? Perhaps we
    want to defer it until some threshold is reached?
    """

    # We use two attributes to hold our ordered collection of
    # maps; each entry in one corresponds to the same entry in the
    # other.
    #
    # Maps are read from 0...N, so newest bucket must be first.

    # The Python list object stores the Python _TransactionRangeObjectIndex
    cdef list maps
    # The C++ vector stores a C++ pointer to the
    # _TransactionRangeObjectIndex object This lets us perform C++
    # operations faster, we avoid iterating lists and performing type checks.
    #
    # NOTE: "Casting to <object>
    # creates an owned reference. Cython will automatically perform a
    # Py_INCREF and Py_DECREF operation. Casting to <PyObject *>
    # creates a borrowed reference, leaving the refcount unchanged."
    # We store borrowed references here.
    cdef vector[PyObjectPtr, PyObjectPtrAlloc] c_maps


    def __init__(self, highest_visible_tid, complete_since_tid=None, data=()):
        """
        An instance is created with the first poll, giving us our
        initial TID. It may optionally have data retrieved from
        previously saving the map.
        """
        initial_bucket = _TransactionRangeObjectIndex(highest_visible_tid, None, ())
        initial_bucket.update(data)
        if complete_since_tid:
            initial_bucket._complete_since_tid = complete_since_tid
        initial_bucket.verify(initial=False)

        self.maps = [initial_bucket]
        self.c_maps.push_back(<PyObject*>initial_bucket)

    def __repr__(self):
        return '<%s at 0x%x maxhvt=%s minhvt=%s cst=%s depth=%s>' % (
            self.__class__.__name__,
            id(self),
            self.maximum_highest_visible_tid,
            self.minimum_highest_visible_tid,
            self.complete_since_tid,
            self.depth,
        )

    def stats(self):
        return {
            'depth': self.depth,
            'hvt': self.maximum_highest_visible_tid,
            'min hvt': self.minimum_highest_visible_tid,
            'total OIDS': self.total_size,
            'unique OIDs': self.unique_key_count(),
        }

    cdef size_t unique_key_count(self):
        cdef _TransactionRangeObjectIndex m
        return multiunion([
            m.bucket for m in self.maps
        ], self.c_total_size()).size()

    def __getitem__(self, OID_t oid):
        for mapping in self.c_maps:
            search = (<_TransactionRangeObjectIndex>mapping).bucket._map.find(oid)
            if search != (<_TransactionRangeObjectIndex>mapping).bucket._map.end():
                return deref(search).second

        # No data. Could it be frozen? We'll let the caller decide.
        return None

    def __contains__(self, OID_t oid):
        for mapping in self.c_maps:
            search = (<_TransactionRangeObjectIndex>mapping).bucket._map.find(oid)
            if search != (<_TransactionRangeObjectIndex>mapping).bucket._map.end():
                return True
        return False

    def __setitem__(self, OID_t oid, TID_t tid):
        # Silently discard things that are too new.
        # Something like loadSerial for conflict resolution might do this;
        # we'll see the object from the future when we poll for changes.

        # Because each successive map is supposed to be a delta, we only
        # store this in the first possible map.

        # TODO: Maybe this should always store into the master object?
        cdef _TransactionRangeObjectIndex mapping
        it = self.c_maps.rbegin()
        end = self.c_maps.rend()
        while it != end:
            ptr = deref(it)
            mapping = <_TransactionRangeObjectIndex>ptr
            preincr(it)

            mapping_hvt = mapping.highest_visible_tid
            accepts_writes = mapping.accepts_writes
            if not accepts_writes:
                # Closed, but still in our list because this is a shared object.
                continue
            if tid > mapping_hvt:
                # Not visible; everything still to come is newer yet,
                # so we're done.
                break
            if mapping._complete_since_tid != -1 and tid > mapping._complete_since_tid:
                assert mapping.contains(oid), (oid, tid, mapping)
                assert mapping.get(oid) == tid, mapping
                continue
            assert mapping._complete_since_tid == -1 or tid <= mapping._complete_since_tid
            mapping.set(oid, tid)
            break

    def as_dict(self):
        result = {}
        it = self.c_maps.rbegin()
        end = self.c_maps.rend()
        while it != end:
            m = <_TransactionRangeObjectIndex>deref(it)
            preincr(it)
            result.update(m.items())
        return result

    cdef MapSizeType c_total_size(self):
        cdef MapSizeType total_size = 0
        for ptr in self.c_maps:
            total_size += (<_TransactionRangeObjectIndex>ptr).size()
        return total_size

    @property
    def total_size(self):
        """
        The total size of this object is the combined length of all maps;
        this is not the same thing as the length of unique keys.
        """
        return self.c_total_size()

    @property
    def depth(self):
        return self.c_maps.size()

    @property
    def highest_visible_tid(self):
        assert not self.c_maps.empty()
        ptr = self.c_maps.front()
        return (<_TransactionRangeObjectIndex>ptr).highest_visible_tid

    # The maximum_highest_visible_tid of this object, and indeed, of any
    # given map, must never change. It must always match what's
    # visible to the clients it has been handed to.
    @property
    def maximum_highest_visible_tid(self):
        return self.highest_visible_tid

    @property
    def minimum_highest_visible_tid(self):
        assert not self.c_maps.empty()
        ptr = self.c_maps.back()
        return (<_TransactionRangeObjectIndex>ptr).highest_visible_tid

    @property
    def complete_since_tid(self):
        # We are complete since the oldest map we have
        # that thinks *it* is complete. This may not necessarily be the
        # last map (but it should be the second to last map!)
        it = self.c_maps.rbegin()
        end = self.c_maps.rend()
        while it != end:
            ptr = deref(it)
            preincr(it)

            cst = (<_TransactionRangeObjectIndex>ptr)._complete_since_tid
            if cst != -1:
                return cst

    cpdef verify(self):
        # Each component has values in range.
        for ptr in self.c_maps:
            (<_TransactionRangeObjectIndex>ptr).verify(initial=False)
        # No gaps in completion
        map_iter = self.c_maps.begin()
        newest_map = <_TransactionRangeObjectIndex>deref(map_iter)
        end = self.c_maps.end()

        while map_iter != end:
            mapping = <_TransactionRangeObjectIndex>deref(map_iter)
            preincr(map_iter)
            assert newest_map._complete_since_tid <= mapping.highest_visible_tid
            newest_map = mapping

    cdef _ObjectIndex _replace_maps(self,
                                    _TransactionRangeObjectIndex first,
                                    _TransactionRangeObjectIndex second=None,
                                    _ObjectIndex extend_index=None):
        self.maps = []
        if first is not None:
            self.maps.append(first)
            self.c_maps.push_back(<PyObject*>first)
        if second is not None:
            self.maps.append(second)
            self.c_maps.push_back(<PyObject*>second)

        if extend_index is not None:
            self.maps.extend(extend_index.maps)
            copy(extend_index.c_maps.begin(), extend_index.c_maps.end(),
                 back_inserter(self.c_maps))

        return self

    cpdef _ObjectIndex with_polled_changes(self,
                                           TID_t highest_visible_tid,
                                           TID_t complete_since_tid,
                                           changes):
        cdef _TransactionRangeObjectIndex incoming_bucket
        cdef _TransactionRangeObjectIndex newest_bucket
        cdef _TransactionRangeObjectIndex oldest_bucket
        cdef _ObjectIndex other

        # Never call this when the poller has specifically said
        # that there are no changes; either the very first poll, or
        # we went backwards due to reverting to a stale state. That's
        # handled at a higher layer.
        assert changes is not None
        assert self.c_maps.size() > 0
        #assert highest_visible_tid >= self.highest_visible_tid
        #assert complete_since_tid is not None

        # First, create the transaction map.
        assert highest_visible_tid and complete_since_tid
        incoming_bucket = _TransactionRangeObjectIndex(highest_visible_tid,
                                                       complete_since_tid,
                                                       changes)
        newest_bucket = <_TransactionRangeObjectIndex>self.c_maps.front()
        oldest_bucket = <_TransactionRangeObjectIndex>self.c_maps.back()

        # Was this our first poll?
        if newest_bucket is oldest_bucket and oldest_bucket._complete_since_tid == -1:
            # Merge the two together and replace ourself.
            assert highest_visible_tid >= oldest_bucket.highest_visible_tid
            if highest_visible_tid == oldest_bucket.highest_visible_tid:
                # Overwrite the incomplete data with the new data, which *may be* complete
                # back to a point. The make the incomplete data become the complete data
                oldest_bucket.complete_to(incoming_bucket)
                #debug("Oldest bucket is now", oldest_bucket, dict(oldest_bucket))
                oldest_bucket.verify()
                return self
            # We need to move forward. Therefore we need a new index.
            # Copy forward any old data we've got and maintain our completion status.
            # But we don't want to lose our connection to the older
            # bucket, because that represents the oldest thing we have indexed.
            # Why merge? That doesn't make much sense. We're no longer a delta.
            # incoming_bucket.merge_older_tid(oldest_bucket)
            other = _ObjectIndex.__new__(_ObjectIndex)
            other._replace_maps(incoming_bucket, oldest_bucket)
            other.verify()
            return other

        # Special cases:
        #
        # - len(incoming_bucket) == 0: No changes. Therefore, if this was
        #   not our first poll, it must have returned the exact same
        #   highest_visible_tid as our current highest visible tid. We
        #   just need to set our frontmost map's
        #   ``complete_since_tid`` to the lowest of the two values.
        #   This is just a degenerate case of polling to a matching highest_visible_tid.

        # We can't get a higher transaction id unless something changed.
        # put the other way, if nothing changed, it must be for our existing tid.
        assert incoming_bucket or (
            not incoming_bucket and highest_visible_tid == newest_bucket.highest_visible_tid
        )

        if highest_visible_tid == newest_bucket.highest_visible_tid:
            #debug("Merging for same tid", newest_bucket, "incoming", incoming_bucket)
            newest_bucket.merge_same_tid(incoming_bucket)
            return self

        # all that's left is to put the new bucket on front of a new object.
        other = _ObjectIndex.__new__(_ObjectIndex)
        other._replace_maps(incoming_bucket, None, self)

        if DEBUG:
            other.verify()
        return other

    cpdef OidTidMap collect_changes_after(self, TID_t last_seen_tid):
        """
        Given a transaction ID *last_seen_tid*, find and return all
        ``(oid, tid)`` pairs (in an `OidTidMap`) that have changed
        **after** the *last_seen_tid*.

        In the result, each ``tid`` is the most recently changed transaction
        ID for the given ``oid``.
        """
        cdef OidTidMap changes = OidTidMap()
        cdef vector[PyObjectPtr, PyObjectPtrAlloc] change_dicts
        cdef _TransactionRangeObjectIndex mapping

        for ptr in self.c_maps:
            mapping = <_TransactionRangeObjectIndex>ptr
            if mapping.highest_visible_tid <= last_seen_tid:
                break
            change_dicts.push_back(<PyObject*>mapping.bucket)

        it = change_dicts.rbegin()
        end = change_dicts.rend()
        while it != end:
            # In reverse order, capturing only the most recent change.
            # TODO: Except for that 'ignore_tid' passed to the viewer's
            # poll method, we could very efficiently do this with
            # OidTMap_multiunion with one call to C.
            changes.update_from_other_map(<OidTidMap>deref(it))
            preincr(it)
        return changes

    def get_second_oldest_transaction(self):
        return self.maps[-2]

    def get_newest_transaction(self):
        return self.maps[0]

    def get_oldest_transaction(self):
        return self.maps[-1]

    def get_transactions_from(self, ix):
        return self.maps[ix:]

    def remove_oldest_transaction_and_collect_invalidations(self, OidTidMap to_delete):
        """
        Remove the oldest transaction record, and return it.

        Before returning it, however, compare the OIDs in it with all
        the OIDs remaining in this index. For all the OIDs in common
        where we have an invalidation for the OID *after* this oldest
        transaction, record that ``(oid, obsolete_tid)`` in the
        *to_delete* map, and remove it from the transaction record.

        Once this is done, anything still found in the record is known
        not to have been invalidated up to
        `maximum_highest_visible_tid` and could be considered for
        special cache status (freezing the key). The reverse is true
        for anything found in *to_delete*: those are invalid, and any
        cache data for that OID and with a TID <= the TID in
        *to_delete* can be discarded.
        """
        cdef _TransactionRangeObjectIndex obsolete_bucket

        obsolete_bucket = <_TransactionRangeObjectIndex>self.maps.pop()
        assert obsolete_bucket is <_TransactionRangeObjectIndex>self.c_maps.back()
        self.c_maps.pop_back()
        if obsolete_bucket is None or to_delete is None:
            raise TypeError

        # Immediately also mark it as closed before we start mutating its
        # contents. No more storing to this one!
        obsolete_bucket.accepts_writes = False
        self._remove_non_matching_values(obsolete_bucket, to_delete)
        return obsolete_bucket

    cdef void _remove_non_matching_values(self,
                                          _TransactionRangeObjectIndex obsolete_bucket,
                                          OidTidMap to_delete) except +:
        # XXX: Now that we have a more efficient multiunion,
        # see what happens if we use that.

        # The original algorithm was simple:
        # (1) Pop the old bucket.
        # (2) Union together all the keys in all the remaining buckets
        # (3) Intersect this with the keys in the old bucket.
        # (4) Iterate this intersection, getting the TID from both the old
        #     bucket and the index, and compare them. If they didn't match
        #     discard it.
        #
        # But this had several performance bottlenecks hiding under those simple
        # steps. Benchmarks would take 55s dealing with 1000 transactions of
        # 250 distinct objects (172,558 OIDs), and 46s with all OIDs the same
        # across transactions (250 total OIDs).
        #
        # The implementation here takes the whole benchmark to 1.7s for
        # the small group of OIDs, and 30s for the big group of OIDs.

        cdef MapType.iterator obsolete_it
        cdef MapType.iterator obsolete_end
        cdef _TransactionRangeObjectIndex mapping
        cdef bint removed


        # TODO: Drop the GIL for this? We can't cast to Python objects
        # in a ``nogil`` section (assignment touches reference counts)
        # we'd have to workaround that.

        obsolete_it = obsolete_bucket.bucket._map.begin()
        obsolete_end = obsolete_bucket.bucket._map.end()

        while obsolete_it != obsolete_end:
            removed = False
            for ptr in self.c_maps:
                mapping = <_TransactionRangeObjectIndex>ptr

                found = mapping.bucket._map.find(deref(obsolete_it).first)
                if found != mapping.bucket._map.end():
                    # Yay, we found it. Does it match?
                    if deref(found).second != deref(obsolete_it).second:
                        # It does not. We have something newer. Drop it.
                        # Note that even though we're removing data from
                        # this bucket that might be in the range that it
                        # claims to have complete index data for, that's
                        # fine: The end result when we put everything back
                        # together is still going to be complete index
                        # data, because the object changed in the future.
                        # This particular transaction chunk won't be complete, but
                        # it's inaccessible.
                        # This is where we should hook in the 'invalidation' tracing.
                        removed = True
                        to_delete.set(deref(obsolete_it).first, deref(obsolete_it).second)
                        # erase() returns an iterator to the *next* element, so we don't
                        # need or want to increment it.
                        obsolete_it = obsolete_bucket.bucket._map.erase(obsolete_it)
                        obsolete_end = obsolete_bucket.bucket._map.end()
                    break
            if not removed:
                preincr(obsolete_it)

classImplements(_ObjectIndex, IMVCCDatabaseViewer)
