# -*- coding: utf-8 -*-
"""
Helper implementations for MVCC.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading

from zope import interface

from BTrees import family64

from .interfaces import IMVCCDatabaseCoordinator
from .interfaces import IDetachableMVCCDatabaseViewer

@interface.implementer(IDetachableMVCCDatabaseViewer)
class DetachableMVCCDatabaseViewer(object):
    __slots__ = (
        'highest_visible_tid',
        'detached',
    )

    def __init__(self):
        self.highest_visible_tid = None
        self.detached = False


@interface.implementer(IMVCCDatabaseCoordinator)
class DetachableMVCCDatabaseCoordinator(object):
    """
    Simple implementation of :class:`IMVCCDatabaseCoordinator`
    that works with :class:`relstorage.interfaces.IDetachableMVCCDatabaseViewer`
    objects.

    We keep hard references to our viewers, so if they reference us there
    could be a cycle. Viewers must be hashable.

    The ``highest_visible_tid`` and ``detached`` values of the viewer
    must *only* be managed through this object.
    """

    maximum_highest_visible_tid = None
    minimum_highest_visible_tid = None

    def __init__(self):
        # Manipulations of metadata must be locked.
        # We don't always hold the lock; we rely on primitive operations of
        # the set() in _registered_viewers to be atomic.
        self._lock = threading.RLock()
        # {tid: {viewer, ...}} of objects not detached and not None
        self._by_tid = family64.IO.Bucket()
        self._registered_viewers = set()
        self.is_registered = self._registered_viewers.__contains__

    @property
    def _viewer_count_at_min(self):
        # Testing.
        if not self._registered_viewers or not self.minimum_highest_visible_tid:
            return 0
        viewers = self._by_tid.values()[0]
        return len(viewers)

    def register(self, viewer):
        with self._lock:
            if self.is_registered(viewer):
                return
            self._registered_viewers.add(viewer)
            if viewer.detached:
                return

            hvt = viewer.highest_visible_tid
            if hvt is None:
                return

            __traceback_info__ = hvt, viewer
            self._by_tid.setdefault(hvt, set()).add(viewer)
            self.minimum_highest_visible_tid = self._by_tid.minKey()
            self.maximum_highest_visible_tid = self._by_tid.maxKey()

    def unregister(self, viewer):
        with self._lock:
            if not self.is_registered(viewer):
                return

            self._registered_viewers.remove(viewer)
            if not self._registered_viewers:
                self.minimum_highest_visible_tid = None
                self.maximum_highest_visible_tid = None
                self._by_tid.clear()
                return

            self.__viewer_does_not_matter(viewer)

    def __set_tids(self):
        by_tid = self._by_tid
        if by_tid:
            self.minimum_highest_visible_tid = by_tid.minKey()
            self.maximum_highest_visible_tid = by_tid.maxKey()
        else:
            self.minimum_highest_visible_tid = None
            self.maximum_highest_visible_tid = None


    def __viewer_does_not_matter(self, viewer):
        # Because it was unregistered or because it
        # was detached.
        hvt = viewer.highest_visible_tid
        by_tid = self._by_tid
        if by_tid and hvt:
            viewers = by_tid.get(hvt)
            if viewers:
                viewers.discard(viewer)
                if not viewers:
                    del by_tid[hvt]
                self.__set_tids()

    def clear(self):
        with self._lock:
            self._registered_viewers.clear()
            self._by_tid.clear()
            self.__set_tids()

    def detach(self, viewer):
        """
        Cause the viewer to become detached.
        """
        with self._lock:
            if not self.is_registered(viewer):
                return

            viewer.detached = True
            self.__viewer_does_not_matter(viewer)

    def detach_all(self):
        with self._lock:
            for viewer in self._registered_viewers:
                viewer.detached = True
            self._by_tid.clear()
            self.__set_tids()

    def change(self, viewer, new_hvt):
        """
        Cause the viewer to have a new ``highest_visible_tid``,
        which can be greater, less, or equal to the current HVT,
        or None.

        If the *viewer* was previously detached, it is now attached.
        """
        with self._lock:
            if not self.is_registered(viewer):
                return

            viewer.detached = False

            old_hvt = viewer.highest_visible_tid
            viewer.highest_visible_tid = new_hvt
            by_tid = self._by_tid
            if old_hvt:
                viewers = by_tid.get(old_hvt)
                if viewers:
                    viewers.discard(viewer)
                    if not viewers:
                        del by_tid[old_hvt]
            if new_hvt:
                by_tid.setdefault(new_hvt, set()).add(viewer)

            self.__set_tids()

    def viewers_at_or_before(self, tid):
        """
        Return all the viewers with tids at least as old as the
        given tid.

        Passing the value from ``minumum_highest_visible_tid`` is always safe,
        even if that value is None. If that value is None, it's because we
        have no viewers, or the viewers we do have haven't looked at the
        database; they'll be ignored.

        Viewers that are already explicitly detached are also ignored.
        """
        with self._lock:
            by_tid = self._by_tid
            if not by_tid:
                return ()
            sets_before = by_tid.values(max=tid, excludemax=False)
            return set().union(*sets_before) if sets_before else ()

    def viewers_at_minimum(self):
        """
        Return all the viewers viewing the ``minimum_highest_visible_tid``.

        If that is None, this is the empty set.
        """
        with self._lock:
            if self._by_tid:
                return self._by_tid.values()[0]
            return ()

    def detach_viewers_at_minimum(self):
        """
        Cause all the viewers at the minimum, if any, to be detached.
        """
        with self._lock:
            if self._by_tid:
                at_min = self._by_tid.pop(self._by_tid.minKey())
                for viewer in at_min:
                    viewer.detached = True
                self.__set_tids()
