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
Test mixin dealing with packing, especially concurrently.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from persistent.mapping import PersistentMapping
import transaction
from ZODB.DB import DB
from ZODB.serialize import referencesf

from .reltestbase import RelStorageTestBase


class TestPackBase(RelStorageTestBase):
    # pylint:disable=abstract-method

    def setUp(self):
        super(TestPackBase, self).setUp()
        self.main_db = self._closing(DB(self._storage))

    def tearDown(self):
        self.main_db.close()
        self.main_db = None
        super(TestPackBase, self).tearDown()

    def _create_initial_state(self):
        txm = transaction.TransactionManager(explicit=True)
        conn = self.main_db.open(txm)
        txm.begin()

        A = conn.root.A = PersistentMapping()
        B = A['B'] = PersistentMapping()
        C = B['C'] = PersistentMapping()

        txm.commit()
        oids = {
            'A': A._p_oid,
            'B': B._p_oid,
            'C': C._p_oid,
        }
        conn.close()

        return oids

    def _inject_changes_after_object_refs_added_and_check_objects_present(
            self,
            initial_oids, # type: Dict[str, bytes]
            inject_changes,
            delta=1,
    ):
        # We expect inject_changes to mutate *initial_oids* by *delta*
        len_initial_oids = len(initial_oids)

        adapter = self._storage._adapter
        adapter.packundo.on_filling_object_refs_added = inject_changes

        # Pack to the current TID (RelStorage extension)
        packtime = None
        self._storage.pack(packtime, referencesf)

        # "The on_filling_object_refs_added hook should have been called once"
        self.assertEqual(len(initial_oids),
                         len_initial_oids + delta,
                         initial_oids)

        # All children should still exist.
        for name, oid in initial_oids.items():
            __traceback_info__ = name, oid
            state, _tid = self._storage.load(oid, '')
            self.assertIsNotNone(state)

    def test_pack_when_object_ref_moved_during_prepack(self):
        # Given a set of referencing objects present at the beginning
        # of the pre pack:
        #
        #   T1: root -> A -> B -> C
        #
        # If a new transaction is committed after we gather the initial list of
        # objects but before we start examining their state such that
        # the graph now becomes:
        #
        #  T2: root -> A
        #          \-> B -> D -> C
        #
        # That is, C is no longer referenced from B but a new object D, B is referenced
        # not from A but from the root.
        #
        # Then when we pack with GC, no objects are removed.
        expect_oids = self._create_initial_state()

        def inject_changes(**_kwargs):
            txm = transaction.TransactionManager(explicit=True)
            conn = self.main_db.open(txm)
            txm.begin()

            A = conn.root.A
            B = A['B']
            del A['B']
            D = B['D'] = PersistentMapping()
            C = B['C']
            del B['C']
            D['C'] = C

            txm.commit()

            expect_oids['D'] = D._p_oid
            conn.close()

        self._inject_changes_after_object_refs_added_and_check_objects_present(expect_oids,
                                                                               inject_changes)

    # https://ci.appveyor.com/project/jamadden/relstorage/build/1.0.19/job/a1vq619n84ss1s9a
    def test_pack_when_referring_object_mutates_during_prepack(self):
        # Packing should not remove objects referenced by an object
        # that changes during packing. In this case, we're adding a
        # new object (which should be untouched because it's not
        # visible as-of the pack-TID) and mutating an existing object
        # to refer to the new object as well as the new object. add
        # some data to be packed

        expect_oids = self._create_initial_state()

        def inject_changes(**_kwargs):
            # Change the database just after the list of objects
            # to analyze has been determined.
            txm = transaction.TransactionManager(explicit=True)
            conn = self.main_db.open(txm)
            txm.begin()
            Added = conn.root.ADDED = PersistentMapping()
            txm.commit()
            expect_oids['Added'] = Added._p_oid
            conn.close()

        self._inject_changes_after_object_refs_added_and_check_objects_present(expect_oids,
                                                                               inject_changes)

class HistoryFreeTestPack(TestPackBase):
    # pylint:disable=abstract-method
    keep_history = False

    @unittest.expectedFailure
    def test_pack_when_object_ref_moved_during_prepack(self):
        TestPackBase.test_pack_when_object_ref_moved_during_prepack(self)


class HistoryPreservingTestPack(TestPackBase):
    # pylint:disable=abstract-method
    keep_history = True
