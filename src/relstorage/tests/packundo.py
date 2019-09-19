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

import functools


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

    OID_ROOT = 0
    OID_A = 1
    OID_B = 2
    OID_C = 3
    OID_D = 4

    OID_INITIAL_SET = (OID_ROOT, OID_A, OID_B, OID_C)

    def _create_initial_state(self):
        # Given a set of referencing objects present at the beginning
        # of the pre pack:
        #          0    1    2    3
        #   T1: root -> A -> B -> C
        #
        # If a new transaction is committed such that the graph becomes:
        #
        #         0    1
        #  T2: root -> A
        #          \-> B -> D -> C
        #              2    4    3
        #
        # That is, C is no longer referenced from B but a new object
        # D, B is referenced not from A but from the root.
        txm = transaction.TransactionManager(explicit=True)
        conn = self.main_db.open(txm)
        txm.begin()

        A = conn.root.A = PersistentMapping() # OID 0x1
        B = A['B'] = PersistentMapping() # OID 0x2
        C = B['C'] = PersistentMapping() # OID 0x3

        txm.commit()
        oids = {
            'A': A._p_oid,
            'B': B._p_oid,
            'C': C._p_oid,
        }
        conn.close()

        return oids

    def _mutate_state(self, initial_oids, *_args, **_kwargs):
        txm = transaction.TransactionManager(explicit=True)
        conn = self.main_db.open(txm)
        txm.begin()

        A = conn.root.A
        B = A['B']
        del A['B']
        D = B['D'] = PersistentMapping() # OID 0x4
        C = B['C']
        del B['C']
        D['C'] = C

        txm.commit()

        initial_oids['D'] = D._p_oid
        conn.close()

    def _check_inject_changes(
            self,
            initial_oids, # type: Dict[str, bytes]
            inject_changes,
            delta=1,
            hook='on_filling_object_refs_added'
    ):
        # We expect inject_changes to mutate *initial_oids* by *delta*
        # (see _mutate_state)
        len_initial_oids = len(initial_oids)

        adapter = self._storage._adapter
        setattr(adapter.packundo, hook, inject_changes)

        # Pack to the current TID (RelStorage extension)
        packtime = None
        self._storage.pack(packtime, referencesf)

        # The on_filling_object_refs_added hook should have been called once
        self.assertEqual(len(initial_oids),
                         len_initial_oids + delta,
                         initial_oids)

        # All children should still exist.
        missing = {}
        for name, oid in initial_oids.items():
            try:
                state, _tid = self._storage.load(oid, '')
            except KeyError as e:
                missing[name] = e
            else:
                self.assertIsNotNone(state)

        self.assertEmpty(missing)

    def test_pack_when_object_ref_moved_during_prepack(self):
        # If we mutate after we gather the initial list of
        # objects but before we start examining their state
        # we shouldn't collect any objects.
        expect_oids = self._create_initial_state()

        inject_changes = functools.partial(
            self._mutate_state,
            expect_oids
        )

        self._check_inject_changes(expect_oids,
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

        self._check_inject_changes(expect_oids,
                                   inject_changes)

class HistoryFreeTestPack(TestPackBase):
    # pylint:disable=abstract-method
    keep_history = False

    def test_pack_when_object_ref_moved_after_ref_finding_first_batch(self):
        # If we mutate after gather the initial list of objects, and after
        # finding references in the first batch (of all objects), we should not
        # collect anything.
        #
        # Even though this would seem to be similar to the 'ref_moved_during_prepack' case,
        # it is different. This currently fails because even though we find a reference
        # to the object, it's from a state that doesn't exist anymore
        # and we don't bother to look at it.
        expect_oids = self._create_initial_state()

        def inject_changes(oid_batch, refs_found):
            # We're examining the root and the first three objects
            self.assertEqual(sorted(oid_batch), sorted(self.OID_INITIAL_SET))
            self.assertEqual(len(refs_found), 3)
            # We're only called once: a single batch
            self.assertNotIn('D', expect_oids)
            self._mutate_state(expect_oids)

        self._check_inject_changes(
            expect_oids,
            inject_changes,
            hook='on_fill_object_ref_batch',
        )

        self.assertIn('D', expect_oids) # hook was called.

    def test_pack_when_object_ref_moved_just_before_examine_prev_reference(self):
        # We mutate just before we examine the state for the B object.
        expect_oids = self._create_initial_state()
        seen_oids = []
        def inject_changes(oid_batch, refs_found): # pylint:disable=unused-argument
            oid_batch = list(oid_batch)
            seen_oids.extend(oid_batch)
            self.assertEqual(1, len(oid_batch))
            if oid_batch == [self.OID_A]:
                # Next batch will be B.
                self._mutate_state(expect_oids)

        # Limit batch size and force commits.
        packundo = self._storage._adapter.packundo
        packundo.fill_object_refs_batch_size = 1
        packundo.fill_object_refs_commit_frequency = 0

        try:
            self._check_inject_changes(
                expect_oids,
                inject_changes,
                hook='on_fill_object_ref_batch',
            )
        finally:
            self.assertEqual(sorted(seen_oids), sorted(self.OID_INITIAL_SET))
            self.assertIn('D', expect_oids) # hook was called.


class HistoryPreservingTestPack(TestPackBase):
    # pylint:disable=abstract-method
    keep_history = True
