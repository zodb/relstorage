# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from relstorage.cache import local_database
from relstorage.cache.local_database import Database
from relstorage.cache.persistence import sqlite_connect

from relstorage.tests import TestCase
from relstorage.cache.tests import MockOptions

class MockOptionsWithMemoryDB(MockOptions):
    cache_local_dir = ':memory:'


class UpdateTests(TestCase):
    # Tests to specifically cover the cases that
    # UPSERTS or multi-column updates aren't available.

    USE_UPSERT = False

    def setUp(self):
        self.options = MockOptionsWithMemoryDB()
        self.connection = sqlite_connect(
            self.options, "pfx-ignored")
        assert self.connection.rs_db_filename == ':memory:', self.connection
        self.db = self._makeOne()

    def tearDown(self):
        # Be sure we can commit; this can be an issue on PyPy
        self.connection.commit()
        self.db.close()

    def _makeOne(self):
        db = Database.from_connection(
            self.connection,
            use_upsert=self.USE_UPSERT,
        )
        ost = db.store_temp
        def store_temp(rows):
            return ost([
                (r[0], r[1], 0, r[2], r[3])
                for r
                in rows
            ])
        db.store_temp = store_temp
        db.real_store_temp = ost
        return db


    def test_set_checkpoints(self):
        self.db.update_checkpoints(1, 0)
        self.assertEqual(self.db.checkpoints, (1, 0))

    def test_update_checkpoints_newer(self):
        self.db.update_checkpoints(1, 0)
        self.db.update_checkpoints(2, 1)
        self.assertEqual(self.db.checkpoints, (2, 1))

    def test_update_checkpoints_older(self):
        self.db.update_checkpoints(2, 1)
        self.db.update_checkpoints(1, 0)
        self.assertEqual(self.db.checkpoints, (2, 1))

    def test_move_from_temp_empty(self):
        rows = [
            (0, 0, b'', 0),
            (1, 0, b'', 0)
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()
        self.assertEqual(dict(self.db.oid_to_tid),
                         {0: 0, 1: 0})

    def test_move_from_temp_mixed_updates(self):
        rows = [
            (0, 1, 0, b'0', 0),
            (1, 1, 0, b'1', 0),
            (2, 1, 0, b'2', 0),
        ]
        self.db.real_store_temp(rows)
        self.db.move_from_temp()

        new_rows = [
            # 0 goes backwards
            (0, 0, 0, b'-1', 0),
            # 1 stays the same (but we use a different state
            # to verify)
            (1, 1, 0, b'-1', 0),
            # 2 moves forward
            (2, 2, 0, b'2b', 6)
        ]

        self.db.real_store_temp(new_rows)
        self.db.move_from_temp()

        self.connection.commit()
        self.assertEqual(
            dict(self.db.oid_to_tid),
            {0: 1, 1: 1, 2: 2}
        )

        rows_in_db = list(self.db.fetch_rows_by_priority())
        rows_in_db.sort()
        self.assertEqual(rows_in_db[0], (0, 1, b'0', 1, 0))
        self.assertEqual(rows_in_db[1], (1, 1, b'-1', 1, 0))
        self.assertEqual(rows_in_db[2], (2, 2, b'2b', 2, 6))

    def test_remove_invalid_persistent_oids(self):
        rows = [
            (0, 1, b'0', 0),
            (1, 1, b'0', 0),
        ]

        self.db.store_temp(rows)
        self.db.move_from_temp()

        invalid_oids = range(1, 5000)
        count = self.db.remove_invalid_persistent_oids(invalid_oids)
        self.assertEqual(dict(self.db.oid_to_tid), {0: 1})
        self.assertEqual(count, len(invalid_oids))


    def test_trim_to_size_deletes_stale(self):
        rows = [
            (0, 1, b'0', 0),
            (1, 1, b'0', 0),
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()

        self.db.trim_to_size(
            # It's not trimming for a limit
            10000,
            # But we do know stale things that have to go.
            # OID 1 must be at least TID 2
            {1: 2}
        )
        # Leaving behind only one row
        self.assertEqual(dict(self.db.oid_to_tid), {0: 1})

    def test_trim_to_size_removes_size_oldest_first(self):
        # We delete the oldest, least used, biggest objects first.
        # These are tied on frequency, size, and transaction age.
        # Tie-breaker is OID, which indicates an older object again.
        rows = [
            (0, 1, b'0', 0),
            (1, 1, b'0', 0),
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()

        self.db.trim_to_size(
            1,
            ()
        )
        self.assertEqual(dict(self.db.oid_to_tid), {1: 1})

    def test_trim_to_size_removes_least_frequent_first(self):
        # We delete the oldest, least used, biggest objects first.
        # The newer object is less frequent than the older, so it goes.
        rows = [
            (0, 1, b'0', 1),
            (1, 1, b'0', 0),
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()

        self.db.trim_to_size(
            1,
            ()
        )
        self.assertEqual(dict(self.db.oid_to_tid), {0: 1})

    def test_trim_to_size_removes_biggest_first(self):
        # We delete the oldest, least used, biggest objects first.
        # The newer object is bigger, so it goes
        rows = [
            (0, 1, b'0', 0),
            (1, 1, b'00', 0),
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()

        self.db.trim_to_size(
            1,
            ()
        )
        self.assertEqual(dict(self.db.oid_to_tid), {0: 1})

    def test_total_state_len_with_embedded_nulls(self):
        # automatic text-infinity breaks LONGETH()
        # when there are embedded nul characters.
        # https://github.com/zodb/relstorage/issues/317
        #
        # In case the differences depend on the type of the parameter,
        # (they don't, not exactly)
        # we explicitly test bytes, unicode, and native
        # strings. What they do depend on is the version of Python.
        # In Python 2, the type and length of the five rows are
        #     text(1), text(1), text(1), text(3), text(3)
        # In Python 3, the type and length are
        #     blob(3), text(1), text(1), text(3), blob(3)
        rows = [
            (0, 1, b'1\x003', 0),
            (1, 1, u'2\x003', 0),
            (2, 2, '3\x003', 0),
            (3, 3, '303', 0),
            (4, 3, b'403', 0),
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()

        self.assertEqual(sum(len(row[2]) for row in rows),
                         15)

        self.assertEqual(self.db.total_state_len,
                         sum(len(row[2]) for row in rows))

@unittest.skipIf(not local_database.SUPPORTS_UPSERT,
                 "Requires upserts")
class UpsertUpdateTests(UpdateTests):
    USE_UPSERT = True


class MultiConnectionTests(TestCase):

    timeout = 0

    def setUp(self):
        import tempfile
        import shutil
        self.options = MockOptionsWithMemoryDB()
        tempdir = tempfile.mkdtemp('.rstest')
        self.addCleanup(shutil.rmtree, tempdir, True)
        self.options.cache_local_dir = tempdir
        self.connection = self.connect()
        self.addCleanup(self.connection.close)
        self.db = self._makeOne()
        self.addCleanup(self.db.close)

    def connect(self):
        return sqlite_connect(
            self.options, "pfx-ignored",
            timeout=self.timeout
        )

    def tearDown(self):
        # Be sure we can commit; this can be an issue on PyPy
        self.connection.commit()
        self.db.close()

    def _makeOne(self, conn=None):
        return Database.from_connection(
            conn or self.connection,
        )

    def test_delete_oids_other_open_transaction(self):
        rows = [
            (0, 1, 0, b'0', 0),
            (1, 1, 0, b'0', 0),
        ]
        self.db.store_temp(rows)
        self.db.move_from_temp()

        invalid_oids = range(1, 5000)

        conn2 = self.connect()
        try:
            cur2 = conn2.cursor()
            cur2.execute("BEGIN")
            cur2.execute('DELETE FROM object_state WHERE zoid = 0')

            count = self.db.remove_invalid_persistent_oids(invalid_oids)
            self.assertEqual(count, -1)
            cur2.close()
        finally:
            conn2.close()
