##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
"""IObjectMover implementation.
"""

from perfmetrics import Metric
from .batch import RowBatcher
from relstorage.adapters.interfaces import IObjectMover
from relstorage.iter import fetchmany
from zope.interface import implementer
from hashlib import md5

from relstorage._compat import db_binary_to_bytes

def compute_md5sum(data):
    if data is not None:
        return md5(data).hexdigest()
    else:
        # George Bailey object
        return None


metricmethod_sampled = Metric(method=True, rate=0.1)

class _Lazy(object):

    def __init__(self, func, name=None):
        self.func = func
        self.name = name or func.__name__

    def __get__(self, inst, klazz):
        if inst is None:
            return self

        value = self.func(inst)
        inst.__dict__[self.name] = value
        return value

def _query_property(base_name):
    def prop(inst):
        queries = getattr(inst, base_name + '_queries')
        return queries[0] if inst.keep_history else queries[1]

    return _Lazy(prop, base_name + '_query')

@implementer(IObjectMover)
class AbstractObjectMover(object):

    def __init__(self, database_type, options, runner=None,
                 Binary=None, version_detector=None,
                 batcher_factory=RowBatcher):
        self.database_type = database_type
        self.keep_history = options.keep_history
        self.blob_chunk_size = options.blob_chunk_size
        self.runner = runner
        self.Binary = Binary
        self.version_detector = version_detector
        self.make_batcher = batcher_factory

        if self.keep_history:
            self._compute_md5sum = compute_md5sum
        else:
            self._compute_md5sum = lambda arg: None

    _load_current_queries = (
        """
        SELECT state, tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = %s
        """,
        """
        SELECT state, tid
        FROM object_state
        WHERE zoid = %s
        """)

    _load_current_query = _query_property('_load_current')

    @metricmethod_sampled
    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        stmt = self._load_current_query

        cursor.execute(stmt, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            state, tid = cursor.fetchone()
            state = db_binary_to_bytes(state)
            # If it's None, the object's creation has been
            # undone.
            return state, tid
        else:
            return None, None

    _load_revision_query = """
        SELECT state
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """

    @metricmethod_sampled
    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = self._load_revision_query
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state,) = cursor.fetchone()
            return db_binary_to_bytes(state)
        return None

    _exists_queries = (
        "SELECT 1 FROM current_object WHERE zoid = %s",
        "SELECT 1 FROM object_state WHERE zoid = %s"
    )

    _exists_query = _query_property('_exists')

    @metricmethod_sampled
    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        stmt = self._exists_query
        cursor.execute(stmt, (oid,))
        return cursor.rowcount

    @metricmethod_sampled
    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        stmt = """
        SELECT state, tid
        FROM object_state
        WHERE zoid = %s
            AND tid < %s
        ORDER BY tid DESC
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            state, tid = cursor.fetchone()
            state = db_binary_to_bytes(state)
            # None in state means The object's creation has been undone
            return state, tid
        else:
            return None, None


    @metricmethod_sampled
    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT tid
        FROM object_state
        WHERE zoid = %s
            AND tid > %s
        ORDER BY tid
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()[0]
        else:
            return None

    # NOTE: These are not database param escapes, they are Python
    # escapes, so they shouldn't be translated to :1, etc.
    _current_object_tids_queries = (
        "SELECT zoid, tid FROM current_object WHERE zoid IN (%s)",
        "SELECT zoid, tid FROM object_state WHERE zoid IN (%s)"
    )

    _current_object_tids_query = _query_property('_current_object_tids')

    @metricmethod_sampled
    def current_object_tids(self, cursor, oids):
        """Returns the current {oid: tid} for specified object ids."""
        res = {}
        _stmt = self._current_object_tids_query
        oids = list(oids)
        while oids:
            # XXX: Dangerous (SQL injection)! And probably slow. Can we do better?
            oid_list = ','.join(str(oid) for oid in oids[:1000])
            del oids[:1000]
            stmt = _stmt % (oid_list,)
            cursor.execute(stmt)
            for oid, tid in fetchmany(cursor):
                res[oid] = tid
        return res

    def on_store_opened(self, cursor, restart=False):
        raise NotImplementedError()

    def _generic_store_temp(self, batcher, oid, prev_tid, data, command='INSERT',):
        md5sum = self._compute_md5sum(data)

        if command == 'INSERT':
            batcher.delete_from('temp_store', zoid=oid)
        batcher.insert_into(
            "temp_store (zoid, prev_tid, md5, state)",
            "%s, %s, %s, %s",
            (oid, prev_tid, md5sum, self.Binary(data)),
            rowkey=oid,
            size=len(data),
            command=command,
        )

    def store_temp(self, cursor, batcher, oid, prev_tid, data):
        raise NotImplementedError()

    @metricmethod_sampled
    def _generic_restore(self, batcher, oid, tid, data, command='INSERT'):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        md5sum = self._compute_md5sum(data)

        if data is not None:
            encoded = self.Binary(data)
            size = len(data)
        else:
            encoded = None
            size = 0

        if self.keep_history:
            if command == 'INSERT':
                batcher.delete_from("object_state", zoid=oid, tid=tid)
            row_schema = """
                %s, %s,
                COALESCE((SELECT tid FROM current_object WHERE zoid = %s), 0),
                %s, %s, %s
            """
            batcher.insert_into(
                "object_state (zoid, tid, prev_tid, md5, state_size, state)",
                row_schema,
                (oid, tid, oid, md5sum, size, encoded),
                rowkey=(oid, tid),
                size=size,
                command=command,
            )
        else:
            if data:
                if command == 'INSERT':
                    batcher.delete_from('object_state', zoid=oid)
                batcher.insert_into(
                    "object_state (zoid, tid, state_size, state)",
                    "%s, %s, %s, %s",
                    (oid, tid, size, encoded),
                    rowkey=oid,
                    size=size,
                    command=command,
                )
            else:
                batcher.delete_from('object_state', zoid=oid)

    def restore(self, cursor, batcher, oid, tid, data):
        raise NotImplementedError()

    _detect_conflict_queries = (
        """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid
        FROM temp_store
                JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        """,
        """
        SELECT temp_store.zoid, object_state.tid, temp_store.prev_tid
        FROM temp_store
                JOIN object_state ON (temp_store.zoid = object_state.zoid)
        WHERE temp_store.prev_tid != object_state.tid
        """
    )

    _detect_conflict_query = _query_property('_detect_conflict')

    @metricmethod_sampled
    def detect_conflict(self, cursor):
        """Find all conflicts in the data about to be committed.

        If there is a conflict, returns a sequence of (oid, prev_tid, attempted_prev_tid).
        """
        stmt = self._detect_conflict_query
        cursor.execute(stmt)
        rows = cursor.fetchall()
        return rows

    @metricmethod_sampled
    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """
        md5sum = self._compute_md5sum(data)

        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = %s
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, self.Binary(data), oid))

    _move_from_temp_hp_insert_query = """
    INSERT INTO object_state
      (zoid, tid, prev_tid, md5, state_size, state)
    SELECT zoid, %s, prev_tid, md5,
      COALESCE(LENGTH(state), 0), state
    FROM temp_store
    """

    _move_from_temp_hf_insert_query = """
    INSERT INTO object_state (zoid, tid, state_size, state)
    SELECT zoid, %s, COALESCE(LENGTH(state), 0), state
    FROM temp_store
    """

    _move_from_temp_copy_blob_query = """
    INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
    SELECT zoid, %s, chunk_num, chunk
    FROM temp_blob_chunk
    """

    def _move_from_temp_object_state(self, cursor, tid):
        """
        Called for history-free databases.

        Should replace all entries in object_state with the
        same zoid from temp_store.
        """

        stmt = """
        DELETE FROM object_state
        WHERE zoid IN (SELECT zoid FROM temp_store)
        """
        cursor.execute(stmt)

        stmt = self._move_from_temp_hf_insert_query
        cursor.execute(stmt, (tid,))


    @metricmethod_sampled
    def move_from_temp(self, cursor, tid, txn_has_blobs):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """

        if self.keep_history:
            stmt = self._move_from_temp_hp_insert_query
            cursor.execute(stmt, (tid,))
        else:
            self._move_from_temp_object_state(cursor, tid)

            if txn_has_blobs:
                stmt = """
                DELETE FROM blob_chunk
                WHERE zoid IN (SELECT zoid FROM temp_store)
                """
                cursor.execute(stmt)

        if txn_has_blobs:
            stmt = self._move_from_temp_copy_blob_query
            cursor.execute(stmt, (tid,))

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in fetchmany(cursor)]

    _update_current_insert_query = """
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %s
            AND prev_tid = 0"""

    _update_current_update_query = """
        UPDATE current_object SET tid = %s
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = %s
                AND prev_tid != 0
            ORDER BY zoid)
        """

    @metricmethod_sampled
    def update_current(self, cursor, tid): # pylint:disable=method-hidden
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        if not self.keep_history:
            # nothing needs to be updated
            # Can elide this check in the future.
            self.update_current = lambda cursor, tid: None
            return

        stmt = self._update_current_insert_query
        cursor.execute(stmt, (tid,))

        # Change existing objects.  To avoid deadlocks,
        # update in OID order.
        stmt = self._update_current_update_query
        cursor.execute(stmt, (tid, tid))

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        raise NotImplementedError()

    def upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        raise NotImplementedError()
