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
from __future__ import absolute_import
from __future__ import print_function

import os
from hashlib import md5

from zope.interface import implementer

from .._compat import OID_TID_MAP_TYPE
from .._compat import metricmethod_sampled
from ._util import noop_when_history_free
from ._util import query_property as _query_property
from ._util import DatabaseHelpersMixin
from .._compat import ABC
from .batch import RowBatcher
from .interfaces import IObjectMover
from .schema import Schema
from .sql import it
from .sql.schema import ColumnExpression

objects = Schema.all_current_object_state
object_state = Schema.object_state


def _compute_md5sum(_self, data):
    if data is None:
        return None
    return md5(data).hexdigest()



@implementer(IObjectMover)
class AbstractObjectMover(DatabaseHelpersMixin, ABC):

    def __init__(self, database_driver, options, runner=None,
                 version_detector=None,
                 batcher_factory=RowBatcher):
        """
        :param database_driver: The `IDBDriver` in use.
        """
        self.driver = database_driver
        self.keep_history = options.keep_history
        self.blob_chunk_size = options.blob_chunk_size
        self.runner = runner

        self.version_detector = version_detector
        self.make_batcher = batcher_factory

    _compute_md5sum = noop_when_history_free(_compute_md5sum)

    _load_current_query = objects.select(
        objects.c.state, objects.c.tid
    ).where(
        objects.c.zoid == objects.orderedbindparam()
    ).prepared()

    @metricmethod_sampled
    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        stmt = self._load_current_query
        stmt.execute(cursor, (oid,))
        # Note that we cannot rely on cursor.rowcount being
        # a valid indicator. The DB-API doesn't require it, and
        # some implementations, like MySQL Connector/Python are
        # unbuffered by default and can't provide it.
        row = cursor.fetchone()
        if row:
            state, tid = row
            state = self.driver.binary_column_as_state_type(state)
            # If it's None, the object's creation has been
            # undone.
            return state, tid

        return None, None

    _load_currents_queries = (
        (('zoid', 'state', 'tid'), 'current_object JOIN object_state USING(zoid, tid)', 'zoid'),
        (('zoid', 'state', 'tid'), 'object_state', 'zoid'),
    )

    _load_currents_query = _query_property('_load_currents')

    @metricmethod_sampled
    def load_currents(self, cursor, oids):
        """Returns the current (oid, state, tid) for specified object ids."""
        columns, table, filter_column = self._load_currents_query
        binary_column_as_state_type = self.driver.binary_column_as_state_type
        batcher = self.make_batcher(cursor, row_limit=1000)
        rows = batcher.select_from(columns, table, **{filter_column: oids})
        for row in rows:
            oid, state, tid = row
            yield oid, binary_column_as_state_type(state), tid

    _load_revision_query = object_state.select(
        object_state.c.state
    ).where(
        object_state.c.zoid == object_state.orderedbindparam()
    ).and_(
        object_state.c.tid == object_state.orderedbindparam()
    ).prepared()

    @metricmethod_sampled
    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = self._load_revision_query
        stmt.execute(cursor, (oid, tid))
        row = cursor.fetchone()
        if row:
            (state,) = row
            return self.driver.binary_column_as_state_type(state)
        return None

    _exists_query = Schema.all_current_object.select(
        Schema.all_current_object.c.zoid
    ).where(
        Schema.all_current_object.c.zoid == Schema.all_current_object.orderedbindparam()
    )

    @metricmethod_sampled
    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        stmt = self._exists_query
        stmt.execute(cursor, (oid,))
        row = cursor.fetchone()
        return row

    _load_before_query = object_state.select(
        object_state.c.state, object_state.c.tid
    ).where(
        object_state.c.zoid == object_state.orderedbindparam()
    ).and_(
        object_state.c.tid < object_state.orderedbindparam()
    ).order_by(
        object_state.c.tid, "DESC"
    ).limit(
        1
    ).prepared()


    @metricmethod_sampled
    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """

        self._load_before_query.execute(cursor, (oid, tid))
        row = cursor.fetchone()
        if row:
            state, tid = row
            state = self.driver.binary_column_as_state_type(state)
            # None in state means The object's creation has been undone
            return state, tid
        return None, None

    _get_tid_after_query = object_state.select(
        object_state.c.tid
    ).where(
        object_state.c.zoid == object_state.orderedbindparam()
    ).and_(
        object_state.c.tid > object_state.orderedbindparam()
    ).order_by(
        object_state.c.tid, "ASC"
    ).limit(
        1
    ).prepared()


    @metricmethod_sampled
    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        self._get_tid_after_query.execute(cursor, (oid, tid))
        row = cursor.fetchone()
        if row:
            return row[0]

    _current_object_tids_queries = (
        (('zoid', 'tid'), 'current_object', 'zoid'),
        (('zoid', 'tid'), 'object_state', 'zoid'),
    )

    _current_object_tids_query = _query_property('_current_object_tids')

    _current_object_tids_map_type = OID_TID_MAP_TYPE

    @metricmethod_sampled
    def current_object_tids(self, cursor, oids):
        """Returns the current {oid: tid} for specified object ids."""
        res = self._current_object_tids_map_type()
        columns, table, filter_column = self._current_object_tids_query
        batcher = self.make_batcher(cursor)
        rows = batcher.select_from(columns, table, **{filter_column: oids})
        res = self._current_object_tids_map_type(list(rows))

        return res


    def on_store_opened(self, cursor, restart=False):
        """
        Hook for subclasses.
        """

    def on_load_opened(self, cursor, restart=False):
        """
        Hook for subclasses.
        """

    _store_temp_query = Schema.temp_store.upsert(
        it.c.zoid,
        it.c.prev_tid,
        it.c.md5,
        it.c.state
    ).on_conflict(
        it.c.zoid
    ).do_update(
        it.c.prev_tid,
        it.c.md5,
        it.c.state
    )

    @metricmethod_sampled
    def store_temps(self, cursor, state_oid_tid_iter):
        """
        Uses the cursor's ``executemany`` method to store temporary
        objects.

        If there is a more optimal way to implement putting objects in
        the database, please do so.

            - On SQLite, ``executemany`` is implemnted in a C looping
              over the provided iterator. Which it turns out is
              exactly what the normal ``execute`` method also does (it
              just uses a one-row iterator). So ``executemany`` that
              saves substantial setup overhead dealing with sqlite's
              prepared statements.

            - On Postgresql, we use COPY for this (unless we're using
              the 'gevent psycopg2' driver; it's the only thing that
              doesn't support COPY). None of the supported PostgreSQL
              drivers have a good ``executemany`` method, so they
              should fall back to using our own RowBatcher.

            - On Oracle, we use the RowBatcher with a combination of
              bulk array operations and direct inserts.

            - On MySQL, the preferred driver (mysqlclient) has a
              decent implementation of executemany for INSERT or
              REPLACE (basically an optimized form of what our
              RowBatcher does). That implementation is shared with
              PyMySQL as well, but it must be a simple INSERT
              statement matching a regular expression. Note that it
              has a bug though: it can't handle an iterator that's
              empty.
        """
        query = self._store_temp_query
        do_md5 = self._compute_md5sum
        Binary = self.driver.Binary

        query.executemany(
            cursor,
            (
                (oid_int, tid_int, do_md5(data), Binary(data))
                for (data, oid_int, tid_int)
                in state_oid_tid_iter
            )
        )

    @metricmethod_sampled
    def replace_temps(self, cursor, state_oid_tid_iter):
        """
        Assumes that ``store_temps`` is using an upsert query and simply calls
        that method.

        The same comments apply. In particular,
        MySQLclient won't optimize an UPDATE in the same way it does an INSERT.
        """
        self.store_temps(cursor, state_oid_tid_iter)

    @metricmethod_sampled
    def _generic_restore(self, batcher, oid, tid, data,
                         command, suffix):
        """
        Store an object directly, without conflict detection.

        Used for copying transactions into this database.

        Either the *command* or the *suffix* must be capable of
        handling conflicts in a single query. For example,
        ``command='INSERT OR REPLACE'``
        or ``command='INSERT', suffix='ON CONFLICT (zoid) DO...``
        """
        if data is not None:
            encoded = self.driver.Binary(data)
            size = len(data)
        else:
            encoded = None
            size = 0

        if self.keep_history:
            # We can record deletion/un-creation via a null state.
            md5sum = self._compute_md5sum(data)
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
                suffix=suffix
            )
        else:
            # history free can only delete the entire record.
            if data:
                batcher.insert_into(
                    "object_state (zoid, tid, state_size, state)",
                    "%s, %s, %s, %s",
                    (oid, tid, size, encoded),
                    rowkey=oid,
                    size=size,
                    command=command,
                    suffix=suffix
                )
            else:
                batcher.delete_from('object_state', zoid=oid)

    def restore(self, cursor, batcher, oid, tid, data):
        raise NotImplementedError()

    _detect_conflict_query = Schema.temp_store.inner_join(
        Schema.all_current_object_state
    ).using(
        Schema.all_current_object_state.c.zoid
    ).select(
        Schema.temp_store.c.zoid,
        Schema.all_current_object_state.c.tid,
        Schema.temp_store.c.prev_tid,
        Schema.all_current_object_state.c.state,
    ).where(
        # Some databases may be able to benefit from prev_tid <> 0, but it depends
        # on their ability to make use of indexes
        Schema.all_current_object_state.c.tid != Schema.temp_store.c.prev_tid
    ).prepared()

    @metricmethod_sampled
    def detect_conflict(self, cursor):
        self._detect_conflict_query.execute(cursor)
        # Note that we're not transforming the state into
        # bytes; it doesn't seem to be needed here, even with sqlite3
        # on Python 2 (where it is a buffer).
        rows = cursor.fetchall()
        return rows

    # Subclasses may override any of these queries if there is a
    # more optimal form.
    _move_from_temp_hp_insert_query = Schema.object_state.insert(
    ).from_select(
        (Schema.object_state.c.zoid,
         Schema.object_state.c.tid,
         Schema.object_state.c.prev_tid,
         Schema.object_state.c.md5,
         Schema.object_state.c.state_size,
         Schema.object_state.c.state),
        Schema.temp_store.select(
            Schema.temp_store.c.zoid,
            Schema.temp_store.orderedbindparam(),
            Schema.temp_store.c.prev_tid,
            Schema.temp_store.c.md5,
            'COALESCE(LENGTH(state), 0)',
            Schema.temp_store.c.state
        ).order_by(
            Schema.temp_store.c.zoid
        )
    ).prepared()

    _move_from_temp_hf_upsert_query = Schema.object_state.upsert(
        it.c.zoid,
        it.c.tid,
        it.c.state_size,
        it.c.state
    ).from_select(
        (Schema.temp_store.c.zoid,
         Schema.object_state.c.tid, # correct column for typing
         Schema.object_state.c.state_size,
         Schema.temp_store.c.state),
        Schema.temp_store.select(
            it.c.zoid,
            ColumnExpression(it.orderedbindparam()).aliased('tid'),
            ColumnExpression('COALESCE(LENGTH(state), 0)').aliased('state_size'),
            it.c.state
        ).order_by(
            it.c.zoid
        )
    ).on_conflict(
        it.c.zoid
    ).do_update(
        it.c.state,
        it.c.tid,
        it.c.state_size
    ).prepared()

    _move_from_temp_copy_blob_query = Schema.blob_chunk.insert(
    ).from_select(
        (Schema.blob_chunk.c.zoid,
         Schema.blob_chunk.c.tid,
         Schema.blob_chunk.c.chunk_num,
         Schema.blob_chunk.c.chunk),
        Schema.temp_blob_chunk.select(
            Schema.temp_blob_chunk.c.zoid,
            Schema.temp_blob_chunk.orderedbindparam(),
            Schema.temp_blob_chunk.c.chunk_num,
            Schema.temp_blob_chunk.c.chunk
        )
    ).prepared()


    _move_from_temp_hf_delete_blob_chunk_query = """
    DELETE FROM blob_chunk
    WHERE zoid IN (SELECT zoid FROM temp_store)
    """


    def _move_from_temp_object_state(self, cursor, tid):
        """
        Called for history-free databases.

        Should replace all entries in object_state with the same zoid
        from temp_store.

        This implementation is in two steps, first deleting from
        ``object_state`` with :attr:`_move_from_temp_hf_delete_query`,
        and then copying from ``temp_store`` using
        :attr:`_move_from_temp_hf_insert_query`.

        If a subclass can do this in a single step with an ``UPSERT``,
        it should set :attr:`_move_from_temp_hf_delete_query` to a
        false value.

        Recall that the queries that touch ``current_object`` and
        ``object_state`` need to be certain the order they use (by
        ``zoid``) to avoid deadlocks.

        Blobs are handled separately.
        """
        stmt = self._move_from_temp_hf_upsert_query
        __traceback_info__ = stmt
        stmt.execute(cursor, (tid,))


    @metricmethod_sampled
    def move_from_temp(self, cursor, tid, txn_has_blobs):
        """
        Move the temporarily stored objects to permanent storage.
        """
        if self.keep_history:
            stmt = self._move_from_temp_hp_insert_query
            __traceback_info__ = stmt
            stmt.execute(cursor, (tid,))
        else:
            self._move_from_temp_object_state(cursor, tid)

            if txn_has_blobs:
                # If we can require storages to have an UPSERT (mysql and
                # postgres do), then can we remove the DELETE?
                # Answer: probably not. What if the blob shrunk and we
                # have fewer chunks than we used to?
                stmt = self._move_from_temp_hf_delete_blob_chunk_query
                cursor.execute(stmt)

        if txn_has_blobs:
            stmt = self._move_from_temp_copy_blob_query
            __traceabck_info__ = stmt
            stmt.execute(cursor, (tid,))


    # Insert and update current objects.
    # Note that to avoid deadlocks, it is incredibly important
    # to order the updates in OID order.
    _update_current_upsert_query = Schema.current_object.upsert(
        it.c.zoid, it.c.tid
    ).from_select(
        (it.c.zoid, it.c.tid),
        Schema.object_state.select(
            it.c.zoid, Schema.object_state.c.tid # correct column for typing
        ).where(
            it.c.tid == it.orderedbindparam()
        ).order_by(it.c.zoid)
    ).on_conflict(
        it.c.zoid
    ).do_update(
        it.c.tid
    ).prepared()

    @noop_when_history_free
    @metricmethod_sampled
    def update_current(self, cursor, tid):
        """
        Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        stmt = self._update_current_upsert_query
        stmt.execute(cursor, (tid,))

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        stmt = """
        SELECT chunk
        FROM blob_chunk
        WHERE zoid = %s
            AND tid = %s
            AND chunk_num = %s
        """

        f = None
        bytecount = 0
        try:
            chunk_num = 0
            while True:
                cursor.execute(stmt, (oid, tid, chunk_num))
                rows = list(cursor)
                if rows:
                    assert len(rows) == 1
                    chunk = rows[0][0]
                else:
                    # No more chunks.  Note: if there are no chunks at
                    # all, then this method should not write a file.
                    break

                if f is None:
                    f = open(filename, 'wb')

                f.write(chunk)
                bytecount += len(chunk)
                chunk_num += 1
        except:
            if f is not None:
                f.close()
                os.remove(filename)
            raise

        if f is not None:
            f.close()
        return bytecount

    _upload_blob_uses_chunks = True

    def _upload_blob_clear_old_blob(self, cursor, oid, tid):
        if tid is not None:
            if self.keep_history:
                delete_stmt = """
                DELETE FROM blob_chunk
                WHERE zoid = %s AND tid = %s
                """
                cursor.execute(delete_stmt, (oid, tid))
            else:
                delete_stmt = "DELETE FROM blob_chunk WHERE zoid = %s"
                cursor.execute(delete_stmt, (oid,))

            use_tid = True
            insert_stmt = """
            INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
            VALUES (%s, %s, %s, %s)
            """
        else:
            use_tid = False
            delete_stmt = "DELETE FROM temp_blob_chunk WHERE zoid = %s"
            cursor.execute(delete_stmt, (oid,))

            insert_stmt = """
            INSERT INTO temp_blob_chunk (zoid, chunk_num, chunk)
            VALUES (%s, %s, %s)
            """

        return insert_stmt, use_tid

    def _upload_blob_read_chunks(self, cursor, oid, tid, filename,
                                 use_chunks, insert_stmt, use_tid):
        Binary = self.driver.Binary
        with open(filename, 'rb') as f:
            chunk_num = 0
            while True:
                chunk = f.read(self.blob_chunk_size) if use_chunks else f.read()
                if not chunk and chunk_num > 0:
                    # EOF.  Note that we always write at least one
                    # chunk, even if the blob file is empty.
                    break
                if use_tid:
                    params = (oid, tid, chunk_num, Binary(chunk))
                else:
                    params = (oid, chunk_num, Binary(chunk))
                cursor.execute(insert_stmt, params)
                chunk_num += 1

    @metricmethod_sampled
    def upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        insert_stmt, use_tid = self._upload_blob_clear_old_blob(cursor, oid, tid)
        self._upload_blob_read_chunks(
            cursor, oid, tid, filename,
            self._upload_blob_uses_chunks, insert_stmt, use_tid
        )


class RowBatcherStoreTemps(object):
    """
    A helper class to implement ``store_temps`` using a RowBatcher.
    You must provide an implementation of
    :meth:`store_temp_into_batcher` and it must be an upsert. The
    :meth:`generic_store_temp_into_batcher` method can be used to help
    with this.
    """

    def __init__(self, keep_history, binary, batcher_factory=RowBatcher):
        self.make_batcher = batcher_factory
        self.keep_history = keep_history
        self.binary = binary

    _compute_md5sum = noop_when_history_free(_compute_md5sum)

    @metricmethod_sampled
    def store_temps(self, cursor, state_oid_tid_iter):
        store_temp = self.store_temp_into_batcher
        batcher = self.make_batcher(cursor) # Default row limit
        for data, oid_int, tid_int in state_oid_tid_iter:
            store_temp(batcher, oid_int, tid_int, data)
        batcher.flush()

    replace_temps = store_temps

    # The _generic methods allow for UPSERTs, at least on MySQL
    # and PostgreSQL. Previously, MySQL used `command='REPLACE'`
    # for an UPSERT; now it uses a suffix 'ON DUPLICATE KEY UPDATE ...'.
    # PostgreSQL uses a suffix 'ON CONFLICT (...) UPDATE ...'.

    generic_command = 'INSERT'
    generic_suffix = ''

    def generic_store_temp_into_batcher(self, batcher, oid, prev_tid, data):
        md5sum = self._compute_md5sum(data)
        command = self.generic_command
        suffix = self.generic_suffix
        # TODO: Now that we guarantee not to feed duplicates here, drop
        # the conflict handling.
        if command == 'INSERT' and not suffix:
            batcher.delete_from('temp_store', zoid=oid)
        batcher.insert_into(
            "temp_store (zoid, prev_tid, md5, state)",
            batcher.row_schema_of_length(4),
            (oid, prev_tid, md5sum, self.binary(data)),
            rowkey=oid,
            size=len(data) + 32,
            command=command,
            suffix=suffix
        )

    store_temp_into_batcher = generic_store_temp_into_batcher
