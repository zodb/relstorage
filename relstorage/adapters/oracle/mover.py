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


from relstorage.adapters.interfaces import IObjectMover

from zope.interface import implementer
import os
import sys

from relstorage._compat import xrange

from ..mover import metricmethod_sampled

from .scriptrunner import format_to_named
from ..mover import AbstractObjectMover

def _to_oracle_ordered(query_tuple):
    # Replace %s with :1, :2, etc
    assert len(query_tuple) == 2
    return format_to_named(query_tuple[0]), format_to_named(query_tuple[1])


@implementer(IObjectMover)
class OracleObjectMover(AbstractObjectMover):

    # This is assigned to by the adapter.
    inputsizes = None

    _move_from_temp_hp_insert_query = format_to_named(AbstractObjectMover._move_from_temp_hp_insert_query)
    _move_from_temp_hf_insert_query = format_to_named(AbstractObjectMover._move_from_temp_hf_insert_query)
    _move_from_temp_copy_blob_query = format_to_named(AbstractObjectMover._move_from_temp_copy_blob_query)

    _load_current_queries = _to_oracle_ordered(AbstractObjectMover._load_current_queries)

    @metricmethod_sampled
    def load_current(self, cursor, oid):
        stmt = self._load_current_query
        return self.runner.run_lob_stmt(
            cursor, stmt, (oid,), default=(None, None))


    _load_revision_query = format_to_named(AbstractObjectMover._load_revision_query)

    @metricmethod_sampled
    def load_revision(self, cursor, oid, tid):
        stmt = self._load_revision_query
        (state,) = self.runner.run_lob_stmt(
            cursor, stmt, (oid, tid), default=(None,))
        return state


    _exists_queries = _to_oracle_ordered(AbstractObjectMover._exists_queries)

    @metricmethod_sampled
    def exists(self, cursor, oid):
        stmt = self._exists_query
        cursor.execute(stmt, (oid,))
        for _row in cursor:
            return True
        return False

    @metricmethod_sampled
    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        stmt = """
        SELECT state, tid
        FROM object_state
        WHERE zoid = :oid
            AND tid = (
                SELECT MAX(tid)
                FROM object_state
                WHERE zoid = :oid
                    AND tid < :tid
            )
        """
        return self.runner.run_lob_stmt(
            cursor, stmt, {'oid': oid, 'tid': tid}, default=(None, None))

    @metricmethod_sampled
    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT MIN(tid)
        FROM object_state
        WHERE zoid = :1
            AND tid > :2
        """
        cursor.execute(stmt, (oid, tid))
        rows = cursor.fetchall()
        if rows:
            # XXX: If we can use rowcount here, we can combine
            # with superclass.
            assert len(rows) == 1
            return rows[0][0]
        else:
            return None

    # no store connection initialization needed for Oracle
    def on_store_opened(self, cursor, restart=False):
        pass

    @metricmethod_sampled
    def store_temp(self, cursor, batcher, oid, prev_tid, data):
        """Store an object in the temporary table."""
        md5sum = self._compute_md5sum(data)

        size = len(data)
        if size <= 2000:
            # Send data inline for speed.  Oracle docs say maximum size
            # of a RAW is 2000 bytes.
            stmt = "BEGIN relstorage_op.store_temp(:1, :2, :3, :4); END;"
            batcher.add_array_op(
                stmt,
                'oid prev_tid md5sum rawdata',
                (oid, prev_tid, md5sum, data),
                rowkey=oid,
                size=size,
            )
        else:
            # Send data as a BLOB
            row = {
                'oid': oid,
                'prev_tid': prev_tid,
                'md5sum': md5sum,
                'blobdata': data,
            }
            batcher.insert_into(
                "temp_store (zoid, prev_tid, md5, state)",
                ":oid, :prev_tid, :md5sum, :blobdata",
                row,
                rowkey=oid,
                size=size,
            )

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        md5sum = self._compute_md5sum(data)

        size = len(data) if data is not None else 0

        if size <= 2000:
            # Send data inline for speed.  Oracle docs say maximum size
            # of a RAW is 2000 bytes.
            if self.keep_history:
                stmt = "BEGIN relstorage_op.restore(:1, :2, :3, :4); END;"
                batcher.add_array_op(
                    stmt,
                    'oid tid md5sum rawdata',
                    (oid, tid, md5sum, data),
                    rowkey=(oid, tid),
                    size=size,
                )
            else:
                stmt = "BEGIN relstorage_op.restore(:1, :2, :3); END;"
                batcher.add_array_op(
                    stmt,
                    'oid tid rawdata',
                    (oid, tid, data),
                    rowkey=(oid, tid),
                    size=size,
                )

        else:
            # Send as a BLOB
            if self.keep_history:
                row = {
                    'oid': oid,
                    'tid': tid,
                    'md5sum': md5sum,
                    'state_size': size,
                    'blobdata': data,
                }
                row_schema = """
                    :oid, :tid,
                    COALESCE((SELECT tid
                              FROM current_object
                              WHERE zoid = :oid), 0),
                    :md5sum, :state_size, :blobdata
                """
                batcher.insert_into(
                    "object_state (zoid, tid, prev_tid, md5, state_size, state)",
                    row_schema,
                    row,
                    rowkey=(oid, tid),
                    size=size,
                )
            else:
                batcher.delete_from('object_state', zoid=oid)
                if data:
                    row = {
                        'oid': oid,
                        'tid': tid,
                        'state_size': size,
                        'blobdata': data,
                    }
                    batcher.insert_into(
                        "object_state (zoid, tid, state_size, state)",
                        ":oid, :tid, :state_size, :blobdata",
                        row,
                        rowkey=oid,
                        size=size,
                    )

    @metricmethod_sampled
    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """
        md5sum = self._compute_md5sum(data)

        stmt = """
        UPDATE temp_store SET
            prev_tid = :prev_tid,
            md5 = :md5sum,
            state = :blobdata
        WHERE zoid = :oid
        """
        cursor.setinputsizes(blobdata=self.inputsizes['blobdata']) # pylint:disable=unsubscriptable-object
        cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
                       md5sum=md5sum, blobdata=self.Binary(data))



    _update_current_insert_query = format_to_named(AbstractObjectMover._update_current_insert_query)
    _update_current_update_query = format_to_named(AbstractObjectMover._update_current_update_query)
    _update_current_update_query = _update_current_update_query.replace('ORDER BY zoid', '')

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        stmt = """
        SELECT chunk
        FROM blob_chunk
        WHERE zoid = :1
            AND tid = :2
        ORDER BY chunk_num
        """

        f = None
        bytecount = 0
        # Current versions of cx_Oracle only support offsets up
        # to sys.maxint or 4GB, whichever comes first.
        maxsize = min(sys.maxsize, 1 << 32)
        try:
            cursor.execute(stmt, (oid, tid))
            while True:
                try:
                    blob, = cursor.fetchone()
                except TypeError:
                    # No more chunks.  Note: if there are no chunks at
                    # all, then this method should not write a file.
                    break

                if f is None:
                    f = open(filename, 'wb')
                # round off the chunk-size to be a multiple of the oracle
                # blob chunk size to maximize performance
                read_chunk_size = int(
                    max(
                        round(1.0 * self.blob_chunk_size / blob.getchunksize()),
                        1)
                    * blob.getchunksize())
                offset = 1 # Oracle still uses 1-based indexing.
                reader = iter(lambda: blob.read(offset, read_chunk_size), b'')
                for read_chunk in reader:
                    f.write(read_chunk)
                    bytecount += len(read_chunk)
                    offset += len(read_chunk)
                    if offset > maxsize:
                        # We have already read the maximum we can store
                        # so we can assume we are done. If we do not break
                        # off here, cx_Oracle will throw an overflow
                        # exception anyway.
                        break
        except:
            if f is not None:
                f.close()
                os.remove(filename)
            raise

        if f is not None:
            f.close()
        return bytecount

    # Current versions of cx_Oracle only support offsets up
    # to sys.maxint or 4GB, whichever comes first. We divide up our
    # upload into chunks within this limit.
    oracle_blob_chunk_maxsize = min(sys.maxsize, 1 << 32)

    @metricmethod_sampled
    def upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        # pylint:disable=too-many-locals
        if tid is not None:
            if self.keep_history:
                delete_stmt = """
                DELETE FROM blob_chunk
                WHERE zoid = :1 AND tid = :2
                """
                cursor.execute(delete_stmt, (oid, tid))
            else:
                delete_stmt = "DELETE FROM blob_chunk WHERE zoid = :1"
                cursor.execute(delete_stmt, (oid,))

            use_tid = True
            insert_stmt = """
            INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
            VALUES (:oid, :tid, :chunk_num, empty_blob())
            """
            select_stmt = """
            SELECT chunk FROM blob_chunk
            WHERE zoid=:oid AND tid=:tid AND chunk_num=:chunk_num
            """

        else:
            use_tid = False
            delete_stmt = "DELETE FROM temp_blob_chunk WHERE zoid = :1"
            cursor.execute(delete_stmt, (oid,))

            insert_stmt = """
            INSERT INTO temp_blob_chunk (zoid, chunk_num, chunk)
            VALUES (:oid, :chunk_num, empty_blob())
            """
            select_stmt = """
            SELECT chunk FROM temp_blob_chunk
            WHERE zoid=:oid AND chunk_num=:chunk_num
            """

        f = open(filename, 'rb')
        maxsize = self.oracle_blob_chunk_maxsize
        try:
            chunk_num = 0
            while True:
                blob = None
                params = dict(oid=oid, chunk_num=chunk_num)
                if use_tid:
                    params['tid'] = tid
                cursor.execute(insert_stmt, params)
                cursor.execute(select_stmt, params)
                blob, = cursor.fetchone()
                blob.open()
                write_chunk_size = int(
                    max(
                        round(1.0 * self.blob_chunk_size / blob.getchunksize()),
                        1)
                    * blob.getchunksize())
                offset = 1 # Oracle still uses 1-based indexing.
                for _i in xrange(maxsize // write_chunk_size):
                    write_chunk = f.read(write_chunk_size)
                    if not blob.write(write_chunk, offset):
                        # EOF.
                        return
                    offset += len(write_chunk)
                if blob is not None and blob.isopen():
                    blob.close()
                chunk_num += 1
        finally:
            f.close()
            if blob is not None and blob.isopen():
                blob.close()
