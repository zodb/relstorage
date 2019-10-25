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
from __future__ import absolute_import, print_function

import io
import os
import struct

from zope.interface import implementer


from ..interfaces import IObjectMover
from ..mover import AbstractObjectMover
from ..mover import RowBatcherStoreTemps
from ..mover import metricmethod_sampled

class PostgreSQLRowBatcherStoreTemps(RowBatcherStoreTemps):
    generic_suffix = """
    ON CONFLICT (zoid) DO UPDATE
    SET state = excluded.state,
        prev_tid = excluded.prev_tid,
        md5 = excluded.md5
    """


@implementer(IObjectMover)
class PostgreSQLObjectMover(AbstractObjectMover):

    @metricmethod_sampled
    def on_store_opened(self, cursor, restart=False):
        """Create the temporary tables for storing objects"""
        # Note that the md5 column is not used if self.keep_history == False.
        # Ideally we wouldn't execute any of these on a restart, but
        # I've seen an issue with temp_store apparently going missing on pg8000.
        #
        # In testing, using 'DELETE ROWS' is faster than using 'DROP TABLE':
        # 1230/840ms for zodbshootout add/update with DROP vs 983/681ms.
        # The theory was that maybe a vacuum or analyze would be needed after
        # DELETE ROWS and not DROP TABLE, but that didn't seem to be true (it's possible
        # an ANALYZE would still be helpful before using the temp table, but we
        # haven't benchmarked that).
        if not restart:
            temp_store_table_tmpl = """
            CREATE TEMPORARY TABLE IF NOT EXISTS {NAME} (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                prev_tid    BIGINT NOT NULL,
                md5         CHAR(32),
                state       BYTEA
            ) ON COMMIT DELETE ROWS;
            """
            ddl_stmts = [
                temp_store_table_tmpl.format(NAME='temp_store'),
                temp_store_table_tmpl.format(NAME='temp_store_replacements'),
                """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_blob_chunk (
                    zoid        BIGINT NOT NULL,
                    chunk_num   BIGINT NOT NULL,
                    chunk       OID,
                    PRIMARY KEY (zoid, chunk_num)
                ) ON COMMIT DELETE ROWS;
                """,
                """
                -- This trigger removes blobs that get replaced before being
                -- moved to blob_chunk.  Note that it is never called when
                -- the temp_blob_chunk table is being dropped or truncated.
                CREATE TRIGGER temp_blob_chunk_delete
                    BEFORE DELETE ON temp_blob_chunk
                    FOR EACH ROW
                    EXECUTE PROCEDURE temp_blob_chunk_delete_trigger();
                """,
            ]

            for stmt in ddl_stmts:
                cursor.execute(stmt)
            cursor.connection.commit()

            if not self.driver.supports_copy:
                batcher = PostgreSQLRowBatcherStoreTemps(self.keep_history,
                                                         self.driver.Binary,
                                                         self.make_batcher)
                self.replace_temps = batcher.replace_temps
                self.store_temps = batcher.store_temps

        AbstractObjectMover.on_store_opened(self, cursor, restart)

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        if self.keep_history:
            suffix = """
            ON CONFLICT (zoid, tid) DO UPDATE SET
                tid = excluded.tid,
                prev_tid = excluded.prev_tid,
                md5 = excluded.md5,
                state_size = excluded.state_size,
                state = excluded.state
            """
        else:
            suffix = """
            ON CONFLICT (zoid) DO UPDATE SET
                tid = excluded.tid,
                state_size = excluded.state_size,
                state = excluded.state
            """
        self._generic_restore(batcher, oid, tid, data,
                              command='INSERT', suffix=suffix)

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        stmt = """
        SELECT chunk
        FROM blob_chunk
        WHERE zoid = %s
            AND tid = %s
        ORDER BY chunk_num
        """
        # Beginning in RelStorage 3, we no longer chunk blobs.
        # All chunks were collapsed into one as part of the migration.
        bytecount = 0
        cursor.execute(stmt, (oid, tid))
        rows = cursor.fetchall()
        assert len(rows) == 1
        loid, = rows[0]

        blob = cursor.connection.lobject(loid, 'rb')
        # Use the native psycopg2 blob export functionality
        blob.export(filename)
        blob.close()
        bytecount = os.path.getsize(filename)
        return bytecount

    @metricmethod_sampled
    def upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        # pylint:disable=too-many-branches,too-many-locals
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
            VALUES (%(oid)s, %(tid)s, %(chunk_num)s, %(loid)s)
            """

        else:
            use_tid = False
            delete_stmt = "DELETE FROM temp_blob_chunk WHERE zoid = %s"
            cursor.execute(delete_stmt, (oid,))

            insert_stmt = """
            INSERT INTO temp_blob_chunk (zoid, chunk_num, chunk)
            VALUES (%(oid)s, %(chunk_num)s, %(loid)s)
            """

        # Since we only run on 9.6 and above, the sizes of large objects
        # are allowed to exceed 2GB (int_32). The server is already chunking
        # large objects internally by itself into 4KB pages, so there's no
        # advantage to us also adding a layer of chunking.
        #
        # As long as we keep our usage simple, that's fine. Only
        # blob.seek(), blob.truncate() and blob.tell() have a need to
        # use a specific 64-bit function. `export()` and `import()`
        # (called implicitly by creating the lobject with a local
        # filename in psycopg2) work with small fixed buffers (8KB) and
        # don't care about filesize or offset; they just need the
        # `open` and `read` syscalls to handle 64-bit files (and don't
        # they have to for Python to handle 64-bit files?)
        #
        # psycopg2 explicitly uses the 64 family of functions;
        # psycopg2cffi does *not* but if it's built on 64-bit
        # platform, that's fine. pg8000 uses the SQL interfaces, not
        # the libpq interfaces, and that's also fine. Since we don't use
        # any of the functions that need 64-bit aware, none of that should be an
        # issue.

        # Create and upload the blob, getting a large object identifier.
        blob = cursor.connection.lobject(0, 'wb', 0, filename)
        blob.close()

        # Now put it into our blob_chunk table.
        params = dict(oid=oid, chunk_num=0, loid=blob.oid)
        if use_tid:
            params['tid'] = tid
        cursor.execute(insert_stmt, params)


    def _do_store_temps(self, cursor, state_oid_tid_iter, table_name):
        # History-preserving storages need the md5 to compare states.
        # We could calculate that on the server using pgcrypto, if its
        # available. Or we could just compare directly, instead of comparing
        # md5; that's fast on PostgreSQL.
        if state_oid_tid_iter:
            buf = TempStoreCopyBuffer(table_name,
                                      state_oid_tid_iter,
                                      self._compute_md5sum if self.keep_history else None)
            cursor.copy_expert(buf.COPY_COMMAND, buf)

    @metricmethod_sampled
    def store_temps(self, cursor, state_oid_tid_iter):
        self._do_store_temps(cursor, state_oid_tid_iter, 'temp_store')

    @metricmethod_sampled
    def replace_temps(self, cursor, state_oid_tid_iter):
        # Upload and then replace. We *could* go right into the table
        # if we first deleted but that would require either iterating twice
        # and/or bufferring all the state data in memory. If it's small that's ok,
        # but it could be large.
        self._do_store_temps(cursor, state_oid_tid_iter, 'temp_store_replacements')
        # TODO: Prepare this query.
        cursor.execute(
            """
            UPDATE temp_store
            SET prev_tid = r.prev_tid,
                md5 = r.md5,
                state = r.state
            FROM temp_store_replacements r
            WHERE temp_store.zoid = r.zoid
            """
        )


class TempStoreCopyBuffer(io.BufferedIOBase):
    """
    A binary file-like object for putting data into
    ``temp_store``.
    """

    # pg8000 uses readinto(); psycopg2 uses read().

    COPY_COMMAND_TMPL = "COPY {NAME} (zoid, prev_tid, md5, state) FROM STDIN WITH (FORMAT binary)"

    def __init__(self, table, state_oid_tid_iterable, digester):
        super(TempStoreCopyBuffer, self).__init__()
        self.COPY_COMMAND = self.COPY_COMMAND_TMPL.format(NAME=table)
        self.state_oid_tid_iterable = state_oid_tid_iterable
        self._iter = iter(state_oid_tid_iterable)
        self._digester = digester
        if digester and bytes is not str:
            # On Python 3, this outputs a str, but our protocol needs bytes
            self._digester = lambda s: digester(s).encode("ascii")
        if self._digester:
            self._read_tuple = self._read_one_tuple_md5
        else:
            self._read_tuple = self._read_one_tuple_no_md5

        self._done = False
        self._header = self.HEADER
        self._buffer = bytearray(8192)


    SIGNATURE = b'PGCOPY\n\xff\r\n\0'
    FLAGS = struct.pack("!i", 0)
    EXTENSION_LEN = struct.pack("!i", 0)
    HEADER = SIGNATURE + FLAGS + EXTENSION_LEN
    # All tuples begin with their length in 16 signed bits, which is the same for all tuples
    # (zoid, prev_tid, md5, state)
    _common = "!hiqiqi"
    WITH_SUM = struct.Struct(_common + "32si")
    NO_SUM = struct.Struct(_common + "i")
    # Each column in the tuple is a 32-bit length (-1
    # for NULL), followed by exactly that many bytes of data.
    # Each column datum is written in binary format; for character
    # fields (like md5) that turns out to be a direct dump of the ascii.
    # For BIGINT fields, that's an 8-byte big-endian encoding
    # For BYTEA fields, it's just the raw data
    # Finally, the trailer is a tuple size of -1
    TRAILER = struct.pack("!h", -1)

    def read(self, size=-1):
        # We don't handle "read everything in one go".
        # assert size is not None and size > 0
        if self._done:
            return b''

        if len(self._buffer) < size:
            self._buffer.extend(bytearray(size - len(self._buffer)))

        count = self.readinto(self._buffer)
        if not count:
            return b''
        return bytes(self._buffer)

    def readinto(self, buf):
        # We basically ignore the size of the buffer,
        # writing more into it if we need to.
        if self._done:
            return 0

        requested = len(buf)
        # bytearray.clear() is only in Python 3
        del buf[:]

        buf.extend(self._header)
        self._header = b''

        while len(buf) < requested:
            try:
                self._read_tuple(buf)
            except StopIteration:
                buf.extend(self.TRAILER)
                self._done = True
                break

        return len(buf)

    def __len__(self):
        return len(self.state_oid_tid_iterable)

    def _read_one_tuple_md5(self,
                            buf,
                            _pack_into=WITH_SUM.pack_into,
                            _header_size=WITH_SUM.size,
                            _blank_header=bytearray(WITH_SUM.size)):

        data, oid_int, tid_int = next(self._iter)
        len_data = len(data)
        md5 = self._digester(data)
        offset = len(buf)
        buf.extend(_blank_header)
        _pack_into(
            buf, offset,
            4,
            8, oid_int,
            8, tid_int,
            32, md5,
            len_data
        )
        buf.extend(data)

    def _read_one_tuple_no_md5(self,
                               buf,
                               _pack_into=NO_SUM.pack_into,
                               _header_size=NO_SUM.size,
                               _blank_header=bytearray(NO_SUM.size)):
        data, oid_int, tid_int = next(self._iter)
        len_data = len(data)
        offset = len(buf)
        buf.extend(_blank_header)
        _pack_into(
            buf, offset,
            4,
            8, oid_int,
            8, tid_int,
            -1,
            len_data
        )
        buf.extend(data)
