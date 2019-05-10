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
from ..mover import AbstractObjectMover
from relstorage.adapters.interfaces import IObjectMover

from zope.interface import implementer
import os
import functools

from relstorage._compat import xrange

from ..mover import metricmethod_sampled
from .._util import query_property

def to_prepared_queries(name, queries, datatypes=''):
    # Only handles one param
    return [
        'PREPARE ' + name + ' ' + datatypes + ' AS ' + x.replace('%s', '$1')
        for x in queries
    ]

@implementer(IObjectMover)
class PostgreSQLObjectMover(AbstractObjectMover):

    __need_version_check = True

    _prepare_load_current_queries = to_prepared_queries(
        'load_current',
        AbstractObjectMover._load_current_queries,
        '(BIGINT)')

    _prepare_load_current_query = query_property('_prepare_load_current')

    _load_current_query = 'EXECUTE load_current(%s)'

    _prepare_detect_conflict_queries = to_prepared_queries(
        'detect_conflicts',
        AbstractObjectMover._detect_conflict_queries)

    _prepare_detect_conflict_query = query_property('_prepare_detect_conflict')

    _detect_conflict_query = 'EXECUTE detect_conflicts'

    on_load_opened_statement_names = ('_prepare_load_current_query',)
    on_store_opened_statement_names = on_load_opened_statement_names + (
        '_prepare_detect_conflict_query',)

    # Sadly we can't PREPARE this statement; apparently it holds a
    # lock on OBJECT_STATE that interferes with taking the commit lock.
    _move_from_temp_object_state_95_query = """
        INSERT INTO object_state (zoid, tid, state_size, state)
        SELECT zoid, %s, COALESCE(LENGTH(state), 0), state
        FROM temp_store
        ON CONFLICT (zoid) DO UPDATE SET state_size = COALESCE(LENGTH(excluded.state), 0),
                              tid = %s,
                              STATE = excluded.state
    """

    def _move_from_temp_object_state_95(self, cursor, tid):
        stmt = self._move_from_temp_object_state_95_query
        cursor.execute(stmt, (tid, tid))

    @metricmethod_sampled
    def on_store_opened(self, cursor, restart=False):
        """Create the temporary tables for storing objects"""
        # note that the md5 column is not used if self.keep_history == False.
        stmts = [
            """
            CREATE TEMPORARY TABLE temp_store (
                zoid        BIGINT NOT NULL,
                prev_tid    BIGINT NOT NULL,
                md5         CHAR(32),
                state       BYTEA
            ) ON COMMIT DROP;
            """,
            """
            CREATE UNIQUE INDEX temp_store_zoid ON temp_store (zoid);
            """,
            """
            CREATE TEMPORARY TABLE temp_blob_chunk (
                zoid        BIGINT NOT NULL,
                chunk_num   BIGINT NOT NULL,
                chunk       OID
            ) ON COMMIT DROP;
            """,
            """
            CREATE UNIQUE INDEX temp_blob_chunk_key
            ON temp_blob_chunk (zoid, chunk_num);
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
        for stmt in stmts:
            cursor.execute(stmt)

        if self.__need_version_check:
            self.__need_version_check = False
            supports_conflict = self.version_detector.get_version(cursor) >= (9, 5)
            if supports_conflict:
                self._move_from_temp_object_state = self._move_from_temp_object_state_95
                self.store_temp = self._store_temp_95

        AbstractObjectMover.on_store_opened(self, cursor, restart)

    def _store_temp_95(self, _cursor, batcher, oid, prev_tid, data):

        suffix = """
        ON CONFLICT (zoid) DO UPDATE SET state = excluded.state,
                              prev_tid = excluded.prev_tid,
                              md5 = excluded.md5
        """
        self._generic_store_temp(batcher, oid, prev_tid, data, suffix=suffix)

    @metricmethod_sampled
    def store_temp(self, cursor, batcher, oid, prev_tid, data): # pylint:disable=method-hidden
        self._generic_store_temp(batcher, oid, prev_tid, data)

    @metricmethod_sampled
    def restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        self._generic_restore(batcher, oid, tid, data)

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        stmt = """
        SELECT chunk_num, chunk
        FROM blob_chunk
        WHERE zoid = %s
            AND tid = %s
        ORDER BY chunk_num
        """

        f = None
        bytecount = 0
        read_chunk_size = self.blob_chunk_size

        try:
            cursor.execute(stmt, (oid, tid))
            for chunk_num, loid in cursor.fetchall():

                blob = cursor.connection.lobject(loid, 'rb')

                if chunk_num == 0:
                    # Use the native psycopg2 blob export functionality
                    blob.export(filename)
                    blob.close()
                    bytecount = os.path.getsize(filename)
                    continue

                if f is None:
                    f = open(filename, 'ab') # Append, chunk 0 was an export

                reader = iter(functools.partial(blob.read, read_chunk_size), b'')
                for read_chunk in reader:
                    f.write(read_chunk)
                    bytecount += len(read_chunk)
                blob.close()
        except:
            if f is not None:
                f.close()
                os.remove(filename)
            raise

        if f is not None:
            f.close()
        return bytecount

    # PostgreSQL < 9.3 only supports up to 2GB of data per BLOB.
    # Even above that, we can only use larger blobs on 64-bit builds.
    postgresql_blob_chunk_maxsize = 1 << 31

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

        blob = None

        maxsize = self.postgresql_blob_chunk_maxsize
        filesize = os.path.getsize(filename)
        write_chunk_size = self.blob_chunk_size

        if filesize <= maxsize:
            # File is small enough to fit in one chunk, just use
            # psycopg2 native file copy support
            blob = cursor.connection.lobject(0, 'wb', 0, filename)
            blob.close()
            params = dict(oid=oid, chunk_num=0, loid=blob.oid)
            if use_tid:
                params['tid'] = tid
            cursor.execute(insert_stmt, params)
            return

        # We need to divide this up into multiple chunks
        f = open(filename, 'rb')
        try:
            chunk_num = 0
            while True:
                blob = cursor.connection.lobject(0, 'wb')
                params = dict(oid=oid, chunk_num=chunk_num, loid=blob.oid)
                if use_tid:
                    params['tid'] = tid
                cursor.execute(insert_stmt, params)

                for _i in xrange(maxsize // write_chunk_size):
                    write_chunk = f.read(write_chunk_size)
                    if not blob.write(write_chunk):
                        # EOF.
                        return
                if not blob.closed:
                    blob.close()
                chunk_num += 1
        finally:
            f.close()
            if blob is not None and not blob.closed:
                blob.close()


class PG8000ObjectMover(PostgreSQLObjectMover):
    # pg8000 1.10 can't handle prepared statements that take parameters
    # but it doesn't need to because it prepares every statement
    # anyway. https://github.com/mfenniak/pg8000/issues/132

    on_load_opened_statement_names = ()
    on_store_opened_statement_names = ('_prepare_detect_conflict_query',)

    _load_current_query = AbstractObjectMover._load_current_query
