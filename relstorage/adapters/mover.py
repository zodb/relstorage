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
from relstorage.adapters.batch import MySQLRowBatcher
from relstorage.adapters.batch import OracleRowBatcher
from relstorage.adapters.batch import PostgreSQLRowBatcher
from relstorage.adapters.interfaces import IObjectMover
from relstorage.iter import fetchmany
from zope.interface import implementer
import os
import sys
from hashlib import md5


from relstorage._compat import xrange
from relstorage._compat import db_binary_to_bytes

def compute_md5sum(data):
    if data is not None:
        return md5(data).hexdigest()
    else:
        # George Bailey object
        return None


metricmethod_sampled = Metric(method=True, rate=0.1)


@implementer(IObjectMover)
class ObjectMover(object):

    _method_names = (
        'load_current',
        'load_revision',
        'exists',
        'load_before',
        'get_object_tid_after',
        'current_object_tids',
        'on_store_opened',
        'make_batcher',
        'store_temp',
        'restore',
        'detect_conflict',
        'replace_temp',
        'move_from_temp',
        'update_current',
        'download_blob',
        'upload_blob',
    )

    def __init__(self, database_type, options, runner=None,
                 Binary=None, inputsizes=None, version_detector=None):
        # The inputsizes parameter is for Oracle only.
        self.database_type = database_type
        self.keep_history = options.keep_history
        self.blob_chunk_size = options.blob_chunk_size
        self.runner = runner
        self.Binary = Binary
        self.inputsizes = inputsizes
        self.version_detector = version_detector

        for method_name in self._method_names:
            method = getattr(self, '%s_%s' % (database_type, method_name))
            setattr(self, method_name, method)




    @metricmethod_sampled
    def postgresql_load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        if self.keep_history:
            stmt = """
            SELECT state, tid
            FROM current_object
                JOIN object_state USING(zoid, tid)
            WHERE zoid = %s
            """
        else:
            stmt = """
            SELECT state, tid
            FROM object_state
            WHERE zoid = %s
            """
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

    @metricmethod_sampled
    def mysql_load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        if self.keep_history:
            stmt = """
            SELECT state, tid
            FROM current_object
                JOIN object_state USING(zoid, tid)
            WHERE zoid = %s
            """
        else:
            stmt = """
            SELECT state, tid
            FROM object_state
            WHERE zoid = %s
            """
        cursor.execute(stmt, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()
        else:
            return None, None

    @metricmethod_sampled
    def oracle_load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        if self.keep_history:
            stmt = """
            SELECT state, tid
            FROM current_object
                JOIN object_state USING(zoid, tid)
            WHERE zoid = :1
            """
        else:
            stmt = """
            SELECT state, tid
            FROM object_state
            WHERE zoid = :1
            """
        return self.runner.run_lob_stmt(
            cursor, stmt, (oid,), default=(None, None))




    @metricmethod_sampled
    def postgresql_load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = """
        SELECT state
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state,) = cursor.fetchone()
            return db_binary_to_bytes(state)
        return None

    @metricmethod_sampled
    def mysql_load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = """
        SELECT state
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state,) = cursor.fetchone()
            return state
        return None

    @metricmethod_sampled
    def oracle_load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = """
        SELECT state
        FROM object_state
        WHERE zoid = :1
            AND tid = :2
        """
        (state,) = self.runner.run_lob_stmt(
            cursor, stmt, (oid, tid), default=(None,))
        return state




    @metricmethod_sampled
    def generic_exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        if self.keep_history:
            stmt = "SELECT 1 FROM current_object WHERE zoid = %s"
        else:
            stmt = "SELECT 1 FROM object_state WHERE zoid = %s"
        cursor.execute(stmt, (oid,))
        return cursor.rowcount

    postgresql_exists = generic_exists
    mysql_exists = generic_exists

    @metricmethod_sampled
    def oracle_exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        if self.keep_history:
            stmt = "SELECT 1 FROM current_object WHERE zoid = :1"
        else:
            stmt = "SELECT 1 FROM object_state WHERE zoid = :1"
        cursor.execute(stmt, (oid,))
        for _row in cursor:
            return True
        return False




    @metricmethod_sampled
    def postgresql_load_before(self, cursor, oid, tid):
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
    def mysql_load_before(self, cursor, oid, tid):
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
            return cursor.fetchone()
        else:
            return None, None

    @metricmethod_sampled
    def oracle_load_before(self, cursor, oid, tid):
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
    def generic_get_object_tid_after(self, cursor, oid, tid):
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

    postgresql_get_object_tid_after = generic_get_object_tid_after
    mysql_get_object_tid_after = generic_get_object_tid_after

    @metricmethod_sampled
    def oracle_get_object_tid_after(self, cursor, oid, tid):
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
            assert len(rows) == 1
            return rows[0][0]
        else:
            return None




    @metricmethod_sampled
    def generic_current_object_tids(self, cursor, oids):
        """Returns the current {oid: tid} for specified object ids."""
        res = {}
        if self.keep_history:
            table = 'current_object'
        else:
            table = 'object_state'
        oids = list(oids)
        while oids:
            oid_list = ','.join(str(oid) for oid in oids[:1000])
            del oids[:1000]
            stmt = "SELECT zoid, tid FROM %s WHERE zoid IN (%s)" % (
                table, oid_list)
            cursor.execute(stmt)
            for oid, tid in fetchmany(cursor):
                res[oid] = tid
        return res

    postgresql_current_object_tids = generic_current_object_tids
    mysql_current_object_tids = generic_current_object_tids
    oracle_current_object_tids = generic_current_object_tids




    @metricmethod_sampled
    def postgresql_on_store_opened(self, cursor, restart=False):
        """Create the temporary tables for storing objects"""
        # note that the md5 column is not used if self.keep_history == False.
        stmts = ["""
        CREATE TEMPORARY TABLE temp_store (
            zoid        BIGINT NOT NULL,
            prev_tid    BIGINT NOT NULL,
            md5         CHAR(32),
            state       BYTEA
        ) ON COMMIT DROP;""",
        """
        CREATE UNIQUE INDEX temp_store_zoid ON temp_store (zoid);
        """,
        """
        CREATE TEMPORARY TABLE temp_blob_chunk (
            zoid        BIGINT NOT NULL,
            chunk_num   BIGINT NOT NULL,
            chunk       OID
        ) ON COMMIT DROP;""",
        """
        CREATE UNIQUE INDEX temp_blob_chunk_key
            ON temp_blob_chunk (zoid, chunk_num);""",
        """
        -- This trigger removes blobs that get replaced before being
        -- moved to blob_chunk.  Note that it is never called when
        -- the temp_blob_chunk table is being dropped or truncated.
        CREATE TRIGGER temp_blob_chunk_delete
            BEFORE DELETE ON temp_blob_chunk
            FOR EACH ROW
            EXECUTE PROCEDURE temp_blob_chunk_delete_trigger();
        """,]
        for stmt in stmts:
            cursor.execute(stmt)

    @metricmethod_sampled
    def mysql_on_store_opened(self, cursor, restart=False):
        """Create the temporary table for storing objects"""
        if restart:
            stmt = "DROP TEMPORARY TABLE IF EXISTS temp_store"
            cursor.execute(stmt)
            stmt = "DROP TEMPORARY TABLE IF EXISTS temp_blob_chunk"
            cursor.execute(stmt)

        # note that the md5 column is not used if self.keep_history == False.
        stmt = """
        CREATE TEMPORARY TABLE temp_store (
            zoid        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            prev_tid    BIGINT UNSIGNED NOT NULL,
            md5         CHAR(32),
            state       LONGBLOB
        ) ENGINE MyISAM
        """
        cursor.execute(stmt)

        stmt = """
        CREATE TEMPORARY TABLE temp_blob_chunk (
            zoid        BIGINT UNSIGNED NOT NULL,
            chunk_num   BIGINT UNSIGNED NOT NULL,
                        PRIMARY KEY (zoid, chunk_num),
            chunk       LONGBLOB
        ) ENGINE MyISAM
        """
        cursor.execute(stmt)

    # no store connection initialization needed for Oracle
    oracle_on_store_opened = None




    @metricmethod_sampled
    def postgresql_make_batcher(self, cursor, row_limit):
        return PostgreSQLRowBatcher(cursor, row_limit)

    @metricmethod_sampled
    def mysql_make_batcher(self, cursor, row_limit):
        return MySQLRowBatcher(cursor, row_limit)

    @metricmethod_sampled
    def oracle_make_batcher(self, cursor, row_limit):
        return OracleRowBatcher(cursor, self.inputsizes, row_limit)


    @metricmethod_sampled
    def postgresql_store_temp(self, cursor, batcher, oid, prev_tid, data):
        """Store an object in the temporary table."""
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None
        batcher.delete_from('temp_store', zoid=oid)
        batcher.insert_into(
            "temp_store (zoid, prev_tid, md5, state)",
            "%s, %s, %s, %s",
            (oid, prev_tid, md5sum, self.Binary(data)),
            rowkey=oid,
            size=len(data),
        )

    @metricmethod_sampled
    def mysql_store_temp(self, cursor, batcher, oid, prev_tid, data):
        """Store an object in the temporary table."""
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None
        batcher.insert_into(
            "temp_store (zoid, prev_tid, md5, state)",
            "%s, %s, %s, %s",
            (oid, prev_tid, md5sum, self.Binary(data)),
            rowkey=oid,
            size=len(data),
            command='REPLACE',
        )

    @metricmethod_sampled
    def oracle_store_temp(self, cursor, batcher, oid, prev_tid, data):
        """Store an object in the temporary table."""
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None

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
    def postgresql_restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None

        if data is not None:
            encoded = self.Binary(data)
            size = len(data)
        else:
            encoded = None
            size = 0

        if self.keep_history:
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
            )
        else:
            batcher.delete_from('object_state', zoid=oid)
            if data:
                batcher.insert_into(
                    "object_state (zoid, tid, state_size, state)",
                    "%s, %s, %s, %s",
                    (oid, tid, size, encoded),
                    rowkey=oid,
                    size=size,
                )

    @metricmethod_sampled
    def mysql_restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None

        if data is not None:
            encoded = self.Binary(data)
            size = len(data)
        else:
            encoded = None
            size = 0

        if self.keep_history:
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
                command='REPLACE',
            )
        else:
            if data:
                batcher.insert_into(
                    "object_state (zoid, tid, state_size, state)",
                    "%s, %s, %s, %s",
                    (oid, tid, size, encoded),
                    rowkey=oid,
                    size=size,
                    command='REPLACE',
                )
            else:
                batcher.delete_from('object_state', zoid=oid)

    @metricmethod_sampled
    def oracle_restore(self, cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None

        if data is not None:
            size = len(data)
        else:
            size = 0

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
    def postgresql_detect_conflict(self, cursor):
        """Find all conflicts in the data about to be committed.

        If there is a conflict, returns a sequence of (oid, prev_tid, attempted_prev_tid).
        """
        if self.keep_history:
            stmt = """
            SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid
            FROM temp_store
                JOIN current_object ON (temp_store.zoid = current_object.zoid)
            WHERE temp_store.prev_tid != current_object.tid
            """
        else:
            stmt = """
            SELECT temp_store.zoid, object_state.tid, temp_store.prev_tid
            FROM temp_store
                JOIN object_state ON (temp_store.zoid = object_state.zoid)
            WHERE temp_store.prev_tid != object_state.tid
            """
        cursor.execute(stmt)
        if cursor.rowcount:
            return cursor.fetchall()
        return ()

    @metricmethod_sampled
    def mysql_detect_conflict(self, cursor):
        """Find all conflicts in the data about to be committed.

        If there is a conflict, returns a list of (oid, prev_tid, attempted_prev_tid).
        """
        # Lock in share mode to ensure the data being read is up to date.
        if self.keep_history:
            stmt = """
            SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid
            FROM temp_store
                JOIN current_object ON (temp_store.zoid = current_object.zoid)
            WHERE temp_store.prev_tid != current_object.tid
            LOCK IN SHARE MODE
            """
        else:
            stmt = """
            SELECT temp_store.zoid, object_state.tid, temp_store.prev_tid
            FROM temp_store
                JOIN object_state ON (temp_store.zoid = object_state.zoid)
            WHERE temp_store.prev_tid != object_state.tid
            LOCK IN SHARE MODE
            """
        cursor.execute(stmt)
        if cursor.rowcount:
            return cursor.fetchall()
        return ()

    @metricmethod_sampled
    def oracle_detect_conflict(self, cursor):
        """Find all conflicts in the data about to be committed.

        If there is a conflict, returns a list of (oid, prev_tid, attempted_prev_tid).
        """
        if self.keep_history:
            stmt = """
            SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid
            FROM temp_store
                JOIN current_object ON (temp_store.zoid = current_object.zoid)
            WHERE temp_store.prev_tid != current_object.tid
            """
        else:
            stmt = """
            SELECT temp_store.zoid, object_state.tid, temp_store.prev_tid
            FROM temp_store
                JOIN object_state ON (temp_store.zoid = object_state.zoid)
            WHERE temp_store.prev_tid != object_state.tid
            """
        cursor.execute(stmt)
        if cursor.rowcount:
            return cursor.fetchall()
        return ()


    @metricmethod_sampled
    def postgresql_replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = %s
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, self.Binary(data), oid))

    @metricmethod_sampled
    def mysql_replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = %s
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, self.Binary(data), oid))

    @metricmethod_sampled
    def oracle_replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """
        if self.keep_history:
            md5sum = compute_md5sum(data)
        else:
            md5sum = None
        stmt = """
        UPDATE temp_store SET
            prev_tid = :prev_tid,
            md5 = :md5sum,
            state = :blobdata
        WHERE zoid = :oid
        """
        cursor.setinputsizes(blobdata=self.inputsizes['blobdata'])
        cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
            md5sum=md5sum, blobdata=self.Binary(data))




    @metricmethod_sampled
    def generic_move_from_temp(self, cursor, tid, txn_has_blobs):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """
        if self.keep_history:
            if self.database_type == 'oracle':
                stmt = """
                INSERT INTO object_state
                    (zoid, tid, prev_tid, md5, state_size, state)
                SELECT zoid, :1, prev_tid, md5,
                    COALESCE(LENGTH(state), 0), state
                FROM temp_store
                """
            else:
                stmt = """
                INSERT INTO object_state
                    (zoid, tid, prev_tid, md5, state_size, state)
                SELECT zoid, %s, prev_tid, md5,
                    COALESCE(LENGTH(state), 0), state
                FROM temp_store
                """
            cursor.execute(stmt, (tid,))

        else:
            if self.database_type == 'mysql':
                stmt = """
                REPLACE INTO object_state (zoid, tid, state_size, state)
                SELECT zoid, %s, COALESCE(LENGTH(state), 0), state
                FROM temp_store
                """
                cursor.execute(stmt, (tid,))

            else:
                stmt = """
                DELETE FROM object_state
                WHERE zoid IN (SELECT zoid FROM temp_store)
                """
                cursor.execute(stmt)

                if self.database_type == 'oracle':
                    stmt = """
                    INSERT INTO object_state (zoid, tid, state_size, state)
                    SELECT zoid, :1, COALESCE(LENGTH(state), 0), state
                    FROM temp_store
                    """
                else:
                    stmt = """
                    INSERT INTO object_state (zoid, tid, state_size, state)
                    SELECT zoid, %s, COALESCE(LENGTH(state), 0), state
                    FROM temp_store
                    """
                cursor.execute(stmt, (tid,))

            if txn_has_blobs:
                stmt = """
                DELETE FROM blob_chunk
                WHERE zoid IN (SELECT zoid FROM temp_store)
                """
                cursor.execute(stmt)

        if txn_has_blobs:
            if self.database_type == 'oracle':
                stmt = """
                INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
                SELECT zoid, :1, chunk_num, chunk
                FROM temp_blob_chunk
                """
            else:
                stmt = """
                INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
                SELECT zoid, %s, chunk_num, chunk
                FROM temp_blob_chunk
                """
            cursor.execute(stmt, (tid,))

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in fetchmany(cursor)]

    postgresql_move_from_temp = generic_move_from_temp
    mysql_move_from_temp = generic_move_from_temp
    oracle_move_from_temp = generic_move_from_temp




    @metricmethod_sampled
    def postgresql_update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        if not self.keep_history:
            # nothing needs to be updated
            return

        params = {'tid': tid}
        cursor.execute("""
        -- Insert objects created in this transaction into current_object.
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %(tid)s
            AND prev_tid = 0;""", params)

        cursor.execute("""
        -- Change existing objects.  To avoid deadlocks,
        -- update in OID order.
        UPDATE current_object SET tid = %(tid)s
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = %(tid)s
                AND prev_tid != 0
            ORDER BY zoid
        )
        """, params)

    @metricmethod_sampled
    def mysql_update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        if not self.keep_history:
            # nothing needs to be updated
            return

        cursor.execute("""
        REPLACE INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %s
        """, (tid,))

    @metricmethod_sampled
    def oracle_update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        if not self.keep_history:
            # nothing needs to be updated
            return

        # Insert objects created in this transaction into current_object.
        stmt = """
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = :1
            AND prev_tid = 0
        """
        cursor.execute(stmt, (tid,))

        # Change existing objects.
        stmt = """
        UPDATE current_object SET tid = :1
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = :1
                AND prev_tid != 0
        )
        """
        cursor.execute(stmt, (tid,))




    @metricmethod_sampled
    def postgresql_download_blob(self, cursor, oid, tid, filename):
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

                reader = iter(lambda: blob.read(read_chunk_size), b'')
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

    @metricmethod_sampled
    def mysql_download_blob(self, cursor, oid, tid, filename):
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

    @metricmethod_sampled
    def oracle_download_blob(self, cursor, oid, tid, filename):
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
        maxsize = min(sys.maxsize, 1<<32)
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
                read_chunk_size = int(max(round(
                    1.0 * self.blob_chunk_size / blob.getchunksize()), 1) *
                    blob.getchunksize())
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

    # PostgreSQL < 9.3 only supports up to 2GB of data per BLOB.
    # Even above that, we can only use larger blobs on 64-bit builds.
    postgresql_blob_chunk_maxsize = 1<<31

    @metricmethod_sampled
    def postgresql_upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
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

    @metricmethod_sampled
    def mysql_upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
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

        f = open(filename, 'rb')
        try:
            chunk_num = 0
            while True:
                chunk = f.read(self.blob_chunk_size)
                if not chunk and chunk_num > 0:
                    # EOF.  Note that we always write at least one
                    # chunk, even if the blob file is empty.
                    break
                if use_tid:
                    params = (oid, tid, chunk_num, chunk)
                else:
                    params = (oid, chunk_num, chunk)
                cursor.execute(insert_stmt, params)
                chunk_num += 1
        finally:
            f.close()

    # Current versions of cx_Oracle only support offsets up
    # to sys.maxint or 4GB, whichever comes first. We divide up our
    # upload into chunks within this limit.
    oracle_blob_chunk_maxsize = min(sys.maxsize, 1<<32)

    @metricmethod_sampled
    def oracle_upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
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
