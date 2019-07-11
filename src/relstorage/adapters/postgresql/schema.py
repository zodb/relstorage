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
"""
Database schema installers
"""
from __future__ import absolute_import

import re

from zope.interface import implementer

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller

logger = __import__('logging').getLogger(__name__)

# Versions of the installed stored procedures. Change these when
# the corresponding code changes.
postgresql_proc_version = '3.0B'


postgresql_procedures = [
    """
CREATE OR REPLACE FUNCTION blob_chunk_delete_trigger() RETURNS TRIGGER
AS $blob_chunk_delete_trigger$
    -- Version: %(postgresql_proc_version)s
    -- Unlink large object data file after blob_chunk row deletion
    DECLARE
        cnt integer;
    BEGIN
        SELECT count(*) into cnt FROM blob_chunk WHERE chunk=OLD.chunk;
        IF (cnt = 1) THEN
            -- Last reference to this oid, unlink
            PERFORM lo_unlink(OLD.chunk);
        END IF;
        RETURN OLD;
    END;
$blob_chunk_delete_trigger$ LANGUAGE plpgsql;
""" % globals(),
    """
CREATE OR REPLACE FUNCTION temp_blob_chunk_delete_trigger() RETURNS TRIGGER
AS $temp_blob_chunk_delete_trigger$
    -- Version: %(postgresql_proc_version)s
    -- Unlink large object data file after temp_blob_chunk row deletion
    DECLARE
        cnt integer;
    BEGIN
        SELECT count(*) into cnt FROM blob_chunk WHERE chunk=OLD.chunk;
        IF (cnt = 0) THEN
            -- No more references to this oid, unlink
            PERFORM lo_unlink(OLD.chunk);
        END IF;
        RETURN OLD;
    END;
$temp_blob_chunk_delete_trigger$ LANGUAGE plpgsql;
""" % globals(),
    """
CREATE OR REPLACE FUNCTION merge_blob_chunks()
  RETURNS VOID
AS $$
    -- Version: %(postgresql_proc_version)s
    -- Merge blob chunks into one entirely on the server.
DECLARE
  masterfd integer;
  masterloid oid;
  chunkfd integer;
  rec record;
  buf bytea;
BEGIN
  -- In history-free mode, zoid and chunk_num is the key.
  -- in history-preserving mode, zoid/tid/chunk_num is the key.
  -- for chunks, zoid and tid must always be used to look up the master.
  FOR rec IN SELECT zoid, tid, chunk_num, chunk
    FROM blob_chunk WHERE chunk_num > 0
    ORDER BY zoid, chunk_num LOOP
    -- Find the master and open it.
    SELECT chunk
    INTO STRICT masterloid
    FROM blob_chunk
    WHERE zoid = rec.zoid and tid = rec.tid AND chunk_num = 0;

    -- open master for writing
    SELECT lo_open(masterloid, 131072) -- 0x20000, AKA INV_WRITE
    INTO STRICT masterfd;

    -- position at the end
    PERFORM lo_lseek(masterfd, 0, 2);
    -- open the child for reading
    SELECT lo_open(rec.chunk, 262144) -- 0x40000 AKA INV_READ
    INTO STRICT chunkfd;
    -- copy the data
    LOOP
      SELECT loread(chunkfd, 8192)
      INTO buf;

      EXIT WHEN LENGTH(buf) = 0;

      PERFORM lowrite(masterfd, buf);
    END LOOP;
    -- close the files
    PERFORM lo_close(chunkfd);
    PERFORM lo_close(masterfd);

  END LOOP;

  -- Finally, remove the redundant chunks. Our trigger
  -- takes care of removing the large objects.
  DELETE FROM blob_chunk WHERE chunk_num > 0;

END;
$$
LANGUAGE plpgsql;
    """ % globals()
]


@implementer(ISchemaInstaller)
class PostgreSQLSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'postgresql'

    def __init__(self, options, connmanager, runner, locker):
        self.options = options
        super(PostgreSQLSchemaInstaller, self).__init__(
            connmanager, runner, options.keep_history)
        self.locker = locker

    def get_database_name(self, cursor):
        cursor.execute("SELECT current_database()")
        for (name,) in cursor:
            if isinstance(name, str):
                return name
            if hasattr(name, 'encode'):
                # OK, name must be a unicode object, and we must be on Py2.
                # pg8000 does this.
                assert isinstance(name, unicode) # pylint:disable=undefined-variable
                return name.encode('ascii')
            assert isinstance(name, bytes)
            return name.decode('ascii')

    def _prepare_with_connection(self, conn, cursor):
        super(PostgreSQLSchemaInstaller, self)._prepare_with_connection(conn, cursor)

        if not self.all_procedures_installed(cursor):
            self.install_procedures(cursor)
            if not self.all_procedures_installed(cursor):
                raise AssertionError(
                    "Could not get version information after "
                    "installing the stored procedures.")

        triggers = self.list_triggers(cursor)
        if 'blob_chunk_delete' not in triggers:
            self.install_triggers(cursor)

        # Do we need to merge blob chunks?
        if not self.options.shared_blob_dir:
            cursor.execute('SELECT chunk_num FROM blob_chunk WHERE chunk_num > 0 LIMIT 1')
            if cursor.fetchone():
                logger.info("Merging blob chunks on the server.")
                cursor.execute("SELECT merge_blob_chunks()")
                # If we've done our job right, any blobs cached on
                # disk are still perfectly valid.

    def list_tables(self, cursor):
        cursor.execute("SELECT tablename FROM pg_tables")
        return [name if isinstance(name, str) else name.decode('ascii')
                for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        cursor.execute("SELECT relname FROM pg_class WHERE relkind = 'S'")
        return [name for (name,) in cursor.fetchall()]

    def list_languages(self, cursor):
        cursor.execute("SELECT lanname FROM pg_catalog.pg_language")
        return [name for (name,) in cursor.fetchall()]

    def install_languages(self, cursor):
        if 'plpgsql' not in self.list_languages(cursor):
            cursor.execute("CREATE LANGUAGE plpgsql")

    def list_procedures(self, cursor):
        """Returns {procedure name: version}.  version may be None."""
        stmt = """
        SELECT proname, prosrc
        FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_proc p ON pronamespace = n.oid
        JOIN pg_catalog.pg_type t ON prorettype = t.oid
        WHERE nspname = 'public'
        """
        cursor.execute(stmt)
        res = {}
        for (name, text) in cursor.fetchall():
            name = self._metadata_to_native_str(name)
            text = self._metadata_to_native_str(text)

            version = None
            match = re.search(r'Version:\s*([0-9a-zA-Z.]+)', text)
            if match is not None:
                version = match.group(1)
            res[name.lower()] = version
        return res

    def all_procedures_installed(self, cursor):
        """Check whether all required stored procedures are installed.

        Returns True only if all required procedures are installed and
        up to date.
        """
        expect = [
            'blob_chunk_delete_trigger',
            'temp_blob_chunk_delete_trigger',
            'merge_blob_chunks',
        ]
        current_procs = self.list_procedures(cursor)
        for proc in expect:
            if current_procs.get(proc) != postgresql_proc_version:
                return False
        return True

    def install_procedures(self, cursor):
        """Install the stored procedures"""
        self.install_languages(cursor)
        for stmt in postgresql_procedures:
            cursor.execute(stmt)

    def list_triggers(self, cursor):
        cursor.execute("SELECT tgname FROM pg_trigger")
        return [name for (name,) in cursor]

    def install_triggers(self, cursor):
        stmt = """
        CREATE TRIGGER blob_chunk_delete
            BEFORE DELETE ON blob_chunk
            FOR EACH ROW
            EXECUTE PROCEDURE blob_chunk_delete_trigger()
        """
        cursor.execute(stmt)

    def drop_all(self):
        def callback(_conn, cursor):
            if 'blob_chunk' in self.list_tables(cursor):
                # Trigger deletion of blob OIDs.
                cursor.execute("DELETE FROM blob_chunk")
        self.connmanager.open_and_call(callback)
        super(PostgreSQLSchemaInstaller, self).drop_all()

    def _create_pack_lock(self, cursor):
        return

    def _create_new_oid(self, cursor):
        stmt = """
        CREATE SEQUENCE IF NOT EXISTS zoid_seq;
        """
        self.runner.run_script(cursor, stmt)


    CREATE_PACK_OBJECT_IX_TMPL = """
    CREATE INDEX pack_object_keep_false ON pack_object (zoid)
        WHERE keep = false;
    CREATE INDEX pack_object_keep_true ON pack_object (visited)
        WHERE keep = true;
    """

    def _reset_oid(self, cursor):
        stmt = "ALTER SEQUENCE zoid_seq RESTART WITH 1;"
        self.runner.run_script(cursor, stmt)

    # Use the fast, semi-transactional way to truncate tables. It's
    # not MVCC safe, but "TRUNCATE is transaction-safe with respect to
    # the data in the tables: the truncation will be safely rolled
    # back if the surrounding transaction does not commit."
    _zap_all_tbl_stmt = 'TRUNCATE TABLE %s CASCADE'

    def _before_zap_all_tables(self, cursor, tables, slow=False):
        super(PostgreSQLSchemaInstaller, self)._before_zap_all_tables(cursor, tables, slow)
        if not slow and 'blob_chunk' in tables:
            # If we're going to be truncating, it's important to
            # remove the large objects through lo_unlink. We have a
            # trigger that does that, but only for DELETE.
            # The `vacuumlo` command cleans up any that might have been
            # missed.

            # This unfortunately results in returning a row for each
            # object unlinked, but it should still be faster than
            # running a DELETE and firing the trigger for each row.
            cursor.execute("""
            SELECT lo_unlink(t.chunk)
            FROM
            (SELECT DISTINCT chunk FROM blob_chunk)
            AS t
            """)
