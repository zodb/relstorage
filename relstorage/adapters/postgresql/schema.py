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

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller
from zope.interface import implementer
import re

# Versions of the installed stored procedures. Change these when
# the corresponding code changes.
postgresql_proc_version = '1.5B'


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
]


@implementer(ISchemaInstaller)
class PostgreSQLSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'postgresql'

    # Use the fast, semi-transactional way to truncate tables. It's
    # not MVCC safe, but "TRUNCATE is transaction-safe with respect to
    # the data in the tables: the truncation will be safely rolled
    # back if the surrounding transaction does not commit."
    _zap_all_tbl_stmt = 'TRUNCATE TABLE %s CASCADE'

    def __init__(self, connmanager, runner, locker, keep_history):
        super(PostgreSQLSchemaInstaller, self).__init__(
            connmanager, runner, keep_history)
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

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(_conn, cursor):
            tables = self.list_tables(cursor)
            if 'object_state' not in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)

            if not self.all_procedures_installed(cursor):
                self.install_procedures(cursor)
                if not self.all_procedures_installed(cursor):
                    raise AssertionError(
                        "Could not get version information after "
                        "installing the stored procedures.")

            triggers = self.list_triggers(cursor)
            if 'blob_chunk_delete' not in triggers:
                self.install_triggers(cursor)

        self.connmanager.open_and_call(callback)

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

    def _create_commit_lock(self, cursor):
        stmt = "CREATE TABLE commit_lock ();"
        self.runner.run_script(cursor, stmt)

    def _create_pack_lock(self, cursor):
        return

    def _create_transaction(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE transaction (
                tid         BIGINT NOT NULL PRIMARY KEY,
                packed      BOOLEAN NOT NULL DEFAULT FALSE,
                empty       BOOLEAN NOT NULL DEFAULT FALSE,
                username    BYTEA NOT NULL,
                description BYTEA NOT NULL,
                extension   BYTEA
            );
            """
            self.runner.run_script(cursor, stmt)

    def _create_new_oid(self, cursor):
        stmt = """
        CREATE SEQUENCE zoid_seq;
        """
        self.runner.run_script(cursor, stmt)


    def _create_object_state(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_state (
                zoid        BIGINT NOT NULL,
                tid         BIGINT NOT NULL REFERENCES transaction
                            CHECK (tid > 0),
                            PRIMARY KEY (zoid, tid),
                prev_tid    BIGINT NOT NULL REFERENCES transaction,
                md5         CHAR(32),
                state_size  BIGINT NOT NULL CHECK (state_size >= 0),
                state       BYTEA
            );
            CREATE INDEX object_state_tid ON object_state (tid);
            CREATE INDEX object_state_prev_tid ON object_state (prev_tid);
            """
        else:
            stmt = """
            CREATE TABLE object_state (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                tid         BIGINT NOT NULL CHECK (tid > 0),
                state_size  BIGINT NOT NULL CHECK (state_size >= 0),
                state       BYTEA
            );
            CREATE INDEX object_state_tid ON object_state (tid);
            """

        self.runner.run_script(cursor, stmt)

    def _create_blob_chunk(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE blob_chunk (
                zoid        BIGINT NOT NULL,
                tid         BIGINT NOT NULL,
                chunk_num   BIGINT NOT NULL,
                            PRIMARY KEY (zoid, tid, chunk_num),
                chunk       OID NOT NULL
            );
            CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
            CREATE INDEX blob_chunk_loid ON blob_chunk (chunk);
            ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid, tid)
                REFERENCES object_state (zoid, tid)
                ON DELETE CASCADE;
            """
        else:
            stmt = """
            CREATE TABLE blob_chunk (
                zoid        BIGINT NOT NULL,
                chunk_num   BIGINT NOT NULL,
                            PRIMARY KEY (zoid, chunk_num),
                tid         BIGINT NOT NULL,
                chunk       OID NOT NULL
            );
            CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
            CREATE INDEX blob_chunk_loid ON blob_chunk (chunk);
            ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid)
                REFERENCES object_state (zoid)
                ON DELETE CASCADE;
            """

        self.runner.run_script(cursor, stmt)

    def _create_current_object(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE current_object (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                tid         BIGINT NOT NULL,
                            FOREIGN KEY (zoid, tid)
                                REFERENCES object_state (zoid, tid)
            );
            CREATE INDEX current_object_tid ON current_object (tid);
            """
            self.runner.run_script(cursor, stmt)

    def _create_object_ref(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_ref (
                zoid        BIGINT NOT NULL,
                tid         BIGINT NOT NULL,
                to_zoid     BIGINT NOT NULL,
                PRIMARY KEY (tid, zoid, to_zoid)
            );
            """
        else:
            stmt = """
            CREATE TABLE object_ref (
                zoid        BIGINT NOT NULL,
                to_zoid     BIGINT NOT NULL,
                tid         BIGINT NOT NULL,
                PRIMARY KEY (zoid, to_zoid)
            );
            """

        self.runner.run_script(cursor, stmt)

    def _create_object_refs_added(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_refs_added (
                tid         BIGINT NOT NULL PRIMARY KEY
            );
            """
        else:
            stmt = """
            CREATE TABLE object_refs_added (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                tid         BIGINT NOT NULL
            );
            """
        self.runner.run_script(cursor, stmt)

    def _create_pack_object(self, cursor):
        stmt = """
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        );
        CREATE INDEX pack_object_keep_false ON pack_object (zoid)
            WHERE keep = false;
        CREATE INDEX pack_object_keep_true ON pack_object (visited)
            WHERE keep = true;

        """
        self.runner.run_script(cursor, stmt)

    def _create_pack_state(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE pack_state (
                tid         BIGINT NOT NULL,
                zoid        BIGINT NOT NULL,
                PRIMARY KEY (tid, zoid)
            );
            """
            self.runner.run_script(cursor, stmt)

    def _create_pack_state_tid(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE pack_state_tid (
                tid         BIGINT NOT NULL PRIMARY KEY
            );
            """
            self.runner.run_script(cursor, stmt)

    def _reset_oid(self, cursor):
        stmt = "ALTER SEQUENCE zoid_seq RESTART WITH 1;"
        self.runner.run_script(cursor, stmt)
