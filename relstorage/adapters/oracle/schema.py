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
"""Database schema installers
"""
from __future__ import absolute_import

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller

from zope.interface import implementer
import re

# Versions of the installed stored procedures. Change these when
# the corresponding code changes.
oracle_package_version = '1.5A'

oracle_history_preserving_package = """
CREATE OR REPLACE PACKAGE relstorage_op AS
    TYPE numlist IS TABLE OF NUMBER(20) INDEX BY BINARY_INTEGER;
    TYPE md5list IS TABLE OF VARCHAR2(32) INDEX BY BINARY_INTEGER;
    TYPE statelist IS TABLE OF RAW(2000) INDEX BY BINARY_INTEGER;
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist);
    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        md5s IN md5list,
        states IN statelist);
END relstorage_op;
/

CREATE OR REPLACE PACKAGE BODY relstorage_op AS
/* Version: %(oracle_package_version)s */
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM temp_store WHERE zoid = zoids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO temp_store (zoid, prev_tid, md5, state) VALUES
            (zoids(indx), prev_tids(indx), md5s(indx), states(indx));
    END store_temp;

    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        md5s IN md5list,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM object_state
            WHERE zoid = zoids(indx)
                AND tid = tids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO object_state
                (zoid, tid, prev_tid, md5, state_size, state)
                VALUES (zoids(indx), tids(indx),
                    COALESCE((SELECT tid
                        FROM current_object
                        WHERE zoid = zoids(indx)), 0),
                md5s(indx), COALESCE(LENGTH(states(indx)), 0), states(indx));
    END restore;
END relstorage_op;
/
""" % globals()


oracle_history_free_package = """
CREATE OR REPLACE PACKAGE relstorage_op AS
    TYPE numlist IS TABLE OF NUMBER(20) INDEX BY BINARY_INTEGER;
    TYPE md5list IS TABLE OF VARCHAR2(32) INDEX BY BINARY_INTEGER;
    TYPE statelist IS TABLE OF RAW(2000) INDEX BY BINARY_INTEGER;
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist);
    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        states IN statelist);
END relstorage_op;
/

CREATE OR REPLACE PACKAGE BODY relstorage_op AS
/* Version: %(oracle_package_version)s */
    PROCEDURE store_temp(
        zoids IN numlist,
        prev_tids IN numlist,
        md5s IN md5list,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM temp_store WHERE zoid = zoids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO temp_store (zoid, prev_tid, md5, state) VALUES
            (zoids(indx), prev_tids(indx), md5s(indx), states(indx));
    END store_temp;

    PROCEDURE restore(
        zoids IN numlist,
        tids IN numlist,
        states IN statelist) IS
    BEGIN
        FORALL indx IN zoids.first..zoids.last
            DELETE FROM object_state WHERE zoid = zoids(indx);
        FORALL indx IN zoids.first..zoids.last
            INSERT INTO object_state (zoid, tid, state_size, state) VALUES (
                zoids(indx),
                tids(indx),
                COALESCE(LENGTH(states(indx)), 0),
                states(indx));
    END restore;
END relstorage_op;
/
""" % globals()



@implementer(ISchemaInstaller)
class OracleSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'oracle'

    def get_database_name(self, cursor):
        cursor.execute("SELECT ora_database_name FROM DUAL")
        for (name,) in cursor:
            return name

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(_conn, cursor):
            tables = self.list_tables(cursor)
            if 'object_state' not in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)
            packages = self.list_packages(cursor)
            package_name = 'relstorage_op'
            if packages.get(package_name) != oracle_package_version:
                self.install_package(cursor)
                packages = self.list_packages(cursor)
                if packages.get(package_name) != oracle_package_version:
                    raise AssertionError(
                        "Could not get version information after "
                        "installing the %s package." % package_name)
        self.connmanager.open_and_call(callback)

    def install_package(self, cursor):
        """Install the package containing stored procedures"""
        if self.keep_history:
            code = oracle_history_preserving_package
        else:
            code = oracle_history_free_package

        for stmt in code.split('\n/\n'):
            if stmt.strip():
                cursor.execute(stmt)

    def list_tables(self, cursor):
        cursor.execute("SELECT table_name FROM user_tables")
        return [name.lower() for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        cursor.execute("SELECT sequence_name FROM user_sequences")
        return [name.lower() for (name,) in cursor.fetchall()]

    def list_packages(self, cursor):
        """List installed stored procedure packages.

        Returns {package name: version}.  version may be None.
        """
        stmt = """
        SELECT object_name
        FROM user_objects
        WHERE object_type = 'PACKAGE'
        """
        cursor.execute(stmt)
        names = [name for (name,) in cursor.fetchall()]

        res = {}
        for name in names:
            version = None
            stmt = """
            SELECT TEXT FROM USER_SOURCE
            WHERE TYPE='PACKAGE BODY'
                AND NAME=:1
            """
            cursor.execute(stmt, (name,))
            for (text,) in cursor:
                match = re.search(r'Version:\s*([0-9a-zA-Z.]+)', text)
                if match is not None:
                    version = match.group(1)
                    break
            res[name.lower()] = version
        return res

    def _create_commit_lock(self, cursor):
        return

    def _create_pack_lock(self, cursor):
        stmt = "CREATE TABLE pack_lock (dummy CHAR);"
        self.runner.run_script(cursor, stmt)

    def _create_transaction(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE transaction (
                tid         NUMBER(20) NOT NULL PRIMARY KEY,
                packed      CHAR DEFAULT 'N' CHECK (packed IN ('N', 'Y')),
                empty       CHAR DEFAULT 'N' CHECK (empty IN ('N', 'Y')),
                username    RAW(500),
                description RAW(2000),
                extension   RAW(2000)
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
                zoid        NUMBER(20) NOT NULL,
                tid         NUMBER(20) NOT NULL REFERENCES transaction,
                            PRIMARY KEY (zoid, tid),
                            CONSTRAINT tid_min CHECK (tid > 0),
                prev_tid    NUMBER(20) NOT NULL REFERENCES transaction,
                md5         CHAR(32),
                state_size  NUMBER(20) NOT NULL,
                            CONSTRAINT state_size_min CHECK (state_size >= 0),
                state       BLOB
            );
            CREATE INDEX object_state_tid ON object_state (tid);
            CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

            """
        else:
            stmt = """
            CREATE TABLE object_state (
                zoid        NUMBER(20) NOT NULL PRIMARY KEY,
                tid         NUMBER(20) NOT NULL,
                            CONSTRAINT tid_min CHECK (tid > 0),
                state_size  NUMBER(20) NOT NULL,
                            CONSTRAINT state_size_min CHECK (state_size >= 0),
                state       BLOB
            );
            CREATE INDEX object_state_tid ON object_state (tid);
            """

        self.runner.run_script(cursor, stmt)

    def _create_blob_chunk(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE blob_chunk (
                zoid        NUMBER(20) NOT NULL,
                tid         NUMBER(20) NOT NULL,
                chunk_num   NUMBER(20) NOT NULL,
                            PRIMARY KEY (zoid, tid, chunk_num),
                chunk       BLOB
            );
            CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
            ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid, tid)
                REFERENCES object_state (zoid, tid)
                ON DELETE CASCADE;
            """
        else:
            stmt = """
            CREATE TABLE blob_chunk (
                zoid        NUMBER(20) NOT NULL,
                chunk_num   NUMBER(20) NOT NULL,
                            PRIMARY KEY (zoid, chunk_num),
                tid         NUMBER(20) NOT NULL,
                chunk       BLOB
            );
            CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
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
                zoid        NUMBER(20) NOT NULL PRIMARY KEY,
                tid         NUMBER(20) NOT NULL,
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
                zoid        NUMBER(20) NOT NULL,
                tid         NUMBER(20) NOT NULL,
                to_zoid     NUMBER(20) NOT NULL,
                PRIMARY KEY (tid, zoid, to_zoid)
            );
            """
        else:
            stmt = """
            CREATE TABLE object_ref (
                zoid        NUMBER(20) NOT NULL,
                to_zoid     NUMBER(20) NOT NULL,
                tid         NUMBER(20) NOT NULL,
                PRIMARY KEY (zoid, to_zoid)
            );
            """

        self.runner.run_script(cursor, stmt)

    def _create_object_refs_added(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_refs_added (
                tid         NUMBER(20) NOT NULL PRIMARY KEY
            );
            """
        else:
            stmt = """
            CREATE TABLE object_refs_added (
                zoid        NUMBER(20) NOT NULL PRIMARY KEY,
                tid         NUMBER(20) NOT NULL
            );
            """
        self.runner.run_script(cursor, stmt)

    def _create_pack_object(self, cursor):
        stmt = """
        CREATE TABLE pack_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
            keep_tid    NUMBER(20) NOT NULL,
            visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);
        """
        self.runner.run_script(cursor, stmt)

    def _create_pack_state(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE pack_state (
                tid         NUMBER(20) NOT NULL,
                zoid        NUMBER(20) NOT NULL,
                PRIMARY KEY (tid, zoid)
            );
            """
            self.runner.run_script(cursor, stmt)

    def _create_pack_state_tid(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE pack_state_tid (
                tid         NUMBER(20) NOT NULL PRIMARY KEY
            );
            """
            self.runner.run_script(cursor, stmt)

    def _reset_oid(self, cursor):
        stmt = """
        DROP SEQUENCE zoid_seq;
        CREATE SEQUENCE zoid_seq;
        """
        self.runner.run_script(cursor, stmt)

    def _create_temp_store(self, cursor):
        stmt = """
            CREATE GLOBAL TEMPORARY TABLE temp_store (
                zoid        NUMBER(20) NOT NULL PRIMARY KEY,
                prev_tid    NUMBER(20) NOT NULL,
                md5         CHAR(32),
                state       BLOB,
                blobdata    BLOB
            ) ON COMMIT DELETE ROWS;
            """
        self.runner.run_script(cursor, stmt)

    def _create_temp_blob_chunk(self, cursor):
        stmt = """
            CREATE GLOBAL TEMPORARY TABLE temp_blob_chunk (
                zoid        NUMBER(20) NOT NULL,
                chunk_num   NUMBER(20) NOT NULL,
                chunk       BLOB
            ) ON COMMIT DELETE ROWS;
            """
        self.runner.run_script(cursor, stmt)


    def _create_temp_pack_visit(self, cursor):
        stmt = """
            CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
                zoid        NUMBER(20) NOT NULL PRIMARY KEY,
                keep_tid    NUMBER(20) NOT NULL
            );

            """
        self.runner.run_script(cursor, stmt)

    def _create_temp_undo(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE GLOBAL TEMPORARY TABLE temp_undo (
                zoid        NUMBER(20) NOT NULL PRIMARY KEY,
                prev_tid    NUMBER(20) NOT NULL
            );

            """
            self.runner.run_script(cursor, stmt)

    def _init_after_create(self, cursor):
        if self.keep_history:
            stmt = """
            INSERT INTO transaction (tid, username, description)
                VALUES (0,
                UTL_I18N.STRING_TO_RAW('system', 'US7ASCII'),
                UTL_I18N.STRING_TO_RAW(
                    'special transaction for object creation', 'US7ASCII'));
            """
            self.runner.run_script(cursor, stmt)
