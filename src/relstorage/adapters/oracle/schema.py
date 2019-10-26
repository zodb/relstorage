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

import re

from zope.interface import implementer

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller

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

    all_tables = list(AbstractSchemaInstaller.all_tables)
    all_tables.remove('new_oid')
    all_tables = tuple(all_tables)

    all_sequences = (
        'zoid_seq',
    )

    database_type = 'oracle'

    COLTYPE_OID_TID = 'NUMBER(20)'
    COLTYPE_BINARY_STRING = 'RAW(2000)'
    COLTYPE_BLOB_CHUNK = 'BLOB'
    COLTYPE_BLOB_CHUNK_NUM = COLTYPE_OID_TID
    COLTYPE_STATE_SIZE = COLTYPE_OID_TID
    COLTYPE_STATE = 'BLOB'

    def get_database_name(self, cursor):
        cursor.execute("SELECT ora_database_name FROM DUAL")
        for (name,) in cursor:
            return name

    def create_procedures(self, cursor):
        packages = self.list_packages(cursor)
        package_name = 'relstorage_op'
        if packages.get(package_name) != oracle_package_version:
            self.install_package(cursor)
            packages = self.list_packages(cursor)
            if packages.get(package_name) != oracle_package_version:
                raise AssertionError(
                    "Could not get version information after "
                    "installing the %s package." % package_name)

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
        return [name for (name,) in cursor.fetchall()]

    def list_views(self, cursor):
        cursor.execute("SELECT view_name FROM user_views")
        return [name for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        cursor.execute("SELECT sequence_name FROM user_sequences")
        return {name for (name,) in cursor.fetchall()}

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

    list_procedures = list_packages

    def _create_pack_lock(self, cursor):
        stmt = "CREATE TABLE pack_lock (dummy CHAR);"
        self.runner.run_script(cursor, stmt)

    def _create_transaction(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE transaction (
                tid         NUMBER(20) NOT NULL PRIMARY KEY,
                packed      CHAR DEFAULT 'N' CHECK (packed IN ('N', 'Y')),
                is_empty    CHAR DEFAULT 'N' CHECK (is_empty IN ('N', 'Y')),
                username    RAW(500),
                description RAW(2000),
                extension   RAW(2000)
            );
            """
            self.runner.run_script(cursor, stmt)

    def _create_zoid_seq(self, cursor):
        stmt = """
        CREATE SEQUENCE zoid_seq;
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
