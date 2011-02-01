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
from relstorage.adapters.interfaces import ISchemaInstaller
from ZODB.POSException import StorageError
from zope.interface import implements
import re
import time

relstorage_op_version = '1.4'

history_preserving_schema = """

# commit_lock: Held during commit.  Another kind of lock is used for MySQL
# and Oracle.

    postgresql:
        CREATE TABLE commit_lock ();

# pack_lock: Held during pack.  Another kind of lock is used for MySQL.
# Another kind of lock is used for PostgreSQL >= 8.2.

    oracle:
        CREATE TABLE pack_lock (dummy CHAR);

# transaction: The list of all transactions in the database.

    postgresql:
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            empty       BOOLEAN NOT NULL DEFAULT FALSE,
            username    BYTEA NOT NULL,
            description BYTEA NOT NULL,
            extension   BYTEA
        );

    mysql:
        CREATE TABLE transaction (
            tid         BIGINT NOT NULL PRIMARY KEY,
            packed      BOOLEAN NOT NULL DEFAULT FALSE,
            empty       BOOLEAN NOT NULL DEFAULT FALSE,
            username    BLOB NOT NULL,
            description BLOB NOT NULL,
            extension   BLOB
        ) ENGINE = InnoDB;

    oracle:
        CREATE TABLE transaction (
            tid         NUMBER(20) NOT NULL PRIMARY KEY,
            packed      CHAR DEFAULT 'N' CHECK (packed IN ('N', 'Y')),
            empty       CHAR DEFAULT 'N' CHECK (empty IN ('N', 'Y')),
            username    RAW(500),
            description RAW(2000),
            extension   RAW(2000)
        );

# OID allocation

    postgresql:
        CREATE SEQUENCE zoid_seq;

    mysql:
        CREATE TABLE new_oid (
            zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) ENGINE = MyISAM;

    oracle:
        CREATE SEQUENCE zoid_seq;

# object_state: All object states in all transactions. Note that md5
# and state can be null to represent object uncreation.

    postgresql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL REFERENCES transaction
                        CHECK (tid > 0),
            PRIMARY KEY (zoid, tid),
            prev_tid    BIGINT NOT NULL REFERENCES transaction,
            md5         CHAR(32),
            state       BYTEA
        );
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    mysql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL REFERENCES transaction,
            PRIMARY KEY (zoid, tid),
            prev_tid    BIGINT NOT NULL REFERENCES transaction,
            md5         CHAR(32) CHARACTER SET ascii,
            state       LONGBLOB,
            CHECK (tid > 0)
        ) ENGINE = InnoDB;
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

    oracle:
        CREATE TABLE object_state (
            zoid        NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL REFERENCES transaction
                        CHECK (tid > 0),
            PRIMARY KEY (zoid, tid),
            prev_tid    NUMBER(20) NOT NULL REFERENCES transaction,
            md5         CHAR(32),
            state       BLOB
        );
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);

# current_object: Pointers to the current object state

    postgresql:
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state
        );
        CREATE INDEX current_object_tid ON current_object (tid);

    mysql:
        CREATE TABLE current_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state (zoid, tid)
        ) ENGINE = InnoDB;
        CREATE INDEX current_object_tid ON current_object (tid);

    oracle:
        CREATE TABLE current_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL,
            FOREIGN KEY (zoid, tid) REFERENCES object_state
        );
        CREATE INDEX current_object_tid ON current_object (tid);

# object_ref: A list of referenced OIDs from each object_state. This
# table is populated as needed during packing. To prevent unnecessary
# table locking, it does not use foreign keys, which is safe because
# rows in object_state are never modified once committed, and rows are
# removed from object_state only by packing.

    postgresql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        );

    mysql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_ref (
            zoid        NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL,
            to_zoid     NUMBER(20) NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        );

# The object_refs_added table tracks whether object_refs has been
# populated for all states in a given transaction. An entry is added
# only when the work is finished. To prevent unnecessary table locking,
# it does not use foreign keys, which is safe because object states are
# never added to a transaction once committed, and rows are removed
# from the transaction table only by packing.

    postgresql:
        CREATE TABLE object_refs_added (
            tid         BIGINT NOT NULL PRIMARY KEY
        );

    mysql:
        CREATE TABLE object_refs_added (
            tid         BIGINT NOT NULL PRIMARY KEY
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_refs_added (
            tid         NUMBER(20) NOT NULL PRIMARY KEY
        );

# pack_object: Temporary state during packing: The list of objects to
# pack. If keep is false, the object and all its revisions will be
# removed. If keep is true, instead of removing the object, the pack
# operation will cut the object's history. The keep_tid field specifies
# the oldest revision of the object to keep. The visited flag is set
# when pre_pack is visiting an object's references, and remains set.

    postgresql:
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

    mysql:
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        ) ENGINE = MyISAM;
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

    oracle:
        CREATE TABLE pack_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
            keep_tid    NUMBER(20) NOT NULL,
            visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

# pack_state: Temporary state during packing: the list of object states
# to pack.

    postgresql:
        CREATE TABLE pack_state (
            tid         BIGINT NOT NULL,
            zoid        BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid)
        );

    mysql:
        CREATE TABLE pack_state (
            tid         BIGINT NOT NULL,
            zoid        BIGINT NOT NULL,
            PRIMARY KEY (tid, zoid)
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE pack_state (
            tid         NUMBER(20) NOT NULL,
            zoid        NUMBER(20) NOT NULL,
            PRIMARY KEY (tid, zoid)
        );

# pack_state_tid: Temporary state during packing: the list of
# transactions that have at least one object state to pack.

    postgresql:
        CREATE TABLE pack_state_tid (
            tid         BIGINT NOT NULL PRIMARY KEY
        );

    mysql:
        CREATE TABLE pack_state_tid (
            tid         BIGINT NOT NULL PRIMARY KEY
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE pack_state_tid (
            tid         NUMBER(20) NOT NULL PRIMARY KEY
        );

# Oracle expects temporary tables to be created ahead of time, while
# MySQL and PostgreSQL expect them to be created in the session.

    oracle:
        # States that will soon be stored
        CREATE GLOBAL TEMPORARY TABLE temp_store (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            prev_tid    NUMBER(20) NOT NULL,
            md5         CHAR(32),
            state       BLOB
        ) ON COMMIT DELETE ROWS;

        # Temporary state during packing: a list of objects
        # whose references need to be examined.
        CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep_tid    NUMBER(20)
        );

        # Temporary state during undo: a list of objects
        # to be undone and the tid of the undone state.
        CREATE GLOBAL TEMPORARY TABLE temp_undo (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            prev_tid    NUMBER(20) NOT NULL
        );
"""

history_preserving_init = """
# Create a special '0' transaction to represent object creation. The
# '0' transaction is often referenced by object_state.prev_tid, but
# never by object_state.tid.

    postgresql:
        INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');

    mysql:
        INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');

    oracle:
        INSERT INTO transaction (tid, username, description)
            VALUES (0,
            UTL_I18N.STRING_TO_RAW('system', 'US7ASCII'),
            UTL_I18N.STRING_TO_RAW(
                'special transaction for object creation', 'US7ASCII'));

# Reset the OID counter.

    postgresql:
        ALTER SEQUENCE zoid_seq RESTART WITH 1;

    mysql:
        TRUNCATE new_oid;

    oracle:
        DROP SEQUENCE zoid_seq;
        CREATE SEQUENCE zoid_seq;
"""

oracle_history_preserving_plsql = """
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
/* Version: %s */
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
            INSERT INTO object_state (zoid, tid, prev_tid, md5, state) VALUES
            (zoids(indx), tids(indx),
            COALESCE((SELECT tid
                      FROM current_object
                      WHERE zoid = zoids(indx)), 0),
            md5s(indx), states(indx));
    END restore;
END relstorage_op;
/
""" % relstorage_op_version


history_free_schema = """

# commit_lock: Held during commit.  Another kind of lock is used for MySQL
# and Oracle.

    postgresql:
        CREATE TABLE commit_lock ();

# pack_lock: Held during pack.  Another kind of lock is used for MySQL.
# Another kind of lock is used for PostgreSQL >= 8.2.

    oracle:
        CREATE TABLE pack_lock (dummy CHAR);

# OID allocation

    postgresql:
        CREATE SEQUENCE zoid_seq;

    mysql:
        CREATE TABLE new_oid (
            zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) ENGINE = MyISAM;

    oracle:
        CREATE SEQUENCE zoid_seq;

# object_state: All object states in all transactions.

    postgresql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL CHECK (tid > 0),
            state       BYTEA
        );
        CREATE INDEX object_state_tid ON object_state (tid);

    mysql:
        CREATE TABLE object_state (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL,
            state       LONGBLOB,
            CHECK (tid > 0)
        ) ENGINE = InnoDB;
        CREATE INDEX object_state_tid ON object_state (tid);

    oracle:
        CREATE TABLE object_state (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL,
            state       BLOB
        );
        CREATE INDEX object_state_tid ON object_state (tid);

# object_ref: A list of referenced OIDs from each object_state. This
# table is populated as needed during packing.

    postgresql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );

    mysql:
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_ref (
            zoid        NUMBER(20) NOT NULL,
            to_zoid     NUMBER(20) NOT NULL,
            tid         NUMBER(20) NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );

# The object_refs_added table tracks which state of each object
# has been analyzed for references to other objects.

    postgresql:
        CREATE TABLE object_refs_added (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL
        );

    mysql:
        CREATE TABLE object_refs_added (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL
        ) ENGINE = MyISAM;

    oracle:
        CREATE TABLE object_refs_added (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            tid         NUMBER(20) NOT NULL
        );

# pack_object contains temporary state during garbage collection: The
# list of all objects, a flag signifying whether the object should be
# kept, and a flag signifying whether the object's references have been
# visited. The keep_tid field specifies the current revision of the
# object.

    postgresql:
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

    mysql:
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        ) ENGINE = MyISAM;
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

    oracle:
        CREATE TABLE pack_object (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep        CHAR NOT NULL CHECK (keep IN ('N', 'Y')),
            keep_tid    NUMBER(20) NOT NULL,
            visited     CHAR DEFAULT 'N' NOT NULL CHECK (visited IN ('N', 'Y'))
        );
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);

# Oracle expects temporary tables to be created ahead of time, while
# MySQL and PostgreSQL expect them to be created in the session.
# Note that the md5 column is not used in a history-free storage.

    oracle:
        # States that will soon be stored
        CREATE GLOBAL TEMPORARY TABLE temp_store (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            prev_tid    NUMBER(20) NOT NULL,
            md5         CHAR(32),
            state       BLOB
        ) ON COMMIT DELETE ROWS;

        # Temporary state during packing: a list of objects
        # whose references need to be examined.
        CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
            zoid        NUMBER(20) NOT NULL PRIMARY KEY,
            keep_tid    NUMBER(20)
        );
"""

history_free_init = """
# Reset the OID counter.

    postgresql:
        ALTER SEQUENCE zoid_seq RESTART WITH 1;

    mysql:
        TRUNCATE new_oid;

    oracle:
        DROP SEQUENCE zoid_seq;
        CREATE SEQUENCE zoid_seq;
"""

oracle_history_free_plsql = """
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
/* Version: %s */
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
            INSERT INTO object_state (zoid, tid, state) VALUES
            (zoids(indx), tids(indx), states(indx));
    END restore;
END relstorage_op;
/
""" % relstorage_op_version


def filter_script(script, database_name):
    res = []
    match = False
    for line in script.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.endswith(':'):
            match = (database_name in line[:-1].split())
            continue
        if match:
            res.append(line)
    return '\n'.join(res)


class AbstractSchemaInstaller(object):

    # Keep this list in the same order as the schema scripts
    all_tables = (
        'commit_lock',
        'pack_lock',
        'transaction',
        'new_oid',
        'object_state',
        'blob_chunk',  # Forward compat with RelStorage 1.5
        'current_object',
        'object_ref',
        'object_refs_added',
        'pack_object',
        'pack_state',
        'pack_state_tid',
        'temp_store',
        'temp_blob_chunk',  # Forward compat with RelStorage 1.5
        'temp_pack_visit',
        'temp_undo',
        )

    database_name = None  # provided by a subclass

    def __init__(self, connmanager, runner, keep_history):
        self.connmanager = connmanager
        self.runner = runner
        self.keep_history = keep_history
        if keep_history:
            self.schema_script = history_preserving_schema
            self.init_script = history_preserving_init
        else:
            self.schema_script = history_free_schema
            self.init_script = history_free_init

    def list_tables(self, cursor):
        raise NotImplementedError()

    def list_sequences(self, cursor):
        raise NotImplementedError()

    def create(self, cursor):
        """Create the database tables."""
        script = filter_script(self.schema_script, self.database_name)
        self.runner.run_script(cursor, script)
        script = filter_script(self.init_script, self.database_name)
        self.runner.run_script(cursor, script)

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            tables = self.list_tables(cursor)
            if not 'object_state' in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
        self.connmanager.open_and_call(callback)

    def check_compatibility(self, cursor, tables):
        if self.keep_history:
            if 'transaction' not in tables and 'current_object' not in tables:
                raise StorageError(
                    "Schema mismatch: a history-preserving adapter "
                    "can not connect to a history-free database. "
                    "If you need to convert, use the zodbconvert utility."
                    )
        else:
            if 'transaction' in tables and 'current_object' in tables:
                raise StorageError(
                    "Schema mismatch: a history-free adapter "
                    "can not connect to a history-preserving database. "
                    "If you need to convert, use the zodbconvert utility."
                    )

    def zap_all(self):
        """Clear all data out of the database."""
        def callback(conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = list(self.all_tables)
            todo.reverse()
            for table in todo:
                if table.startswith('temp_'):
                    continue
                if table in existent:
                    cursor.execute("DELETE FROM %s" % table)
            script = filter_script(self.init_script, self.database_name)
            if script:
                self.runner.run_script(cursor, script)
        self.connmanager.open_and_call(callback)

    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = list(self.all_tables)
            todo.reverse()
            for table in todo:
                if table in existent:
                    cursor.execute("DROP TABLE %s" % table)
            for sequence in self.list_sequences(cursor):
                cursor.execute("DROP SEQUENCE %s" % sequence)
        self.connmanager.open_and_call(callback)


class PostgreSQLSchemaInstaller(AbstractSchemaInstaller):
    implements(ISchemaInstaller)

    database_name = 'postgresql'

    def __init__(self, connmanager, runner, locker, keep_history):
        super(PostgreSQLSchemaInstaller, self).__init__(
            connmanager, runner, keep_history)
        self.locker = locker

    def create(self, cursor):
        """Create the database tables."""
        super(PostgreSQLSchemaInstaller, self).create(cursor)
        # Create the pack_lock table only on PostgreSQL 8.1 (not 8.2+)
        self.locker.create_pack_lock(cursor)

    def list_tables(self, cursor):
        cursor.execute("SELECT tablename FROM pg_tables")
        return [name for (name,) in cursor]

    def list_sequences(self, cursor):
        cursor.execute("SELECT relname FROM pg_class WHERE relkind = 'S'")
        return [name for (name,) in cursor]


class MySQLSchemaInstaller(AbstractSchemaInstaller):
    implements(ISchemaInstaller)

    database_name = 'mysql'

    def list_tables(self, cursor):
        cursor.execute("SHOW TABLES")
        return [name for (name,) in cursor]

    def list_sequences(self, cursor):
        return []

    def check_compatibility(self, cursor, tables):
        super(MySQLSchemaInstaller, self).check_compatibility(cursor, tables)
        stmt = "SHOW TABLE STATUS LIKE 'object_state'"
        cursor.execute(stmt)
        for row in cursor:
            for col_index, col in enumerate(cursor.description):
                if col[0].lower() == 'engine':
                    engine = row[col_index]
                    if engine.lower() != 'innodb':
                        raise StorageError(
                            "The object_state table must use the InnoDB "
                            "engine, but it is using the %s engine." % engine)


class OracleSchemaInstaller(AbstractSchemaInstaller):
    implements(ISchemaInstaller)

    database_name = 'oracle'

    def prepare(self):
        """Create the database schema if it does not already exist."""
        def callback(conn, cursor):
            tables = self.list_tables(cursor)
            if not 'object_state' in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
            packages = self.list_packages(cursor)
            if packages.get('relstorage_op') != relstorage_op_version:
                self.install_plsql(cursor)
                packages = self.list_packages(cursor)
                if packages.get('relstorage_op') != relstorage_op_version:
                    raise AssertionError(
                        "Could not get version information after "
                        "installing the relstorage_op package.")
        self.connmanager.open_and_call(callback)

    def install_plsql(self, cursor):
        """Install the unprivileged PL/SQL package"""
        if self.keep_history:
            plsql = oracle_history_preserving_plsql
        else:
            plsql = oracle_history_free_plsql

        lines = []
        for line in plsql.splitlines():
            if line.strip() == '/':
                # end of a statement
                cursor.execute('\n'.join(lines))
                lines = []
            elif line.strip():
                lines.append(line)

    def list_tables(self, cursor):
        cursor.execute("SELECT table_name FROM user_tables")
        return [name.lower() for (name,) in cursor]

    def list_sequences(self, cursor):
        cursor.execute("SELECT sequence_name FROM user_sequences")
        return [name.lower() for (name,) in cursor]

    def list_packages(self, cursor):
        """Returns {package name: version}.  version may be None."""
        stmt = """
        SELECT object_name
        FROM user_objects
        WHERE object_type = 'PACKAGE'
        """
        cursor.execute(stmt)
        names = [name for (name,) in cursor]

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
