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
from __future__ import print_function

from zope.interface import implementer

from ..connmanager import connection_callback
from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller

logger = __import__('logging').getLogger(__name__)

class _StoredFunction(object):
    """
    Represents a function stored in the database.

    It is equal to another instance if it has the same
    name and same checksum (signature and creation statement
    are ignored).
    """

    # The function's name
    name = None

    # The function's signature: (boolean, bytea, bytea)
    signature = None

    # The function's CREATE statement
    create = None

    # The checksum for the create statement,
    # as stored in the object's comment.
    checksum = None

    def __init__(self, name, signature, checksum, create=None):
        self.name = name
        self.signature = signature
        self.checksum = checksum
        self.create = create

    def __str__(self):
        """
        Return ``name(signature)``
        """
        if self.signature is None:
            # We didn't read this from the database at all.
            # Don't pretend we know what it is.
            return self.name
        # Signature can be the empty string if it takes no params.
        return "%s(%s)" % (self.name, self.signature)

    def __repr__(self):
        return "<%s %s>" % (
            self, self.checksum
        )

    def __eq__(self, other):
        if not isinstance(other, _StoredFunction):
            return NotImplemented
        return self.name == other.name and self.checksum == other.checksum


@implementer(ISchemaInstaller)
class PostgreSQLSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'postgresql'

    _PROCEDURES = {} # Caching of proc files.

    def __init__(self, options, connmanager, runner, locker):
        self.options = options
        super(PostgreSQLSchemaInstaller, self).__init__(
            connmanager, runner, options.keep_history)
        self.locker = locker

    def _read_proc_files(self):
        procs = super(PostgreSQLSchemaInstaller, self)._read_proc_files()
        # Convert from bare strings into _StoredFunction objects
        # (which are missing their signatures at this point).
        current_object = 'current_object' if self.keep_history else 'object_state'
        if self.keep_history:
            object_state_join = """
            INNER JOIN object_state ON (object_state.zoid = cur.zoid
                                       AND object_state.tid = cur.tid)
            """
            object_state_name = 'object_state'
        else:
            object_state_join = ""
            object_state_name = 'cur'
        return {
            name: _StoredFunction(
                name,
                None,
                self._checksum_for_str(value),
                value.format(
                    CURRENT_OBJECT=current_object,
                    OBJECT_STATE_JOIN=object_state_join,
                    OBJECT_STATE_NAME=object_state_name
                )
            )
            for name, value
            in procs.items()
        }

    def get_database_name(self, cursor):
        cursor.execute("SELECT current_database()")
        row, = cursor.fetchall()
        name, = row
        return self._metadata_to_native_str(name)

    @connection_callback(inherit=AbstractSchemaInstaller._prepare_with_connection)
    def _prepare_with_connection(self, conn, cursor):
        super(PostgreSQLSchemaInstaller, self)._prepare_with_connection(conn, cursor)

        # Do we need to merge blob chunks?
        if not self.options.shared_blob_dir:
            cursor.execute('SELECT chunk_num FROM blob_chunk WHERE chunk_num > 0 LIMIT 1')
            if cursor.fetchone():
                logger.info("Merging blob chunks on the server.")
                cursor.execute("SELECT merge_blob_chunks()")
                # If we've done our job right, any blobs cached on
                # disk are still perfectly valid.

    def create_procedures(self, cursor):
        if not self.__all_procedures_installed(cursor):
            self.__install_procedures(cursor)
            if not self.__all_procedures_installed(cursor):
                raise AssertionError(
                    "Could not get version information after "
                    "installing the stored procedures.")

    def create_triggers(self, cursor):
        triggers = self.list_triggers(cursor)
        __traceback_info__ = triggers, self.list_tables(cursor), self.get_database_name(cursor)
        if 'blob_chunk_delete' not in triggers:
            self.__install_triggers(cursor)

    def __native_names_only(self, cursor):
        native = self._metadata_to_native_str
        return frozenset([
            native(name)
            for (name,) in cursor.fetchall()
        ])

    def list_tables(self, cursor):
        cursor.execute("SELECT tablename FROM pg_tables")
        return self.__native_names_only(cursor)

    def list_sequences(self, cursor):
        cursor.execute("SELECT relname FROM pg_class WHERE relkind = 'S'")
        return self.__native_names_only(cursor)

    def list_views(self, cursor):
        cursor.execute("SELECT relname FROM pg_class WHERE relkind = 'v'")
        return self.__native_names_only(cursor)

    def list_languages(self, cursor):
        cursor.execute("SELECT lanname FROM pg_catalog.pg_language")
        return self.__native_names_only(cursor)

    def __install_languages(self, cursor):
        if 'plpgsql' not in self.list_languages(cursor):
            cursor.execute("CREATE LANGUAGE plpgsql")

    def list_procedures(self, cursor):
        """
        Returns {procedure name: _StoredFunction}.
        """
        # The description is populated with
        # ``COMMENT ON FUNCTION <name>(<args>) IS 'comment'``.
        # Prior to Postgres 10, the args are required; with
        # 10 and later they are only needed if the function is overloaded.
        stmt = """
        SELECT p.proname AS funcname,
               d.description,
               pg_catalog.pg_get_function_identity_arguments(p.oid)
        FROM pg_proc p
        INNER JOIN pg_namespace n ON n.oid = p.pronamespace
        LEFT JOIN pg_description As d ON (d.objoid = p.oid)
        WHERE n.nspname = 'public'
        """

        cursor.execute(stmt)
        res = {}
        native = self._metadata_to_native_str
        for (name, checksum, signature) in cursor.fetchall():
            name = native(name)
            checksum = native(checksum)
            signature = native(signature)

            disk_func = self.procedures.get(name)
            res[name.lower()] = db_func = _StoredFunction(
                name,
                signature,
                checksum,
                # Include the source, if it's around
                disk_func.create if disk_func else None
            )

            if disk_func and disk_func.checksum == db_func.checksum:
                # Yay, what's in the database matches what's read from disk.
                # That means the signature must match too.
                disk_func.signature = db_func.signature
                # But of course we don't add a checksum to the db_func
                # if we don't already have one; we wouldn't be able to tell
                # a mismatch.

        return res

    def __all_procedures_installed(self, cursor):
        """
        Check whether all required stored procedures are installed.

        Returns True only if all required procedures are installed and
        up to date.
        """

        expected = self.procedures

        installed = self.list_procedures(cursor)
        # If the database evolves over tiem, there could be
        # extra procs still there that we don't care about.
        installed = {k: v for k, v in installed.items() if k in expected}
        if installed != expected:
            logger.info(
                "Procedures incorrect, will reinstall. "
                "Expected: %s."
                "Actual: %s",
                expected, installed
            )
            return False
        return True

    def __install_procedures(self, cursor):
        """Install the stored procedures"""
        self.__install_languages(cursor)

        # PostgreSQL procedures in the SQL language
        # do lots of validation at compile time; in particular,
        # they check that the functions they use in SELECT statements
        # actually exist. When we have procedures that call each other,
        # that means there's an order they have to be created in.
        # Rather than try to figure out what that order is, or encode it
        # in names somehow, we'll disable that validation for the duration
        # of this transaction (just like pg_dump.)
        cursor.execute('SET LOCAL check_function_bodies = off')

        for proc_name, stored_func in self.procedures.items():
            __traceback_info__ = proc_name, self.keep_history
            proc_source = stored_func.create
            # All definitions should be written with 'CREATE OR REPLACE'
            # so we don't need to bother with 'DROP'. Though, if the return
            # type changes, we can't REPLACE.
            cursor.execute(proc_source)

        # Update checksums
        # Postgres < 10 requires the signature to identify the function;
        # after that it's optional if the function isn't overloaded.
        # Rather than try to parse the signature from the file ourself, we
        # let the database do it and then ask it.
        for db_proc in self.list_procedures(cursor).values():
            # db_proc will have the signature but perhaps not the checksum.
            try:
                disk_proc = self.procedures[db_proc.name]
            except KeyError: # pragma: no cover
                # One in the DB no longer on disk.
                # Ignore.
                continue
            disk_proc.signature = db_proc.signature
            if db_proc.checksum == disk_proc.checksum:
                continue

            db_proc.checksum = disk_proc.checksum

            # For pg8000 we can't use a parameter here (because it prepares?)
            comment = "COMMENT ON FUNCTION %s IS '%s'" % (
                str(db_proc), db_proc.checksum
            )
            __traceback_info__ = comment
            cursor.execute(comment)


    def list_triggers(self, cursor):
        cursor.execute("SELECT tgname FROM pg_trigger")
        return self.__native_names_only(cursor)

    def __install_triggers(self, cursor):
        stmt = """
        CREATE TRIGGER blob_chunk_delete
            BEFORE DELETE ON blob_chunk
            FOR EACH ROW
            EXECUTE PROCEDURE blob_chunk_delete_trigger()
        """
        cursor.execute(stmt)

    def drop_all(self):
        def delete_blob_chunk(_conn, cursor):
            if 'blob_chunk' in self.list_tables(cursor):
                # Trigger deletion of blob OIDs.
                cursor.execute("DELETE FROM blob_chunk")
        self.connmanager.open_and_call(delete_blob_chunk)
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

    DROP_TABLE_TMPL = 'DROP TABLE IF EXISTS {table}'

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
