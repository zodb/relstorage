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

from collections import namedtuple

from ZODB.POSException import StorageError
from zope.interface import implementer

from ..connmanager import connection_callback
from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller
from ..schema import Schema
from .._util import DatabaseHelpersMixin

logger = __import__('logging').getLogger(__name__)

###
# Notes on procedures vs functions.
#
# When we have a result, we'd generally like to use a function,
# and simply ``SELECT func()``.
#
# However, MySQL is badly broken and functions play poorly with
# transaction logging (replication logging). For that reason, to
# create a function, you must have SUPER privileges (essentially root)
# so we can't count on being able to do that.
# (https://dev.mysql.com/doc/refman/5.7/en/stored-programs-logging.html).
# (Also, functions take out table locks and limit concurrency, and
# must be marked DETERMINISTIC or NO SQL or just READS SQL DATA ---
# they can't make modifications.)
#
# So we're stuck with procedures.
#
# The procedure should be called with ``cursor.execute('CALL
# proc_name(%s)')`` instead of ``cursor.callproc()`` ---
# ``callproc()`` involves setting server variables in an extra trip to
# the server. That would enable us to use an OUT param for the result,
# but getting that would be another round trip too. Procedures that
# generate results via a SELECT statement also generate a blank result
# that must be retrieved with cursor.nextset().
# ``Driver.callproc_multi_result`` handles the details.


# NOTE: Unlike PostgreSQL, NOW() and UTC_TIMESTAMP() are only consistent
# within a single *statement*; that is, unlike PostgreSQL, these values
# can change within a transaction.

_StoredProcedure = namedtuple('_StoredProcedure',
                              'checksum character_set_client collation_connection')


@implementer(ISchemaInstaller)
class MySQLSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'mysql'
    COLTYPE_BINARY_STRING = 'BLOB'
    TRANSACTIONAL_TABLE_SUFFIX = 'ENGINE = InnoDB'
    COLTYPE_MD5 = 'CHAR(32) CHARACTER SET ascii'
    COLTYPE_STATE = 'LONGBLOB'
    COLTYPE_BLOB_CHUNK = 'LONGBLOB'


    # The names of tables that in the past were explicitly declared as
    # MyISAM but which should now be InnoDB to work with transactions.
    tables_that_used_to_be_myisam_should_be_innodb = (
        'new_oid',
        'object_ref',
        'object_refs_added',
        'pack_object',
        'pack_state',
        'pack_state_tid',
    )

    _PROCEDURES = {}

    def __init__(self, driver=None, version_detector=None, **kwargs):
        self.driver = driver
        self.version_detector = version_detector or MySQLVersionDetector()
        super(MySQLSchemaInstaller, self).__init__(**kwargs)

    def get_database_name(self, cursor):
        cursor.execute("SELECT DATABASE()")
        for (name,) in cursor:
            return self._metadata_to_native_str(name)

    def list_procedures(self, cursor):
        cursor.execute("SHOW PROCEDURE STATUS WHERE db = database()")
        native = self._metadata_to_native_str
        return {
            native(row['name']): _StoredProcedure(
                native(row['comment']),
                native(row['character_set_client']),
                native(row['collation_connection']))
            for row in self._rows_as_dicts(cursor)
        }

    def list_tables(self, cursor):
        return list(self.__list_tables_and_engines(cursor))

    def __list_tables_and_engines(self, cursor):
        # {table_name: engine}, all in lower case.
        cursor.execute("SHOW  TABLE STATUS")
        native = self._metadata_to_native_str

        result = {
            native(row['name']): native(row['engine']).lower()
            for row in self._rows_as_dicts(cursor)
            # This also returns views for some reason, but they don't have
            # an engine.
            if row['engine']
        }
        return result

    def __list_tables_not_innodb(self, cursor):
        return {
            k: v
            for k, v in self.__list_tables_and_engines(cursor).items()
            if k in self.all_tables and v != 'innodb'
        }

    def list_sequences(self, cursor):
        return []

    def list_views(self, cursor):
        cursor.execute("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW'")
        return [self._metadata_to_native_str(r[0]) for r in cursor]

    def check_compatibility(self, cursor, tables):
        super(MySQLSchemaInstaller, self).check_compatibility(cursor, tables)
        tables_that_are_not_innodb = self.__list_tables_not_innodb(cursor)
        if tables_that_are_not_innodb:
            raise StorageError(
                "All RelStorage tables should be InnoDB; MyISAM is no longer supported. "
                "These tables are not using InnoDB: %r" % (tables_that_are_not_innodb,)
            )

    def _create_pack_lock(self, cursor):
        return

    # As usual, MySQL has a quirky implementation of this feature and we
    # have to re-specify *everything* about the column. MySQL 8 supports the
    # simple 'RENAME ... TO ...' syntax that everyone else does.
    _rename_transaction_empty_stmt = (
        "ALTER TABLE transaction CHANGE empty is_empty "
        "BOOLEAN NOT NULL DEFAULT FALSE"
    )

    _new_oid_query = Schema.new_oid.create()

    def _create_new_oid(self, cursor):
        self._new_oid_query.execute(cursor)

    # Temp tables are created in a session-by-session basis
    def _create_temp_store(self, _cursor):
        return

    def _create_temp_blob_chunk(self, _cursor):
        return

    def _create_temp_pack_visit(self, _cursor):
        return

    def _create_temp_undo(self, _cursor):
        return

    def _reset_oid(self, cursor):
        from .oidallocator import MySQLOIDAllocator
        MySQLOIDAllocator(self.driver).reset_oid(cursor)

    def __convert_all_tables_to_innodb(self, cursor):
        tables = self.__list_tables_not_innodb(cursor)
        if not tables:
            logger.debug("All tables already InnoDB")
            return

        logger.info("Converting tables to InnoDB: %s", tables)
        for table in tables:
            logger.info("Converting table %s to Innodb", table)
            cursor.execute("ALTER TABLE %s ENGINE=Innodb" % (table,))
        logger.info("Done converting tables to InnoDB: %s", tables)

    @connection_callback(inherit=AbstractSchemaInstaller._prepare_with_connection)
    def _prepare_with_connection(self, conn, cursor):
        from .oidallocator import MySQLOIDAllocator
        self.__convert_all_tables_to_innodb(cursor)
        super(MySQLSchemaInstaller, self)._prepare_with_connection(conn, cursor)
        MySQLOIDAllocator(self.driver).garbage_collect_oids(cursor)

    def _read_proc_files(self):
        name_to_source = super(MySQLSchemaInstaller, self)._read_proc_files()

        for proc_name, source in name_to_source.items():
            # No leading or trailing lines allowed, only the procedure
            # definition. That way everything is part of the checksum.
            assert source.startswith('CREATE') and source.endswith('END;')
            # Ensure we're creating what we think we are.
            assert proc_name in source
        return name_to_source

    def create_procedures(self, cursor):
        # Apparently procedures remember the ``character_set_client`` and ``collation_connection``
        # that was in use at the time they were defined, and use that
        # to perform implicit conversions on arguments and even internally. If we have
        # done ``SET NAMES binary`` (as we do on Python 2) and we try to pass a JSON
        # argument, or even parse a string into JSON inside the procedure, it will fail,
        # saying it can't convert binary into JSON. Therefore we must be sure to have
        # an appropriate value for both of those installed here.
        installed = self.list_procedures(cursor)
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

        major_version = self.version_detector.get_major_version(cursor)
        if self.version_detector.supports_nowait(cursor):
            set_lock_timeout = ''
            for_share = 'FOR SHARE NOWAIT'
        else:
            set_lock_timeout = 'SET innodb_lock_wait_timeout = 1;'
            for_share = 'LOCK IN SHARE MODE'

        cursor.execute('SET SESSION character_set_client = utf8, '
                       'collation_connection = utf8_general_ci')

        for name, create_stmt in self.procedures.items():
            __traceback_info__ = name
            checksum = self._checksum_for_str(create_stmt) + '; ver ' + str(major_version)
            create_stmt = create_stmt.format(
                CHECKSUM=checksum,
                CURRENT_OBJECT=current_object,
                OBJECT_STATE_NAME=object_state_name,
                OBJECT_STATE_JOIN=object_state_join,
                SET_LOCK_TIMEOUT=set_lock_timeout,
                FOR_SHARE=for_share
            )
            assert checksum in create_stmt
            if name in installed:
                installed_proc = installed[name]
                stored_checksum = installed_proc.checksum
                character_set_client = installed_proc.character_set_client
                collation_connection = installed_proc.collation_connection
                expected = (checksum, 'utf8', 'utf8_general_ci')
                if expected != (stored_checksum, character_set_client, collation_connection):
                    logger.info(
                        "Re-creating procedure %s due to mismatch %s != %s",
                        name,
                        installed_proc, expected
                    )
                    cursor.execute('DROP PROCEDURE %s' % (name,))
                    del installed[name]
                else:
                    logger.debug(
                        "Checksum for procedure %s matches: %s",
                        name, checksum
                    )

            if name not in installed:
                cursor.execute(create_stmt)

    # We can't TRUNCATE tables that have foreign-key relationships
    # with other tables, but we can drop them. This has to be followed up by
    # creating them again.
    _zap_all_tbl_stmt = 'DROP TABLE %s'

    def _after_zap_all_tables(self, cursor, slow=False):
        if not slow:
            logger.debug("Creating tables after drop")
            self.create_tables(cursor)
            logger.debug("Done creating tables after drop")
        else:
            super(MySQLSchemaInstaller, self)._after_zap_all_tables(cursor, slow)


class MySQLVersionDetector(DatabaseHelpersMixin):

    _version = None
    _version_info = None
    _major_version = None

    def _fetch_version_from_server(self, cursor):
        # The newer alternative to VERSION() isn't available everywhere
        # yet.
        # Hook for testing.
        cursor.execute('SELECT version()')
        ver = cursor.fetchone()[0]
        # PyMySQL on Win/Py3 returns this as a byte string; everywhere
        # else it's native.
        return self._metadata_to_native_str(ver)

    def _setup(self, cursor):
        if self._major_version:
            return

        ver = self._version = self._fetch_version_from_server(cursor)
        # . separated parts, with the last one optionally given a suffix
        ver_parts = ver.split('.')
        ver_parts = [part if '-' not in part else part[:part.index('-')]
                     for part in ver_parts]
        self._version_info = tuple(int(i) for i in ver_parts)
        self._major_version = int(ver[0])

    def get_major_version(self, cursor):
        self._setup(cursor)
        return self._major_version

    def get_version(self, cursor):
        self._setup(cursor)
        return self._version

    def get_version_info(self, cursor):
        self._setup(cursor)
        return self._version_info

    def supports_nowait(self, cursor):
        """
        Can we use ``FOR SHARE NOWAIT``?
        """
        return self.get_major_version(cursor) >= 8

    def supports_transaction_isolation(self, cursor):
        """
        The system variable @@transaction_isolation was added in 5.7.20
        along with @@transaction_read_only. Before that there was @@tx_isolation,
        but that's been removed in MySQL 8.
        """
        return self.get_version_info(cursor) >= (5, 7, 20)

    def supports_good_stored_procs(self, cursor):
        """
        Versions of MySQL prior to 5.7.19 crash when we call the stored procedure.
        See https://github.com/zodb/relstorage/pull/287#issuecomment-515518727
        """
        return self.get_version_info(cursor) >= (5, 7, 19)
