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

from ZODB.POSException import StorageError
from zope.interface import implementer

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller

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

    def __init__(self, driver=None, **kwargs):
        self.driver = driver
        super(MySQLSchemaInstaller, self).__init__(**kwargs)

    def get_database_name(self, cursor):
        cursor.execute("SELECT DATABASE()")
        for (name,) in cursor:
            return self._metadata_to_native_str(name)

    def list_procedures(self, cursor):
        cursor.execute("SHOW PROCEDURE STATUS WHERE db = database()")
        native = self._metadata_to_native_str
        return {
            native(row['name']): native(row['comment'])
            for row in self._rows_as_dicts(cursor)
        }

    def list_tables(self, cursor):
        return list(self.__list_tables_and_engines(cursor))

    def __list_tables_and_engines(self, cursor):
        # {table_name: engine}, all in lower case.
        cursor.execute('SHOW TABLE STATUS')
        native = self._metadata_to_native_str
        result = {
            native(row['name']): native(row['engine']).lower()
            for row in self._rows_as_dicts(cursor)
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

    def _create_new_oid(self, cursor):
        stmt = """
        CREATE TABLE new_oid (
            zoid        {oid_type} NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) {transactional_suffix};
        """
        self.runner.run_script(cursor, stmt)

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
        logger.info("Converting tables to InnoDB: %s", tables)
        for table in tables:
            logger.info("Converting table %s to Innodb", table)
            cursor.execute("ALTER TABLE %s ENGINE=Innodb" % (table,))
        logger.info("Done converting tables to InnoDB: %s", tables)

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
        installed = self.list_procedures(cursor)
        for name, create_stmt in self.procedures.items():
            __traceback_info__ = name
            checksum = self._checksum_for_str(create_stmt)
            create_stmt = create_stmt.format(CHECKSUM=checksum)
            assert checksum in create_stmt
            if name in installed:
                stored_checksum = installed[name]
                if stored_checksum != checksum:
                    logger.info(
                        "Re-creating procedure %s due to checksum mismatch %s != %s",
                        name,
                        stored_checksum, checksum
                    )
                    cursor.execute('DROP PROCEDURE %s' % (name,))
                    del installed[name]

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
