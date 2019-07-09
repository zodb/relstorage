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

    def get_database_name(self, cursor):
        cursor.execute("SELECT DATABASE()")
        for (name,) in cursor:
            return self._metadata_to_native_str(name)

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
        MySQLOIDAllocator().reset_oid(cursor)

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
        MySQLOIDAllocator().garbage_collect_oids(cursor)

    # We can't TRUNCATE tables that have foreign-key relationships
    # with other tables, but we can drop them. This has to be followed up by
    # creating them again.
    _zap_all_tbl_stmt = 'DROP TABLE %s'

    def _after_zap_all_tables(self, cursor, slow=False):
        if not slow:
            logger.debug("Creating tables after drop")
            self.create(cursor)
            logger.debug("Done creating tables after drop")
        else:
            super(MySQLSchemaInstaller, self)._after_zap_all_tables(cursor, slow)
