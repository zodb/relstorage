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

# Procedure to set the minimum next OID in one trip to the server.
# This returns a result, so we'd like to use a function (which we'd access
# with `SELECT set_min_oid(...)`), but the MySQL manual warns that functions take
# out table locks and limit concurrency, plus have some replication issues,
# so we don't do that. Instead we have a procedure. The procedure should be called
# using `cursor.execute('CALL set_min_oid(%s)` instead of cursor.callproc()
# --- callproc() involves setting server variables in an extra trip to the server.
# That would enable us to use an OUT param for the result, but getting that would be another
# round trip too. Don't forget to use nextset() to get the result of the procedure itself
# after getting the new oid.
_SET_MIN_OID = """
CREATE PROCEDURE set_min_oid(min_oid BIGINT)
BEGIN
  -- In order to avoid deadlocks, we only do this if
  -- the number we want to insert is strictly greater than
  -- what the current sequence value is. If we use a value less
  -- than that, there's a chance a different session has already allocated
  -- and inserted that value into the table, meaning its locked.
  -- We obviously cannot JUST use MAX(zoid) to find this value, we can't see
  -- what other sessions have done. But if that's already >= to the min_oid,
  -- then we don't have to do anything.
  DECLARE next_oid BIGINT;

  SELECT COALESCE(MAX(ZOID), 0)
  INTO next_oid
  FROM new_oid;

  IF next_oid < min_oid THEN
    -- Can't say for sure. Just because we can only see values
    -- less doesn't mean they're not there in another transaction.

    -- This will never block.
    INSERT INTO new_oid VALUES ();
    SELECT LAST_INSERT_ID()
    INTO next_oid;

    IF min_oid > next_oid THEN
      -- This is unlikely to block. We just confirmed that the
      -- sequence value is strictly less than this, so no one else
      -- should be doing this.
      INSERT IGNORE INTO new_oid (zoid)
      VALUES (min_oid);

      SET next_oid = min_oid;
    END IF;
  ELSE
    -- Return a NULL value to signal that this value cannot
    -- be cached and used because we didn't allocate it.
    SET next_oid = NULL;
  END IF;

  SELECT next_oid;
END;
"""

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

    procedures = {
        'set_min_oid': _SET_MIN_OID,
    }

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
        return [native(row['name']) for row in self._rows_as_dicts(cursor)]

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

    def create_procedures(self, cursor):
        # TODO: Handle updates when we change the text.
        installed = self.list_procedures(cursor)
        for name, create_stmt in self.procedures.items():
            __traceback_info__ = name
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
