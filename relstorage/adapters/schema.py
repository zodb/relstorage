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
import abc
import six

import logging

from ZODB.POSException import StorageError

log = logging.getLogger("relstorage")


@six.add_metaclass(abc.ABCMeta)
class AbstractSchemaInstaller(object):

    # Keep this list in the same order as the schema scripts
    all_tables = (
        'commit_lock',
        'pack_lock',
        'transaction',
        'new_oid',
        'object_state',
        'blob_chunk',
        'current_object',
        'object_ref',
        'object_refs_added',
        'pack_object',
        'pack_state',
        'pack_state_tid',
        'temp_store',
        'temp_blob_chunk',
        'temp_pack_visit',
        'temp_undo',
    )

    database_type = None  # provided by a subclass

    def __init__(self, connmanager, runner, keep_history):
        self.connmanager = connmanager
        self.runner = runner
        self.keep_history = keep_history

    @abc.abstractmethod
    def list_tables(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def list_sequences(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_database_name(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_commit_lock(self, cursor):
        """
        Create the global lock held during commit.

        (MySQL and PostgreSQL do this differently.)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_pack_lock(self, cursor):
        """
        Create the global lock held during pack.

        (MySQL and PostgreSQL do this differently.)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_transaction(self, cursor):
        """
        The transaction table lists all the transactions in the database.

        This table is only used for history-preserving databases.
        """
        raise NotImplementedError()


    @abc.abstractmethod
    def _create_new_oid(self, cursor):
        """
        Create the incrementing sequence for new OIDs.

        This should be the same for history free and preserving
        schemas.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_object_state(self, cursor):
        """
        Create the table holding all object states for all transactions.

        If the schema is history-free, only store the current state.
        History-preserving schemas may have a NULL `object_state` to represent
        uncreation.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_blob_chunk(self, cursor):
        """
        Create the table holding all blob states for all transactions.

        If the schema is history-free, only store the current state.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_current_object(self, cursor):
        """
        Table that stores pointers to the current object state.

        This table is only used for history-preserving databases.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_object_ref(self, cursor):
        """
        A list of referenced OIDs from each object_state. This
        table is populated as needed during packing. To prevent unnecessary
        table locking, it does not use foreign keys, which is safe because
        rows in object_state are never modified once committed, and rows are
        removed from object_state only by packing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_object_refs_added(self, cursor):
        """
        The object_refs_added table tracks whether object_refs has been
        populated for all states in a given transaction. An entry is added
        only when the work is finished. To prevent unnecessary table locking,
        it does not use foreign keys, which is safe because object states are
        never added to a transaction once committed, and rows are removed
        from the transaction table only by packing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_pack_object(self, cursor):
        """
        pack_object contains temporary state during garbage collection: The
        list of all objects, a flag signifying whether the object should be
        kept, and a flag signifying whether the object's references have been
        visited. The keep_tid field specifies the current revision of the
        object.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_pack_state(self, cursor):
        """
        Temporary state during packing: the list of object states
        # to pack.

        This is only used in history-preserving databases.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_pack_state_tid(self, cursor):
        """
        Temporary state during packing: the list of
        transactions that have at least one object state to pack.

        This is only used in history-preserving databases.
        """
        raise NotImplementedError()


    # Most databases handle temp tables on a session-by-session
    # basis.
    def _create_temp_store(self, _cursor):
        """States that will soon be stored."""
        return

    def _create_temp_blob_chunk(self, _cursor):
        """
        Temporary state during packing: a list of objects
        whose references need to be examined.
        """
        return

    def _create_temp_pack_visit(self, _cursor):
        return

    def _create_temp_undo(self, _cursor):
        """
        Temporary state during undo: a list of objects
        to be undone and the tid of the undone state.
        """
        return

    def _init_after_create(self, cursor):
        """
        Create a special '0' transaction to represent object creation. The
        '0' transaction is often referenced by object_state.prev_tid, but
        never by object_state.tid.

        Only in history-preserving databases.
        """
        if self.keep_history:
            stmt = """
            INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');
            """
            self.runner.run_script(cursor, stmt)

    @abc.abstractmethod
    def _reset_oid(self, cursor):
        raise NotImplementedError()

    def create(self, cursor):
        """Create the database tables."""
        for table in self.all_tables:
            meth = getattr(self, '_create_' + table)
            meth(cursor)

        self._init_after_create(cursor)

        tables = self.list_tables(cursor)
        self.check_compatibility(cursor, tables)

    def prepare(self):
        """Create the database schema if it does not already exist."""
        # XXX: We can generalize this to handle triggers, procs, etc,
        # to make subclasses have easier time.
        def callback(_conn, cursor):
            tables = self.list_tables(cursor)
            if 'object_state' not in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)
        self.connmanager.open_and_call(callback)

    def check_compatibility(self, cursor, tables): # pylint:disable=unused-argument
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
        if 'blob_chunk' not in tables:
            raise StorageError(
                "Schema mismatch; please create the blob_chunk tables."
                "See migration instructions for RelStorage 1.5."
            )

    def update_schema(self, cursor, tables):
        pass

    _zap_all_tbl_stmt = 'DELETE FROM %s'

    def zap_all(self, reset_oid=True, slow=False):
        """
        Clear all data out of the database.

        :keyword bool slow: If True (*not* the default) then database
            specific optimizations will be skipped and rows will simply be
            DELETEd. This is helpful when other connections might be open and
            holding some kind of locks.
        """
        stmt = self._zap_all_tbl_stmt if not slow else AbstractSchemaInstaller._zap_all_tbl_stmt

        def callback(_conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = reversed(self.all_tables)
            log.debug("Checking tables: %r", todo)
            for table in todo:
                log.debug("Considering table %s", table)
                if table.startswith('temp_'):
                    continue
                if table in existent:
                    log.debug("Deleting from table %s...", table)
                    cursor.execute(stmt % table)
            log.debug("Done deleting from tables.")

            log.debug("Running init script.")
            self._init_after_create(cursor)
            log.debug("Done running init script.")

            if reset_oid:
                log.debug("Running OID reset script.")
                self._reset_oid(cursor)
                log.debug("Done running OID reset script.")

        self.connmanager.open_and_call(callback)


    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(_conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = list(self.all_tables)
            todo.reverse()
            for table in todo:
                if table in existent:
                    cursor.execute("DROP TABLE %s" % table)
            for sequence in self.list_sequences(cursor):
                cursor.execute("DROP SEQUENCE %s" % sequence)
        self.connmanager.open_and_call(callback)
