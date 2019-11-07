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
import os
import glob
import sys

from functools import partial
from hashlib import md5

from ZODB.POSException import StorageError

from .._compat import ABC
from ._util import DatabaseHelpersMixin
from ._util import query_property
from ._util import noop_when_history_free

from .connmanager import connection_callback

from .sql import View
from .sql import Table
from .sql import TemporaryTable
from .sql import Column
from .sql import HistoryVariantTable
from .sql import OID
from .sql import TID
from .sql import State
from .sql import Boolean
from .sql import BinaryString
from .sql import Char
from .sql import ColumnExpression
from .sql import it

StateSize = OID

logger = __import__('logging').getLogger(__name__)

tmpl_property = partial(query_property,
                        property_suffix='_TMPLS',
                        lazy_suffix='_TMPL')

class Schema(object):
    current_object = Table(
        'current_object',
        Column('zoid', OID),
        Column('tid', TID)
    )

    object_state = Table(
        'object_state',
        Column('zoid', OID),
        Column('tid', TID),
        Column('state', State),
        Column('state_size', StateSize),
        # These two only exist in history preserving.
        # TODO: Wrap this in a HistoryVariantTable
        Column('prev_tid', TID),
        Column('md5'),
    )

    blob_chunk = Table(
        'blob_chunk',
        Column('zoid'),
        Column('tid'),
        Column('chunk_num'),
        Column('chunk')
    )

    # Does the right thing whether history free or preserving
    all_current_object = HistoryVariantTable(
        current_object,
        object_state,
    )

    current_object_state = View(
        'current_object_state',
        object_state.inner_join(
            current_object
        ).using(
            current_object.c.zoid,
            current_object.c.tid
        ).select(
            object_state.c.zoid,
            object_state.c.tid,
            it.c.state
        )
    )

    # Does the right thing whether history free or preserving
    all_current_object_state = HistoryVariantTable(
        current_object_state,
        object_state
    )

    temp_store = TemporaryTable(
        'temp_store',
        Column('zoid', OID, primary_key=True),
        Column('prev_tid', TID, nullable=False),
        Column('md5', Char(32)), # Only used when keep_history=True
        Column('state', State)
    )

    temp_blob_chunk = TemporaryTable(
        'temp_blob_chunk',
        Column('zoid'),
        Column('chunk_num'),
        Column('chunk'),
    )

    transaction = Table(
        'transaction',
        Column('tid', TID, primary_key=True),
        Column('packed', Boolean, nullable=False, default=False),
        Column('is_empty', Boolean, nullable=False, default=False),
        Column('username', BinaryString, nullable=False),
        Column('description', BinaryString, nullable=False),
        Column('extension', BinaryString),
    )

    commit_row_lock = Table(
        'commit_row_lock',
        Column('tid', TID, primary_key=True),
    )

    all_transaction = HistoryVariantTable(
        transaction,
        object_state,
    )

    # Not all databases will need this table
    new_oid = Table(
        'new_oid',
        Column('zoid', OID, primary_key=True, auto_increment=True)
    )

    # Packing
    object_ref = Table(
        'object_ref',
        Column('tid', TID),
    )

    object_refs_added = Table(
        'object_refs_added',
        Column('tid', TID)
    )

    pack_object = Table(
        'pack_object',
        Column('zoid', OID, primary_key=True),
        Column('keep', Boolean, nullable=False),
        Column('keep_tid', TID, nullable=False),
        Column('visited', Boolean, nullable=False, default=False)
    )


class AbstractSchemaInstaller(DatabaseHelpersMixin,
                              ABC):

    # Names of schema objects: Wherever we write them, they should be
    # in lower case, even if the database internally changes the case.
    # (In general, they should be written to match the output of
    # :meth:`_normalize_schema_object_names`.)

    # Keep this list in the same order as the schema scripts,
    # for dependency (Foreign Key) purposes.
    # These must be lower case, and all queries we write must
    # use lower case table names (MySQL has weird casing rules,
    # but this is also how we do comparisons in Python on table metadata.)
    all_tables = (
        # History-free row lock table for commits.
        # The alternative is to also create the transaction table
        # in HF mode.
        'commit_row_lock',
        'pack_lock',
        'transaction',
        'new_oid', # If you use a sequence, remove this
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

    all_sequences = ()

    all_views = (
        'current_object_state',
    )

    # Tables that might exist, but which are unused and obsolete.
    # These can/should be dropped, and names shouldn't be reused.
    obsolete_tables = (
        'commit_lock',
    )

    database_type = None  # provided by a subclass

    # A dictionary from procedure name to procedure definition.
    # We populate this automatically at construction time based on
    # our history-keeping state.
    procedures = None

    # Class variable.
    # {keep_history: {proc_name: proc_source}}
    # subclasses *must* define if they want caching.
    _PROCEDURES = None # type: dict

    def __init__(self, connmanager, runner, keep_history):
        self.connmanager = connmanager
        self.driver = connmanager.driver
        self.keep_history = keep_history
        self.runner = runner.with_format_vars(
            tid_type=self.COLTYPE_OID_TID,
            oid_type=self.COLTYPE_OID_TID,
            binary_string_type=self.COLTYPE_BINARY_STRING,
            blob_chunk_type=self.COLTYPE_BLOB_CHUNK,
            blob_chunk_num_type=self.COLTYPE_BLOB_CHUNK_NUM,
            md5_type=self.COLTYPE_MD5,
            state_type=self.COLTYPE_STATE,
            state_size_type=self.COLTYPE_STATE_SIZE,
            transactional_suffix=self.TRANSACTIONAL_TABLE_SUFFIX,
        )

        # if subclasses don't define, we don't cache.
        proc_cache = self._PROCEDURES if self._PROCEDURES is not None else {}
        if self.keep_history not in proc_cache:
            proc_cache[self.keep_history] = self._read_proc_files()
        self.procedures = proc_cache[self.keep_history]

    def _read_proc_files(self):
        """
        Read the procedure files appropriate for the *keep_history*
        setting and return a dictionary from procedure name to
        procedure definition source.

        The files come from the ``procs`` subdirectory beside the
        ``__file__`` in which the class of *self* is defined, and
        either the ``hf`` or ``hp`` folders within ``procs``
        (depending on ``self.keep_history``). The file name, minus the
        ``.sql`` extension, must match the name of the procedure. All
        ``.sql`` files in the ``procs`` directory are used for both
        history states, and then files in the ``hf`` or ``hp``
        directory are added to that; the file names in ``procs`` and ``h[pf]``
        must not be duplicates. Further, the file name (minus extension)
        must appear in the source.

        The file source is read in text mode and stripped, but other than that
        is unprocessed.
        """

        # TODO: importlib.resources or its backport importlib_resources

        self_dir = os.path.dirname(sys.modules[type(self).__module__].__file__)

        generic_proc_dir = os.path.join(self_dir, 'procs')
        specific_proc_dir = os.path.join(generic_proc_dir, 'hp' if self.keep_history else 'hf')
        logger.debug(
            "Reading stored procedures from %s and %s",
            generic_proc_dir, specific_proc_dir
        )
        proc_files = []
        for d in generic_proc_dir, specific_proc_dir:
            proc_files.extend(
                glob.glob(os.path.join(d, "*.sql"))
            )

        # Make sure there's no dups, that's probably an accident
        assert len(proc_files) == len(set(proc_files))

        procedures = {}
        for proc_file_name in proc_files:
            with open(proc_file_name, "rt") as f:
                source = f.read().strip()
            proc_name = os.path.splitext(os.path.basename(proc_file_name))[0]
            __traceback_info__ = proc_file_name, proc_name
            assert proc_name in source
            procedures[proc_name] = source

        return procedures

    @staticmethod
    def _checksum_for_str(stmt):
        return md5(
            stmt.encode('ascii')
            if not isinstance(stmt, bytes)
            else stmt
        ).hexdigest()

    @abc.abstractmethod
    def list_tables(self, cursor):
        raise NotImplementedError

    @abc.abstractmethod
    def list_sequences(self, cursor):
        raise NotImplementedError

    @abc.abstractmethod
    def list_procedures(self, cursor):
        raise NotImplementedError

    @abc.abstractmethod
    def list_views(self, cursor):
        return ()

    @abc.abstractmethod
    def get_database_name(self, cursor):
        raise NotImplementedError

    _create_commit_row_lock_query = Schema.commit_row_lock.create()

    def _create_commit_row_lock(self, cursor):
        """
        Create the global lock held during commit.

        (MySQL and PostgreSQL do this differently.)
        """
        self._create_commit_row_lock_query.execute(cursor)

    @abc.abstractmethod
    def _create_pack_lock(self, cursor):
        """
        Create the global lock held during pack.

        (MySQL and PostgreSQL do this differently.)
        """
        raise NotImplementedError()

    # TODO: Finish porting the CREATE statements to use the table
    # definitions in Schema and away from string templates.

    #: The type of the column used to hold transaction IDs
    #: and object IDs (64-bit integers).
    COLTYPE_OID_TID = 'BIGINT'
    #: The type of the column used to hold binary strings.
    #: Our default is appropriate for PostgreSQL.
    COLTYPE_BINARY_STRING = 'BYTEA'
    COLTYPE_STATE = COLTYPE_BINARY_STRING
    COLTYPE_STATE_SIZE = 'BIGINT'
    #: The type of the column used to number blob chunks.
    COLTYPE_BLOB_CHUNK_NUM = 'BIGINT'
    #: The type of the column used to store blob chunks.
    COLTYPE_BLOB_CHUNK = 'OID'
    #: The type of the column used to store MD5 hash strings.
    COLTYPE_MD5 = 'CHAR(32)'
    #: The suffix needed (after the closing ')') to make sure a
    #: table behaves in a transactional manner.
    #: Our default is appropriate for PostgreSQL.
    TRANSACTIONAL_TABLE_SUFFIX = ''

    _create_transaction_query = Schema.transaction.create()

    @noop_when_history_free
    def _create_transaction(self, cursor):
        """
        The transaction table lists all the transactions in the database.

        This table is only used for history-preserving databases.
        """
        self._create_transaction_query.execute(cursor)

    # NOTE: Prior to MySQL 8.0.16, CHECK constraints
    # are ignored at creation time and dropped. Thus if you upgrade
    # an existing 5.7 schema to 8, constraints will not be enforced,
    # but if you create a new schema under 8, these constraints will
    # be enforced.

    CREATE_OBJECT_STATE_TMPLS = (
        """
        CREATE TABLE object_state (
            zoid        {oid_type} NOT NULL,
            tid         {tid_type} NOT NULL
                           REFERENCES "transaction",
            prev_tid    {tid_type} NOT NULL
                           REFERENCES "transaction",
            md5         {md5_type},
            state_size  {state_size_type} NOT NULL,
            state       {state_type},
            CONSTRAINT object_state_pk
                PRIMARY KEY (zoid, tid),
            CHECK (tid > 0),
            CHECK (state_size >= 0)
        ) {transactional_suffix};
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);
        """,
        """
        CREATE TABLE object_state (
            zoid        {oid_type} NOT NULL PRIMARY KEY,
            tid         {tid_type} NOT NULL,
            state_size  {state_size_type} NOT NULL,
            state       {state_type},
            CHECK (tid > 0),
            CHECK (state_size >= 0)
        ) {transactional_suffix};
        CREATE INDEX object_state_tid ON object_state (tid);
        """
    )

    CREATE_OBJECT_STATE_TMPL = tmpl_property('CREATE_OBJECT_STATE')

    def _create_object_state(self, cursor):
        """
        Create the table holding all object states for all transactions.

        If the schema is history-free, only store the current state.
        History-preserving schemas may have a NULL `object_state` to represent
        uncreation.
        """
        self.runner.run_script(cursor, self.CREATE_OBJECT_STATE_TMPL)

    CREATE_BLOB_CHUNK_TMPLS = (
        """
        CREATE TABLE blob_chunk (
            zoid        {oid_type} NOT NULL,
            tid         {tid_type} NOT NULL,
            chunk_num   {blob_chunk_num_type} NOT NULL,
            chunk       {blob_chunk_type} NOT NULL,
            CONSTRAINT blob_chunk_pk
                PRIMARY KEY (zoid, tid, chunk_num),
            CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid, tid)
                REFERENCES object_state (zoid, tid)
                ON DELETE CASCADE
        ) {transactional_suffix};
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
        """,
        """
        CREATE TABLE blob_chunk (
            zoid        {oid_type} NOT NULL,
            chunk_num   {blob_chunk_num_type} NOT NULL,
            tid         {tid_type} NOT NULL,
            chunk       {blob_chunk_type} NOT NULL,
            CONSTRAINT blob_chunk_pk
                PRIMARY KEY (zoid, chunk_num),
            CONSTRAINT  blob_chunk_fk
                FOREIGN KEY (zoid)
                REFERENCES object_state (zoid)
                ON DELETE CASCADE
        ) {transactional_suffix};
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
        """
    )

    CREATE_BLOB_CHUNK_TMPL = tmpl_property('CREATE_BLOB_CHUNK')

    def _create_blob_chunk(self, cursor):
        """
        Create the table holding all blob states for all transactions.

        If the schema is history-free, only store the current state.
        """
        self.runner.run_script(cursor, self.CREATE_BLOB_CHUNK_TMPL)

    CREATE_CURRENT_OBJECT_TMPL = """
    CREATE TABLE current_object (
        zoid        {oid_type} NOT NULL PRIMARY KEY,
        tid         {tid_type} NOT NULL,
        FOREIGN KEY (zoid, tid)
            REFERENCES object_state (zoid, tid)
    ) {transactional_suffix};
    CREATE INDEX current_object_tid ON current_object (tid);
    """

    @noop_when_history_free
    def _create_current_object(self, cursor):
        """
        Table that stores pointers to the current object state.

        This table is only used for history-preserving databases.
        """
        self.runner.run_script(cursor, self.CREATE_CURRENT_OBJECT_TMPL)

    CREATE_OBJECT_REF_TMPLS = (
        """
        CREATE TABLE object_ref (
            zoid        {oid_type} NOT NULL,
            tid         {tid_type} NOT NULL,
            to_zoid     {oid_type} NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        ) {transactional_suffix};
        """,
        """
        CREATE TABLE object_ref (
            zoid        {oid_type} NOT NULL,
            to_zoid     {tid_type} NOT NULL,
            tid         {oid_type} NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        ) {transactional_suffix};
        """
    )

    CREATE_OBJECT_REF_TMPL = tmpl_property('CREATE_OBJECT_REF')

    def _create_object_ref(self, cursor):
        """
        A list of referenced OIDs from each object_state. This
        table is populated as needed during packing. To prevent unnecessary
        table locking, it does not use foreign keys, which is safe because
        rows in object_state are never modified once committed, and rows are
        removed from object_state only by packing.
        """
        self.runner.run_script(cursor, self.CREATE_OBJECT_REF_TMPL)


    CREATE_OBJECT_REFS_ADDED_TMPLS = (
        """
        CREATE TABLE object_refs_added (
            tid         {tid_type} NOT NULL PRIMARY KEY
        ) {transactional_suffix};
        """,
        """
        CREATE TABLE object_refs_added (
            zoid        {oid_type} NOT NULL PRIMARY KEY,
            tid         {tid_type} NOT NULL
        ) {transactional_suffix}
        """
    )

    CREATE_OBJECT_REFS_ADDED_TMPL = tmpl_property('CREATE_OBJECT_REFS_ADDED')

    def _create_object_refs_added(self, cursor):
        """
        The object_refs_added table tracks whether object_refs has been
        populated for all states in a given transaction. An entry is added
        only when the work is finished. To prevent unnecessary table locking,
        it does not use foreign keys, which is safe because object states are
        never added to a transaction once committed, and rows are removed
        from the transaction table only by packing.
        """
        self.runner.run_script(cursor, self.CREATE_OBJECT_REFS_ADDED_TMPL)


    CREATE_PACK_OBJECT_IX_TMPL = """
    CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid)
    """

    _create_pack_object_query = Schema.pack_object.create()

    def _create_pack_object(self, cursor):
        """
        pack_object contains temporary state during garbage collection: The
        list of all objects, a flag signifying whether the object should be
        kept, and a flag signifying whether the object's references have been
        visited. The keep_tid field specifies the current revision of the
        object.
        """
        self._create_pack_object_query.execute(cursor)
        # Some databases (pg) have multiple statements
        self.runner.run_script(cursor, self.CREATE_PACK_OBJECT_IX_TMPL)

    CREATE_PACK_STATE_TMPL = """
    CREATE TABLE pack_state (
        tid         {tid_type} NOT NULL,
        zoid        {oid_type} NOT NULL,
        PRIMARY KEY (tid, zoid)
    ) {transactional_suffix};
    """

    @noop_when_history_free
    def _create_pack_state(self, cursor):
        """
        Temporary state populated during pre-packing.

        This is only used in history-preserving databases.

        This table is poorly named. What it actually holds is the set
        of objects, along with their maximum TID, that are potentially
        eligible to be discarded because their most recent change
        (maximum TID) is earlier than the pack time.
        """
        self.runner.run_script(cursor, self.CREATE_PACK_STATE_TMPL)

    CREATE_PACK_STATE_TID_TMPL = """
    CREATE TABLE pack_state_tid (
        tid {tid_type} NOT NULL PRIMARY KEY
    ) {transactional_suffix};
    """

    @noop_when_history_free
    def _create_pack_state_tid(self, cursor):
        """
        Temporary state during pre-packing:

        This is only used in history-preserving databases.

        This table is poorly named. What it actually holds is simply a
        summary of the distinct transaction IDs found in
        ``pack_state``. In other words, it's the list of transaction
        IDs that are eligible to be discarded.
        """
        self.runner.run_script(cursor, self.CREATE_PACK_STATE_TID_TMPL)


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

    _create_current_object_state_query = Schema.current_object_state.create()

    @noop_when_history_free
    def _create_current_object_state(self, cursor):
        self._create_current_object_state_query.execute(cursor)

    def _init_after_create(self, cursor):
        """
        Create a special '0' transaction to represent object creation. The
        '0' transaction is often referenced by object_state.prev_tid, but
        never by object_state.tid. (Only in history-preserving databases.)

        In all databases, populates the ``commit_row_lock`` table
        with a single row to use as a global lock at commit time.
        """
        if self.keep_history:
            stmt = Schema.transaction.insert(
                Schema.transaction.c.tid,
                Schema.transaction.c.username,
                Schema.transaction.c.description
            ).bind(self).compiled()
            stmt.execute(
                cursor,
                (0,
                 self.driver.Binary(b'system'),
                 self.driver.Binary(b'special transaction for object creation'))
            )

        stmt = """
        INSERT INTO commit_row_lock (tid)
        VALUES (0);
        """
        self.runner.run_script(cursor, stmt)

    @abc.abstractmethod
    def _reset_oid(self, cursor):
        raise NotImplementedError()

    def _normalize_schema_object_names(self, names):
        return [n.lower() for n in names]

    def _create_schema_objects(self, cursor, all_object_names, existing_object_names):
        # Normalize names
        existing_object_names = set(self._normalize_schema_object_names(existing_object_names))
        todo = set(all_object_names) - existing_object_names
        # Preserve order
        todo = [t for t in all_object_names if t in todo]
        __traceback_info__ = todo, existing_object_names, all_object_names
        for table in todo:
            meth = getattr(self, '_create_' + table)
            meth(cursor)
        return todo, existing_object_names

    def create_tables(self, cursor, existing_tables=()):
        """Create the database tables."""
        _, existing_tables = self._create_schema_objects(cursor, self.all_tables, existing_tables)

        if self.keep_history and 'transaction' not in existing_tables:
            self._init_after_create(cursor)
        elif not self.keep_history and 'commit_row_lock' not in existing_tables:
            self._init_after_create(cursor)

        tables = self.list_tables(cursor)
        self.check_compatibility(cursor, tables)
        return tables

    def create_procedures(self, cursor):
        "Subclasses should override"

    def create_triggers(self, cursor):
        "Subclasses should override"

    def create_sequences(self, cursor):
        "Subclasses should override."
        self._create_schema_objects(cursor,
                                    self.all_sequences,
                                    self.list_sequences(cursor))

    def create_views(self, cursor):
        self._create_schema_objects(cursor,
                                    self.all_views,
                                    self.list_views(cursor))

    @connection_callback(read_only=False, application_name="RS: PrepareSchema")
    def _prepare_with_connection(self, conn, cursor): # pylint:disable=unused-argument
        # XXX: We can generalize this to handle triggers, procs, etc,
        # to make subclasses have easier time.
        existing_tables = self.list_tables(cursor)
        __traceback_info__ = existing_tables
        all_tables = self.create_tables(cursor, existing_tables)
        __traceback_info__ = existing_tables, all_tables
        if 'transaction' in self._normalize_schema_object_names(existing_tables):
            self.update_schema(cursor, existing_tables)

        self.create_sequences(cursor)
        self.create_procedures(cursor)
        self.create_triggers(cursor)
        self.create_views(cursor)

    def prepare(self):
        self.connmanager.open_and_call(self._prepare_with_connection)

    def verify(self):
        self.connmanager.open_and_call(self._verify)

    @connection_callback(read_only=True, application_name='RS: VerifySchema')
    def _verify(self, conn, cursor): # pylint:disable=unused-argument
        tables = self.list_tables(cursor)
        self.check_compatibility(cursor, tables)

    def check_compatibility(self, cursor, tables): # pylint:disable=unused-argument
        tables = self._normalize_schema_object_names(tables)
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
                "Schema mismatch; please create the blob_chunk tables. "
                "See migration instructions for RelStorage 1.5. "
                "All tables: %s" % (tables,)
            )

    def update_schema(self, cursor, tables): # pylint:disable=unused-argument
        """
        Perform any migration steps that are needed to make a schema
        that has already been created some time in the past match
        what would currently be installed.

        Subclasses may override.
        """

        # Currently we only take care of renaming the `transaction.empty`
        # column (from RelStorage 2.x and earlier) to `transaction.is_empty`
        # as used in RelStorage 3.x.
        if self._needs_transaction_empty_update(cursor):
            cursor.execute(self._rename_transaction_empty_stmt)

    _blank_transaction_query = Schema.transaction.select(
        ColumnExpression('*')
    ).where(Schema.transaction.c.tid < 0)

    def _needs_transaction_empty_update(self, cursor):
        # Get a description of the table, but don't actually return
        # any rows.
        if not self.keep_history:
            return False

        self._blank_transaction_query.execute(cursor)
        columns = self._column_descriptions(cursor)
        # Make sure to read the (empty) result, some drivers (CMySQLConnector)
        # are picky about that and won't let you close a cursor without reading
        # everything.
        cursor.fetchall()

        for column_descr in columns:
            if column_descr.name.lower() == 'is_empty':
                # Yay, nothing to do.
                return False

        # If we get here, the is_empty column isn't present.
        # Must rename it.
        return True

    _rename_transaction_empty_stmt = 'ALTER TABLE transaction RENAME COLUMN empty TO is_empty'

    # Subclasses can redefine these.
    _slow_zap_all_tbl_stmt = _zap_all_tbl_stmt = 'DELETE FROM "%s"'


    def zap_all(self, reset_oid=True, slow=False):
        """
        Clear all data out of the database.

        :keyword bool slow: If True (*not* the default) then database
            specific optimizations will be skipped and rows will simply be
            DELETEd. This is helpful when other connections might be open and
            holding some kind of locks.
        """
        stmt = self._zap_all_tbl_stmt if not slow else self._slow_zap_all_tbl_stmt

        def zap_all(_conn, cursor):
            existent = set(self.list_tables(cursor))
            to_zap = {} # {normalized_name: recorded_name}
            self._before_zap_all_tables(cursor, existent, slow)
            for possible_table in existent:
                norm_table = self._normalize_schema_object_names([possible_table])[0]
                if norm_table.startswith('temp_'):
                    continue
                if norm_table in self.all_tables:
                    to_zap[norm_table] = possible_table

            # Do in reverse order because that's how Foreign Key
            # constraints are set up.
            for possible_table in reversed(self.all_tables):
                if possible_table in to_zap:
                    table_stmt = stmt % to_zap[possible_table]
                    logger.debug(table_stmt)
                    __traceback_info__ = table_stmt
                    cursor.execute(table_stmt)
            logger.debug("Done deleting from tables.")

            self._after_zap_all_tables(cursor, slow)

            if reset_oid:
                logger.debug("Running OID reset script.")
                self._reset_oid(cursor)
                logger.debug("Done running OID reset script.")

        self.connmanager.open_and_call(zap_all)

    # Hooks for subclasses

    def _before_zap_all_tables(self, cursor, tables, slow=False):
        logger.debug("Before zapping existing tables (%s) with %s; slow: %s",
                     tables, cursor, slow)

    def _after_zap_all_tables(self, cursor, slow=False):
        logger.debug("Running init script. Slow: %s", slow)
        self._init_after_create(cursor)
        logger.debug("Done running init script.")

    DROP_TABLE_TMPL = 'DROP TABLE "{table}"'

    def _drop_all(self, _conn, cursor):
        for view in self.list_views(cursor):
            if view in self.all_views:
                cursor.execute('DROP VIEW %s' % view)

        existent = set(self.list_tables(cursor))
        todo = list(self.all_tables)
        todo.reverse()
        for table in todo:
            if table in existent:
                cursor.execute(self.DROP_TABLE_TMPL.format(table=table))
        for sequence in self.list_sequences(cursor):
            cursor.execute("DROP SEQUENCE %s" % sequence)

    def drop_all(self):
        """Drop all tables and sequences."""
        self.connmanager.open_and_call(self._drop_all)
