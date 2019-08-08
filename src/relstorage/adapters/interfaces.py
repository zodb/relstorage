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
"""Interfaces provided by RelStorage database adapters"""
from __future__ import absolute_import

from ZODB.POSException import StorageError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import ConflictError
from zope.interface import Attribute
from zope.interface import Interface

# pylint:disable=inherit-non-class,no-method-argument,no-self-argument
# pylint:disable=too-many-ancestors,too-many-lines

try:
    from zope.schema import Tuple
    from zope.schema import Object
    from zope.interface.common.interfaces import IException
except ImportError: # pragma: no cover
    # We have nti.testing -> zope.schema as a test dependency; but we
    # don't have it as a hard-coded runtime dependency because we
    # don't want to force a version on consumers of RelStorage.
    def Tuple(*_args, **kwargs):
        return Attribute(kwargs['description'])

    Object = Tuple

    def Factory(schema, description='', **_kw):
        return Attribute(description + " (Must implement %s)" % schema)

    IException = Interface
else:
    from zope.schema.interfaces import SchemaNotProvided as _SchemaNotProvided
    from zope.schema import Field as _Field

    class Factory(_Field):
        def __init__(self, schema, **kw):
            self.schema = schema
            _Field.__init__(self, **kw)

        def _validate(self, value):
            super(Factory, self)._validate(value)
            if not self.schema.implementedBy(value):
                raise _SchemaNotProvided(self.schema, value).with_field_and_value(self, value)



class IRelStorageAdapter(Interface):
    """
    A database adapter for RelStorage.

    Historically, this has just been a holding place for other components
    for the particular database. However, it is moving to holding algorithms
    involved in the storage; this facilitates moving chunks of functionality
    into database stored procedures as appropriate. The basic algorithms are
    implemented in :class:`.adapter.AbstractAdapter`.

    """

    driver = Attribute("The IDBDriver being used")
    connmanager = Attribute("An IConnectionManager")
    dbiter = Attribute("An IDatabaseIterator")
    keep_history = Attribute("True if this adapter supports undo")
    locker = Attribute("An ILocker")
    mover = Attribute("An IObjectMover")
    oidallocator = Attribute("An IOIDAllocator")
    packundo = Attribute("An IPackUndo")
    poller = Attribute("An IPoller")
    runner = Attribute("An IScriptRunner")
    schema = Attribute("An ISchemaInstaller")
    stats = Attribute("An IStats")
    txncontrol = Attribute("An ITransactionControl")

    def new_instance():
        """
        Return an instance for use by another RelStorage instance.

        Adapters that are stateless can simply return self.  Adapters
        that have mutable state must make a clone and return it.
        """

    def __str__():
        """Return a short description of the adapter"""

    def lock_database_and_choose_next_tid(cursor,
                                          username,
                                          description,
                                          extension):
        """
        Lock the database with the commit lock and allocate the next
        tid.

        In a simple implementation, this will first obtain the commit
        lock with around :meth:`ILocker.hold_commit_lock`. Then it
        will query the current most recently committed TID with
        :meth:`ITransactionControl.get_tid`. It will choose the next
        TID based on that value and the current timestamp, and then it
        will write that value to the database with
        :meth:`ITransactionControl.add_transaction`.

        The *username*, *description* and *extension* paramaters are as for
        ``add_transaction.

        :return: The new TID integer.
        """

    def lock_database_and_move(
            store_connection,
            blobhelper,
            ude,
            commit=True,
            committing_tid_int=None,
            after_selecting_tid=None
    ):
        """
        Lock the database, choose the next TID, and move temporary
        data into its final place.

        This is used in two modes. In the usual case, *commit* will be
        true, and this method is called to implement the final step of
        ``tpc_finish``. In that case, this method is responsible for
        committing the transaction (using the *store_connection*
        provided). When *commit* is false, this method is effectively
        part of ``tpc_vote`` and must **not** commit.

        The *blobhelper* is the :class:`IBlobHelper`. This method is
        responsible for moving blob data into place if the blob data
        is stored on the server and there are blobs in this
        transaction. (Implementations that use stored procedures will
        probably not need this argument; it is here to be able to
        provide ``txn_has_blobs`` to
        :meth:`IObjectMover.move_from_temp`.)

        *ude* is a tuple of ``(username, description, extension)``.

        If *committing_tid_int* is None, then this method must lock
        the database and choose the next TID as if by calling
        :meth:`lock_database_and_choose_next_tid` (passing in the
        expanded *ude*); if it is **not** None, the database has
        already been locked and the TID selected.

        *after_selecting_tid* is a function of one argument, the
        committing integer TID. If it is proveded, it must be called
        once the TID has been selected and temporary data moved into
        place.

        Implementations are encouraged to do all of this work in as
        few calls to the database as possible with a stored procedure
        (ideally one call). The default implementation will use
        :meth:`lock_database_and_choose_next_tid`,
        :meth:`IObjectMover.move_from_temp`,
        :meth:`IObjectMover.update_current` and
        :meth:`ITransactionControl.commit_phase1` and
        :meth:`ITransactionControl.commit_phase2`.

        :return: A tuple ``(committing_tid_int, prepared_txn_id)``;
                 the *prepared_txn_id* is irrelevant if *commit* was
                 true.
        """

    def lock_objects_and_detect_conflicts(
            cursor,
            read_current_oids,
    ):
        """
        Without taking the commit lock, lock the objects this
        transaction wants to modify (for update) and the objects in
        *read_current_oids* (for read).

        Returns an iterable of ``(oid_int, committed_tid_int,
        tid_this_txn_saw_int, committed_state)`` for all OIDs that
        were locked (that is, the OIDs that we're modifying plus the
        OIDs in *required_tids*) and which had conflicts.

        If the ``tid_this_txn_saw_int`` is None, that was an object we
        only read, not modified. ``committed_state`` is allowed to be None if
        there isn't an efficient way to query that in bulk from the database.

        Implementations are encouraged to do all this work in as few
        calls to the database as possible with a stored procedure. The
        default implementation will use
        :meth:`ILocker.lock_current_objects`,
        :meth:`IObjectMover.current_object_tids`, and
        :meth:`IObjectMover.detect_conflicts`.

        This method may raise the same lock exceptions and
        :meth:`ILocker.lock_current_objects`. In particular, it should
        take care to distinguish between a failure to acquire an
        update lock and a failure to acquire a read lock by raising
        the appropriate exceptions
        (:class:`UnableToLockRowsToModifyError` and
        :class:`UnableToLockRowsToReadCurrentError`, respectively).

        Optionally, if this method can detect a read current violation
        based on the data in *read_current_oids* at the database
        level, it may raise a :class:`ReadConflictError`.

        :param cursor: The store cursor.
        :param read_current_oids: A mapping from oid integer to tid
            integer that the transaction expects.
        """


class IDBDialect(Interface):
    """
    Handles converting from our internal "standard" SQL queries to
    something database specific.
    """

    # TODO: Fill this in.

class IDBDriver(Interface):
    """
    An abstraction over the information needed for RelStorage to work
    with an arbitrary DB-API driver.
    """

    __name__ = Attribute("The name of this driver")

    disconnected_exceptions = Tuple(
        description=(u"A tuple of exceptions this driver can raise on any operation if it is "
                     u"disconnected from the database."),
        value_type=Factory(IException)
    )

    close_exceptions = Tuple(
        description=(u"A tuple of exceptions that we can ignore when we try to "
                     u"close the connection to the database. Often this is the same "
                     u"or an extension of `disconnected_exceptions`."
                     u"These exceptions may also be ignored on rolling back the connection, "
                     u"if we are otherwise completely done with it and prepared to drop it. "),
        value_type=Factory(IException),
    )

    lock_exceptions = Tuple(
        description=u"A tuple of exceptions",
        value_type=Factory(IException),
    ) # XXX: Document

    use_replica_exceptions = Tuple(
        description=(u"A tuple of exceptions raised by connecting "
                     u"that should cause us to try a replica."),
        value_type=Factory(IException)
    )

    Binary = Attribute("A callable.")

    dialect = Object(IDBDialect, description=u"The IDBDialect for this driver.")

    def binary_column_as_state_type(db_column_data):
        """
        Turn *db_column_data* into something that's a valid pickle
        state.

        Valid pickle states should be acceptable to
        `io.BytesIO` and `pickle.UnPickler`.

        *db_column_dat* came from a column of data declared to be of the
        type that we store state information in (e.g., a BLOB on MySQL
        or Oracle).
        """

    def binary_column_as_bytes(db_column_data):
        """
        Turn *db_column_data* into a `bytes` object.

        Use this when the specific type must be known,
        for example to prefix or suffix additional byte values
        like that produced by `p64`.
        """

class IDBDriverFactory(Interface):
    """
    Information about, and a way to get, an `IDBDriver`
    implementation.
    """

    driver_name = Attribute("The name of this driver produced by this factory.")

    def check_availability():
        """
        Return a boolean indicating whether a call to this factory
        will return a driver (True) or will raise an error (False).
        """

    def __call__(): # pylint:disable=signature-differs
        """
        Return a new `IDBDriver` as represented by this factory.

        If it is not possible to do this, for example because the
        module cannot be imported, raise an `DriverNotAvailableError`.
        """

class DriverNotAvailableError(Exception):
    """
    Raised when a requested driver isn't available.
    """

    #: The name of the requested driver
    driver_name = None

    #: The `IDBDriverOptions` that was asked for the driver.
    driver_options = None

    def __init__(self, driver_name, driver_options=None):
        super(DriverNotAvailableError, self).__init__(driver_name)
        self.driver_name = driver_name
        self.driver_options = driver_options

    def _format_drivers(self):
        driver_factories = getattr(self.driver_options,
                                   'known_driver_factories',
                                   lambda: ())()
        return ' '.join(
            '%r (Module: %r; Available: %s)' % (
                factory.driver_name,
                # This attribute isn't in the interface,
                # it's an extension from AbstractModuleDriver
                getattr(factory, 'MODULE_NAME', '<unknown>'),
                factory.check_availability()
            )
            for factory in driver_factories
        )

    def __str__(self):
        return '%s: Driver %r is not available. Options: %s.' % (
            type(self).__name__, self.driver_name, self._format_drivers()
        )

    __repr__ = __str__


class UnknownDriverError(DriverNotAvailableError):
    """
    Raised when a driver that isn't registered at all is requested.
    """


class NoDriversAvailableError(DriverNotAvailableError):
    """
    Raised when there are no drivers available.
    """

    def __init__(self, driver_name='auto', driver_options=None):
        super(NoDriversAvailableError, self).__init__(driver_name, driver_options)


class IDBDriverOptions(Interface):
    """
    Implemented by a module to provide alternative drivers.
    """

    database_type = Attribute("A string naming the type of database. Informational only.")

    def select_driver(driver_name=None):
        """
        Choose and return an `IDBDriver`.

        The *driver_name* of "auto" is equivalent to a *driver_name* of
        `None` and means to choose the highest priority available driver.
        """

    def known_driver_factories():
        """
        Return an iterable of the potential `IDBDriverFactory`
        objects that can be used by `select_driver`.

        Each driver factory may or may not be available.

        The driver factories are returned in priority order, with the highest priority
        driver being first.
        """


class IConnectionManager(Interface):
    """
    Open and close database connections.

    This is a low-level interface; most operations should instead
    use a pre-existing :class:`IManagedDBConnection`.
    """

    def open():
        """Open a database connection and return (conn, cursor)."""

    def close(conn=None, cursor=None):
        """
        Close a connection and cursor, ignoring certain errors.

        Return a True value if the connection was closed cleanly;
        return a false value if an error was ignored.
        """

    def rollback_and_close(conn, cursor):
        """
        Rollback the connection and close it, ignoring certain errors.

        Certain database drivers, such as MySQLdb using the SSCursor, require
        all cursors to be closed before rolling back (otherwise it generates a
        ProgrammingError: 2014 "Commands out of sync").
        This method abstracts that.

        :return: A true value if the connection was closed without ignoring any exceptions;
            if an exception was ignored, returns a false value.
        """

    def rollback(conn, cursor):
        """
        Like `rollback_and_close`, but without the close, and letting
        errors pass.

        If an error does happen, then the connection and cursor are closed
        before this method returns.
        """

    def rollback_quietly(conn, cursor):
        """
        Like `rollback_and_close`, but without the close.

        :return: A true value if the connection was rolled back without ignoring any exceptions;
            if an exception was ignored, returns a false value (and the connection and cursor
            are closed before this method returns).
        """

    def open_and_call(callback):
        """Call a function with an open connection and cursor.

        If the function returns, commits the transaction and returns the
        result returned by the function.
        If the function raises an exception, aborts the transaction
        then propagates the exception.
        """

    def open_for_load():
        """
        Open a connection for loading objects.

        This connection is read only, and presents a consistent view
        of the database as of the time the first statement is
        executed. It should be opened in ``REPEATABLE READ`` or higher
        isolation level. It must not be in autocommit.

        :return: ``(conn, cursor)``
        """

    def restart_load(conn, cursor):
        """
        Reinitialize a connection for loading objects.

        This gets called when polling the database, so it needs to be
        quick.

        Raise one of self.disconnected_exceptions if the database has
        disconnected.
        """

    def open_for_store():
        """
        Open and initialize a connection for storing objects.

        This connection is read/write, and its view of the database
        needs to be consistent for each statement, but should read a
        fresh snapshot on each statement for purposes of conflict
        resolution and cooperation with other store connections. It
        should be opened in ``READ COMMITTED`` isolation level,
        without autocommit. (Opening in ``REPEATABLE READ`` or higher,
        with a single snapshot, could reduce the use of locks, but
        increases the risk of serialization errors and having
        transactions rollback; we could handle that by raising
        ``ConflictError`` and letting the application retry, but only
        if we did that before ``tpc_finish``, and not all test cases
        can handle that either.)

        This connection will take locks on rows in the state tables,
        and hold them during the commit process.

        A connection opened by this method is the only type of
        connection that can hold the commit lock.

        :return: ``(conn, cursor)``
        """

    def restart_store(conn, cursor):
        """
        Rollback and reuse a store connection.

        Raise one of self.disconnected_exceptions if the database
        has disconnected.
        """

    def open_for_pre_pack():
        """
        Open a connection to be used for the pre-pack phase.

        This connection will make many different queries; each one
        must be consistent unto itself, but they do not all have to be
        consistent with each other. This is because the *first* query
        this object makes establishes a base state, and we will
        manually discard later changes seen in future queries.

        It will read from the state tables and write to the pack tables;
        it will not write to the state tables, nor hold the commit lock.
        It may hold share locks on state rows temporarily.

        This connection may be open for a long period of time, and
        will be committed as appropriate between queries. It is
        acceptable for this connection to be in autocommit mode, if
        required, but it is preferred for it not to be. This should be
        opened in ``READ COMMITTED`` isolation level.

        :return: ``(conn, cursor)``
        """

    def add_on_store_opened(f):
        """
        Add a callable(cursor, restart=bool) for when a store connection
        is opened.

        .. versionadded:: 2.1a1
        """

    def add_on_load_opened(f):
        """
        Add a callable (cursor, restart=bool) for when a load connection is opened.

        .. versionadded:: 2.1a1
        """


class IManagedDBConnection(Interface):
    """
    A managed DB connection consists of a DB-API ``connection`` object
    and a single DB-API ``cursor`` from that connection.

    This encapsulates proper use of ``IConnectionManager``, including
    handling disconnections and re-connecting at appropriate times.

    It is not allowed to use multiple cursors from a connection at the
    same time; not all drivers properly support that.

    If the DB-API connection is not open and presumed to be good, this
    object has a false value.

    "Restarting" a connection means to bring it to a current view of
    the database. Typically this means a rollback so that a new
    transaction can begin with a new MVCC snapshot.
    """

    cursor = Attribute("The DB-API cursor to use. Read-only.")
    connection = Attribute("The DB-API connection to use. Read-only.")

    def __bool__():
        """
        Return true if the database connection is believed to be ready to use.
        """

    def __nonzero__():
        """
        Same as __bool__ for Python 2.
        """

    def drop():
        """
        Unconditionally drop (close) the database connection.
        """

    def rollback_quietly():
        """
        Rollback the connection and return a true value on success.

        When this completes, the connection will be in a neutral state,
        not idle in a transaction.

        If an error occurs during rollback, the connection is dropped
        and a false value is returned.
        """

    def isolated_connection():
        """
        Context manager that opens a new, distinct connection and
        returns its cursor.

        No matter what happens in the ``with`` block, the connection will be
        dropped afterwards.
        """

    def restart_and_call(f, *args, **kw):
        """
        Restart the connection (roll it back) and call a function
        after doing this.

        This may drop and re-connect the connection if necessary.

        :param callable f:
            The function to call: ``f(conn, cursor, *args, **kwargs)``.
            May be called up to twice if it raises a disconnected exception
            on the first try.

        :return: The return value of ``f``.
        """

class IManagedLoadConnection(IManagedDBConnection):
    """
    A managed connection intended for loading.
    """

class IManagedStoreConnection(IManagedDBConnection):
    """
    A managed connection intended for storing data.
    """


class IReplicaSelector(Interface):
    """Selects a database replica"""

    def current():
        """Get the current replica.

        Return a string.  For PostgreSQL and MySQL, the string is
        either a host:port specification or host name.  For Oracle,
        the string is a DSN.
        """

    def next():
        """Return the next replica to try.

        Return None if there are no more replicas defined.
        """


class IDatabaseIterator(Interface):
    """Iterate over the available data in the database"""

    def iter_objects(cursor, tid):
        """Iterate over object states in a transaction.

        Yields (oid, prev_tid, state) for each object state.
        """

    def iter_transactions(cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields (tid, username, description, extension) for each transaction.
        """

    def iter_transactions_range(cursor, start=None, stop=None):
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields (tid, username, description, extension, packed)
        for each transaction.
        """

    def iter_object_history(cursor, oid):
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, state_size)
        for each modification.
        """


class ILocker(Interface):
    """Acquire and release the commit and pack locks."""

    def lock_current_objects(cursor, oids):
        """
        Lock the objects involved in the current transaction, which
        must have been moved into the temporary tables.

        In addition to those, lock the relevant rows for the objects
        whose OIDS are contained in *oids*.

        This should be done as part of the voting phase of TPC, before
        taking out the final commit lock.

        Returns nothing.

        Typically this will be followed by a call to
        :meth:`detect_conflict`.
        """

    def hold_commit_lock(cursor, ensure_current=True, nowait=False):
        """
        Acquire the commit lock.

        If *ensure_current* is True (the default), other tables may be
        locked as well, to ensure the most current data is available.
        When using row level locks, *ensure_current* is always
        implicit.

        With *nowait* set to True, only try to obtain the lock without
        waiting and return a boolean indicating if the lock was
        successful. **Note:** this parameter is deprecated and will be removed
        in the future; it is not currently used.

        Should raise `UnableToAcquireCommitLockError` if the lock can not
        be acquired before a configured timeout.
        """

    def release_commit_lock(cursor):
        """Release the commit lock"""

    def hold_pack_lock(cursor):
        """Try to acquire the pack lock.

        Raise UnableToAcquirePackUndoLockError if packing or undo is already in progress.
        """

    def release_pack_lock(cursor):
        """Release the pack lock."""


class IObjectMover(Interface):
    """Move object states to/from the database and within the database."""

    def load_current(cursor, oid):
        """
        Returns the current state and integer tid for an object.

        *oid* is an integer. Returns (None, None) if object does not
        exist.
        """

    def load_currents(cursor, oids):
        """
        Returns the oid integer, state, and integer tid for all the specified
        objects.

        *oids* is an iterable of integers. If any objects do no exist,
        they are ignored.
        """

    def load_revision(cursor, oid, tid):
        """Returns the state for an object on a particular transaction.

        Returns None if no such state exists.
        """

    def exists(cursor, oid):
        """Returns a true value if the given object exists."""

    def load_before(cursor, oid, tid):
        """Returns the state and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """

    def get_object_tid_after(cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """

    def current_object_tids(cursor, oids):
        """
        Returns the current {oid_int: tid_int} for specified object ids.

        Note that this may be a BTree mapping, not a dictionary.
        """

    def on_store_opened(cursor, restart=False):
        """Create the temporary table for storing objects.

        This method may be None, meaning no store connection
        initialization is required.
        """

    def make_batcher(cursor, row_limit):
        """Return an object to be used for batch store operations.

        *row_limit* is the maximum number of rows to queue before
        calling the database.
        """

    def store_temps(cursor, state_oid_tid_iter):
        """
        Store many objects in the temporary table.

        *batcher* is an object returned by :meth:`make_batcher`.

        *state_oid_tid_iter* is an iterable providing tuples
        ``(data, oid_int, prev_tid_int)``. It is guaranteed that the
        ``oid_int`` values will be distinct. It is further guaranteed that
        this method will not be called more than once in a given transaction;
        further updates to the temporary table will be made using
        ``replace_temps``, which is also only called once.
        """

    def restore(cursor, batcher, oid, tid, data, stmt_buf):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.

        batcher is an object returned by self.make_batcher().
        """

    def detect_conflict(cursor):
        """
        Find all conflicts in the data about to be committed.

        If there is a conflict, returns a sequence of
        ``(oid, committed_tid, attempted_committed_tid, committed_state)``.

        This method should be called during the ``tpc_vote`` phase of a transaction,
        with :meth:`ILocker.lock_current_objects` held.
        """

    def replace_temps(cursor, state_oid_tid_iter):
        """
        Replace all objects in the temporary table with new data from
        *state_oid_tid_iter*.

        This happens after conflict resolution. The param is as for
        ``store_temps``.

        Implementations should try to perform this in as few database operations
        as possible.
        """

    def move_from_temp(cursor, tid, txn_has_blobs):
        """
        Move the temporarily stored objects to permanent storage.

        *tid* is the integer tid of the transaction being committed.

        Returns nothing.

        The steps should be as follows:

            - If we are preserving history, then ``INSERT`` into
              ``object_state`` the values stored in ``temp_store``,
              remembering to coalesce the
              ``LENGTH(temp_store.state)``.

            - Otherwise, when we are not preserving history,
              ``INSERT`` missing rows from ``object_state`` into
              ``temp_store``, and ``UPDATE`` rows that were already
              there. (This is best done with an upsert). If blobs are
              involved, then ``DELETE`` from ``blob_chunk`` where the
              OID is in ``temp_store``.

            - For both types of storage, ``INSERT`` into
              ``blob_chunk`` the values from ``temp_blob_chunk``. In a
              history-free storage, this may be combined with the last
              step in an ``UPSERT``.
        """

    def update_current(cursor, tid):
        """
        Update the current object pointers.

        *tid* is the integer tid of the transaction being committed.

        Returns nothing. This does nothing when the storage is history
        free.

        When the storage preserves history, all the objects in
        ``object_state`` having the given *tid* should have their
        (oid, *tid*) stored into ``current_object``. This can be done
        with a single upsert.

        XXX: Why do we need to look at ``object_state``? Is there a
        reason we can't look at the smaller ``temp_store``? Conflict
        resolution maybe?
        """

    def download_blob(cursor, oid, tid, filename):
        """Download a blob into a file.

        Returns the size of the blob file in bytes.
        """

    def upload_blob(cursor, oid, tid, filename):
        """Upload a blob from a file.

        If tid is None, upload to the temporary table.
        """


class IOIDAllocator(Interface):
    """
    Allocate OIDs and control future allocation.

    The cursor passed here must be from a
    :meth:`store connection <IConnectionManager.open_for_store>`.
    """

    def new_oids(cursor):
        """
        Return a new :class:`list` of new, unused integer OIDs.

        The list should be contiguous and must be in sorted order from
        highest to lowest. It must never contain 0.
        """

    def set_min_oid(cursor, oid_int):
        """
        Ensure the next OID (the rightmost value from
        :meth:`new_oids`) is greater than the given *oid_int*.
        """


class IPackUndo(Interface):
    """Perform pack and undo operations"""

    def verify_undoable(cursor, undo_tid):
        """Raise UndoError if it is not safe to undo the specified txn.
        """

    def undo(cursor, undo_tid, self_tid):
        """Undo a transaction.

        Parameters: "undo_tid", the integer tid of the transaction to undo,
        and "self_tid", the integer tid of the current transaction.

        Returns the states copied forward by the undo operation as a
        list of (oid, old_tid).

        May raise UndoError.
        """

    def fill_object_refs(conn, cursor, get_references):
        """Update the object_refs table by analyzing new transactions.
        """

    def choose_pack_transaction(pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """

    def pre_pack(pack_tid, get_references):
        """Decide what to pack.

        pack_tid specifies the most recent transaction to pack.

        get_references is a function that accepts a stored object state
        and returns a set of OIDs that state refers to.
        """

    def pack(pack_tid, sleep=None, packed_func=None):
        """Pack.  Requires the information provided by pre_pack.

        packed_func, if provided, will be called for every object state
        packed, just after the object is removed. The function must
        accept two parameters, oid and tid (64 bit integers).

        The sleep function defaults to time.sleep(). It can be
        overridden to do something else instead of sleep during
        pauses.
        """

    def deleteObject(cursor, oid_int, tid_int):
        """
        Delete the revision of *oid_int* in transaction *tid_int*.

            This method marks an object as deleted via a new object
            revision. Subsequent attempts to load current data for the
            object will fail with a POSKeyError, but loads for
            non-current data will suceed if there are previous
            non-delete records. The object will be removed from the
            storage when all not-delete records are removed.

            The serial argument must match the most recently committed
            serial for the object. This is a seat belt.

            --- Documentation for ``IExternalGC``

        In history-free databases there is no such thing as a delete record, so
        this should remove the single
        revision of *oid_int* (which *should* be checked to verify it
        is at *tid_int*), leading all access to *oid_int* in the
        future to throw ``POSKeyError``.

        In history preserving databases, this means to set the state for the object
        at the transaction to NULL, signifying that it's been deleted. A subsequent
        pack operation is required to actually remove these deleted items.
        """

class IPoller(Interface):
    """Poll for new data"""

    def poll_invalidations(conn, cursor, prev_polled_tid, ignore_tid):
        """Polls for new transactions.

        conn and cursor must have been created previously by
        open_for_load(). prev_polled_tid is the tid returned at the
        last poll, or None if this is the first poll. If ignore_tid is
        not None, changes committed in that transaction will not be
        included in the list of changed OIDs.

        If the database has disconnected, this method should raise one
        of the exceptions listed in the disconnected_exceptions
        attribute of the associated IConnectionManager.

        Returns (changes, new_polled_tid), where changes is either a
        list of (oid, tid) that have changed, or None to indicate that
        the changes are too complex to list. new_polled_tid is never
        None.

        This method may raise :class:`StaleConnectionError` (a
        ``ReadConflictError``) if the database has reverted to an
        earlier transaction, which can happen in an asynchronously
        replicated database. This exception is one that is transient
        and most transaction middleware will catch it and retry the
        transaction.
        """

    def list_changes(cursor, after_tid, last_tid):
        """
        Return the ``(oid, tid)`` values changed in a range of
        transactions.

        The returned sequence (which has a defined ``len``) must
        include the latest changes in the range *after_tid* < ``tid``
        <= *last_tid*.

        The ``oid`` values returned will be distinct: each ``oid``
        will have been changed in exactly one ``tid``.
        """


class ISchemaInstaller(Interface):
    """Install the schema in the database, clear it, or uninstall it"""

    def prepare():
        """
        Create the database schema if it does not already exist.

        Perform any migration steps needed, and call :meth:`verify`
        before returning.
        """

    def verify():
        """
        Ensure that the schema that's installed can be used by this
        RelStorage.

        If it cannot, for example it's history-preserving and we were configured
        to be history-free, raise an exception.
        """

    def zap_all():
        """Clear all data out of the database."""

    def drop_all():
        """Drop all tables and sequences."""


class IScriptRunner(Interface):
    """Run database-agnostic SQL scripts.

    Using an IScriptRunner is appropriate for batch operations and
    uncommon operations that can be slow, but is not appropriate
    for performance-critical code.
    """

    script_vars = Attribute(
        """A mapping providing replacements for parts of scripts.

        Used for making scripts compatible with databases using
        different parameter styles.
        """)

    def run_script_stmt(cursor, generic_stmt, generic_params=()):
        """Execute a statement from a script with the given parameters.

        generic_params should be either an empty tuple (no parameters) or
        a map.

        The input statement is generic and will be transformed
        into a database-specific statement.
        """

    def run_script(cursor, script, params=()):
        """Execute a series of statements in the database.

        params should be either an empty tuple (no parameters) or
        a map.

        The statements are transformed by run_script_stmt
        before execution.
        """

    def run_many(cursor, stmt, items):
        """Execute a statement repeatedly.  Items should be a list of tuples.

        stmt should use '%s' parameter format (not %(name)s).
        """

    # Note: the Oracle implementation also provides run_lob_stmt, which
    # is useful for reading LOBs from the database quickly.


class ITransactionControl(Interface):
    """Begin, commit, and abort transactions."""

    def get_tid(cursor):
        """Returns the most recent tid."""

    def add_transaction(cursor, tid, username, description, extension,
                        packed=False):
        """Add a transaction."""

    def commit_phase1(store_connection, tid):
        """
        Begin a commit. Returns the transaction name.

        The transaction name must not be None.

        This method should guarantee that :meth:`commit_phase2` will
        succeed, meaning that if commit_phase2() would raise any
        error, the error should be raised in :meth:`commit_phase1`
        instead.

        :param store_connection: An :class:`IManagedStoreConnection`
        """

    def commit_phase2(store_connection, txn):
        """Final transaction commit.

        *txn* is the name returned by commit_phase1.

        :param store_connection: An :class:`IManagedStoreConnection`
        """

    def abort(store_connection, txn=None):
        """
        Abort the commit, ignoring certain exceptions.

        If *txn* is not None, phase 1 is also aborted.

        :param store_connection: An :class:`IManagedStoreConnection`

        :return: A true value if the connection was rolled back
                 without ignoring any exceptions; if an exception was
                 ignored, returns a false value (and the connection
                 and cursor are closed before this method returns).
        """

###
# Exceptions
###

class ReplicaClosedException(Exception):
    """The connection to the replica has been closed"""

class UnableToAcquireCommitLockError(StorageError):
    """
    The commit lock cannot be acquired due to a timeout.

    This means some other transaction had the lock we needed. Retrying
    the transaction may succeed.

    However, for historical reasons, this exception is not a ``TransientError``.
    """

class UnableToLockRowsToModifyError(ConflictError):
    """
    We were unable to lock one or more rows that we intend to modify
    due to a timeout.

    This means another transaction already had the rows locked,
    and thus we are in conflict with that transaction. Retrying the
    transaction may succeed.

    This is a type of ``ConflictError``, which is a transient error.
    """

class UnableToLockRowsToReadCurrentError(ReadConflictError):
    """
    We were unable to lock one or more rows that belong to an object
    that ``Connection.readCurrent()`` was called on.

    This means another transaction already had the rows locked with
    intent to modify them, and thus we are in conflict with that
    transaction. Retrying the transaction may succeed.

    This is a type of ``ReadConflictError``, which is a transient error.
    """

class UnableToAcquirePackUndoLockError(StorageError):
    """A pack or undo operation is in progress."""


class StaleConnectionError(ReadConflictError):
    """
    Raised by `IPoller.poll_invalidations` when a stale connection is
    detected.
    """

    @classmethod
    def from_prev_and_new_tid(cls, prev_polled_tid, new_polled_tid):
        return cls(
            "The database connection is stale: new_polled_tid=%d, "
            "prev_polled_tid=%d." % (new_polled_tid, prev_polled_tid))
