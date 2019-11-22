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

from relstorage.interfaces import Tuple
from relstorage.interfaces import Object
from relstorage.interfaces import Bool
from relstorage.interfaces import Factory
from relstorage.interfaces import IException

###
# Abstractions to support multiple databases.
###

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

    cursor_arraysize = Attribute(
        "The value to assign to each new cursor's ``arraysize`` attribute.")

    connect = Attribute("""
    A callable to create and return a new connection object.

    The signature is not specified here because the
    required parameters differ between databases and drivers. The interface
    should be agreed upon between the :class:`IConnectionManager` and
    the drivers for its database.

    This connection, and all objects created from it such as cursors,
    should be used within a single thread only.
    """)

    def cursor(connection, server_side=False):
        """
        Create and return a new cursor sharing the state of the given
        *connection*.

        The cursor should be closed when it is no longer needed. The
        cursor should be considered forward-only (no backward
        scrolling) and ephemeral (results go away when the attached
        transaction is committed or rolled back).

        For compatibility, previous cursors should not have
        outstanding results pending when this is called and while the
        returned cursor is used (not all drivers permit multiple
        active cursors).

        If *server_side* is true (not the default), request that the
        driver creates a cursor that will **not** buffer the complete
        results of a query on the client. Instead, the results should
        be streamed from the server in batches. This can reduce the
        maximum amount of memory needed to handle results, if done
        carefully.

        For compatibility, server_side cursors can only be used
        to execute a single query.

        Most drivers (``psycopg2``, ``psycopg2cffi``, ``pg8000``,
        ``mysqlclient``) default to buffering the entire results
        client side before returning from the ``execute`` method. This
        can reduce latency and increase overall throughput, but at the
        cost of memory, especially if the results will be copied into
        different data structures.

        Not all drivers support server-side cursors; they will ignore
        that request. At this writing, this includes ``pg8000``. Some
        drivers (at this writing, only ``gevent MySQLdb``) always use
        server-side cursors. The ``cx_Oracle`` driver is unevaluated.

        ``psycopg2`` and ``psycopg2cffi`` both iterate in chunks of
        ``cur.itersize`` by default. PyMySQL seems to iterate one row at a time.
        ``mysqlclient`` defaults to also iterating one row at a time, but
        we patch that to operate in chunks of ``cur.arraysize``.
        """

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

    def enter_critical_phase_until_transaction_end(connection, cursor):
        """
        Given a connection and cursor opened by this driver, cause it
        to attempt to raise its priority and return results faster.

        This mostly has meaning for gevent drivers, which may limit
        the amount of time they spend in the hub and the number of context
        switches to other greenlets.

        This phase continues until *after* the ultimate call that
        commits or aborts is sent, but should revert to normal as quickly as
        possible after that. :class:`IRelStorageAdapter` may cooperate with
        the driver using implementation-specific methods to end the phase
        at an appropriate time if there is a hidden commit.

        This method must be idempotent (have the same effect if called more than
        once) within a given transaction.
        """

    def is_in_critical_phase(connection, cursor):
        """
        Answer whether :meth:`enter_critical_phase_until_transaction_end` is in effect.
        """

    def exit_critical_phase(connection, cursor):
        "If currently in a critical phase, de-escalate."

class IDBDriverSupportsCritical(IDBDriver):
    """
    A marker for database drivers that support
    critical phases.

    They promise that :meth:`enter_critical_phase_until_transaction_end`
    will do something useful.
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


###
# Creating and managing DB-API 2.0 connections.
# (https://www.python.org/dev/peps/pep-0249/)
###

class IConnectionManager(Interface):
    """
    Open and close database connections.

    This is a low-level interface; most operations should instead
    use a pre-existing :class:`IManagedDBConnection`.
    """

    isolation_load = Attribute("Default load isolation level.")
    isolation_store = Attribute("Default store isolation level.")
    isolation_read_committed = Attribute("Read committed.")
    isolation_serializable = Attribute("Serializable.")


    def open(
            isolation=None,
            deferrable=False,
            read_only=False,
            replica_selector=None,
            application_name=None,
            **kwargs):
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

    def begin(conn, cursor):
        """
        Call this on a store connection after restarting it.

        This lets the store connection know that it may need to begin a
        transaction, even if it was freshly opened.
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

    def restart_load(conn, cursor, needs_rollback=True):
        """
        Reinitialize a connection for loading objects.

        This gets called when polling the database, so it needs to be
        quick.

        Raise one of self.disconnected_exceptions if the database has
        disconnected.
        """

    def open_for_store(**open_args):
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

    def restart_store(conn, cursor, needs_rollback=True):
        """
        Rollback and reuse a store connection.

        Raise one of self.disconnected_exceptions if the database
        has disconnected.

        You can set *needs_rollback* to false if you're certain
        the connection does not need rolled back.
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

    def open_for_pack_lock():
        """
        Open a connection to be used for the sole purpose of holding
        the pack lock.

        Use a private connection (lock_conn and lock_cursor) to hold
        the pack lock. Have the adapter open temporary connections
        to do the actual work, allowing the adapter to use special
        transaction modes for packing, and to commit at will without
        losing the lock.

        If the database doesn't actually use a pack lock,
        this may return ``(None, None)``.
        """

    def cursor_for_connection(conn):
        """
        If the cursor returned by an open method was discarded
        for state management purposes, use this to get a new cursor.
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

    If the DB-API connection is not open, presumed to be good, and
    previously accessed, this object has a false value.

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

    def enter_critical_phase_until_transaction_end():
        """
        As for :meth:`IDBDriver.enter_critical_phase_until_transaction_end`.
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
        """
        Iterate over the transaction log, newest first.

        Skips packed transactions. Yields (tid, username, description,
        extension) for each transaction.
        """

    def iter_transactions_range(cursor, start=None, stop=None):
        """
        Return an indexable object over the transactions in the given range, oldest
        first.

        Includes packed transactions.

        Has an object with the properties ``tid_int``, ``username``
        (bytes) ``description`` (bytes) ``extension`` (bytes) and
        ``packed`` (boolean) for each transaction.
        """

    def iter_object_history(cursor, oid):
        """
        Iterate over an object's history.

        Yields an object with the properties ``tid_int``, ``username``
        (bytes) ``description`` (bytes) ``extension`` (bytes) and
        ``pickle_size`` (int) for each transaction.

        :raises KeyError: if the object does not exist
        """


class ILocker(Interface):
    """Acquire and release the commit and pack locks."""

    def lock_current_objects(cursor, read_current_oid_ints, shared_locks_block):
        """
        Lock the objects being modified in the current transaction
        exclusively, plus the relevant rows for the objects whose OIDs
        are contained in *read_current_oid_ints* with a read lock.

        The exclusive locks should always be taken in a blocking fashion;
        the shared read locks should be taken without blocking (raising an
        exception if blocking would occur) if possible, unless *shared_locks_block*
        is set to True.

        See :meth:`IRelStorageAdapter.lock_objects_and_detect_conflicts`
        for a description of the expected behaviour.

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

    def restore(cursor, batcher, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.

        batcher is an object returned by self.make_batcher().
        """

    def detect_conflict(cursor):
        """
        Find all conflicts in the data about to be committed (as stored
        by :meth:`store_temps`)

        Returns a sequence of
        ``(oid, committed_tid, attempted_committed_tid, committed_state)`` where
        each entry refers to a conflicting object. The *committed_state* **must** be
        returned.

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

    def reset_oid(cursor):
        """
        Cause the sequence of OIDs to begin again from the beginning.
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

    def pack(pack_tid, packed_func=None):
        """Pack.  Requires the information provided by pre_pack.

        packed_func, if provided, will be called for every object state
        packed, just after the object is removed. The function must
        accept two parameters, oid and tid (64 bit integers).
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

    def get_current_tid(cursor):
        """
        Returns the highest transaction ID visible to the cursor.

        If there are no transactions, returns 0.
        """

    def poll_invalidations(conn, cursor, prev_polled_tid):
        """
        Polls for new transactions.

        *conn* and *cursor* must have been created previously by
        ``open_for_load()`` (a snapshot connection). *prev_polled_tid*
        is the tid returned at the last poll, or None if this is the
        first poll.

        If the database has disconnected, this method should raise one
        of the exceptions listed in the disconnected_exceptions
        attribute of the associated IConnectionManager.

        Returns ``(changes, new_polled_tid)``, where changes is either
        an iterable of (oid, tid) that have changed, or None to
        indicate that the changes are too complex to list; this must
        cause local storage caches to be invalidated.
        ``new_polled_tid`` is never None.

        Important: You must consume the changes iterable, and you must
        not make any other queries until you do.

        This method may raise :class:`StaleConnectionError` (a
        ``ReadConflictError``) if the database has reverted to an
        earlier transaction, which can happen in an asynchronously
        replicated database. This exception is one that is transient
        and most transaction middleware will catch it and retry the
        transaction.
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

    def delete_transaction(cursor, tid):
        """Remove a transaction."""

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

class IDBStats(Interface):
    """
    Collecting, viewing and updating database information.
    """

    def get_object_count():
        """Returns the approximate number of objects in the database"""

    def get_db_size():
        """Returns the approximate size of the database in bytes"""

    def large_database_change():
        """
        Call this when the database has changed substantially,
        and it would be a good time to perform any updates or
        optimizations.
        """


class IRelStorageAdapter(Interface):
    """
    A database adapter for RelStorage.

    Historically, this has just been a holding place for other components
    for the particular database. However, it is moving to holding algorithms
    involved in the storage; this facilitates moving chunks of functionality
    into database stored procedures as appropriate. The basic algorithms are
    implemented in :class:`.adapter.AbstractAdapter`.

    """

    driver = Object(IDBDriver)
    connmanager = Object(IConnectionManager)
    dbiter = Object(IDatabaseIterator)
    keep_history = Bool(description=u"True if this adapter supports undo")
    locker = Object(ILocker)
    mover = Object(IObjectMover)
    oidallocator = Object(IOIDAllocator)
    packundo = Object(IPackUndo)
    poller = Object(IPoller)
    runner = Object(IScriptRunner)
    schema = Object(ISchemaInstaller)
    stats = Object(IDBStats)
    txncontrol = Object(ITransactionControl)

    def new_instance():
        """
        Return an instance for use by another RelStorage instance.

        Adapters that are stateless can simply return self. Adapters
        that have mutable state must make a clone and return it.
        """

    def release():
        """
        Release the resources held uniquely by this instance.
        """

    def close():
        """
        Release the resources held by this instance and all child instances.
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

        When committing, implementations are encouraged to exit any
        :meth:`critical phase <IDBDriver.enter_critical_phase_until_transaction_end>`
        in the most timely manner possible after ensuring that the commit
        request has been sent, especially if only one
        communication with the database is required that may block for an arbitrary
        time to get the lock.

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
        transaction wants to modify (for exclusive update) and the
        objects in *read_current_oids* (for shared read).

        Returns an iterable of ``(oid_int, committed_tid_int,
        tid_this_txn_saw_int, committed_state)`` for current objects
        that were locked, plus objects which had conflicts.

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

        Because two separate classes of locks are to be obtained,
        implementations will typically need to make two separate
        locking queries. If the second of those queries fails, the
        implementation is encouraged to immediately release locks
        taken by the first operation before raising an exception.
        Ideally this happens in the database stored procedure.
        (PostgreSQL works this way automatically --- any error rolls
        the transaction back and releases locks --- but this takes
        work on MySQL except in the case of deadlock.)

        .. rubric:: Read Current Locks

        The *read_current_oids* maps OID integers to expected TID
        integers. Each such object must be read share locked for the
        duration of the transaction so we can ensure that the final
        commit occurs without those objects having been changed. From
        ReadVerifyingStorage's ``checkCurrentSerialInTransaction``:
        "If no [ReadConflictError] exception is raised, then the
        serial must remain current through the end of the
        transaction."

        Applications sometimes (often) perform readCurrent() on the
        *wrong* object (for example: the ``BTree`` object or the
        ``zope.container`` container object, when what is really
        required, what will actually be modified, is a ``BTree``
        bucket---very hard to predict), so very often these objects
        will not ever be modified. (Indeed, ``BTree.__setitem__`` and
        ``__delitem__`` perform readCurrent on the BTree itself; but
        once the first bucket has been established, the BTree will not
        usually be modified.) A share lock is enough to prevent any
        modifications without causing unnecessary blocking if the
        object would never be modified.

        In the results, if the ``tid_this_txn_saw_int`` is ``None``,
        that was an object we only read, and share locked. If the
        ``committed_tid_int`` does not match the TID we expected to
        get, then the caller will raise a ``ReadConflictError`` and
        abort the transaction. The implementation is encouraged to
        return all such rows *first* so that these inexpensive checks
        can be accomplished before the more expensive conflict
        resolution process.

        Optionally, if this method can detect a read current violation
        based on the data in *read_current_oids* at the database
        level, it may raise a :class:`ReadConflictError`. This method
        is also allowed and encouraged to *only* return read current
        violations; further, it need only return the first such
        violation (because an exception will immediately be raised.)

        Implementations are encouraged to use the database ``NOWAIT``
        feature (or equivalent) to take read current locks. If such an
        object is already locked exclusively, that means it is being
        modified and this transaction is racing the modification
        transaction. Taking the lock with ``NOWAIT`` and raising an
        error lets the write transaction proceed, while this one rolls
        back and retries. (Hopefully the write transaction finishes
        quickly, or several retries may be needed.)

        .. rubric:: Modified Objects and Conflicts

        All objects that this transaction intends to modify (which are
        in the ``temp_store`` table) must be exclusively write locked
        when this method returns.

        The remainder of the results (where ``tid_this_txn_saw_int``
        is not ``None``) give objects that we have detected a conflict
        (a modification that has committed earlier to an object this
        transaction also wants to modify).

        As an optimization for conflict resolution,
        ``committed_state`` may give the current committed state of
        the object (corresponding to ``committed_tid_int``), but is
        allowed to be ``None`` if there isn't an efficient way to
        query that in bulk from the database.

        .. rubric:: Deadlocks, and Shared vs Exclusive Locks

        It might seem that, because no method of a transaction
        (*except* for ``restore()``) writes directly to the
        ``object_state`` or ``current_object`` table *before*
        acquiring the commit lock, a share lock is enough even for
        objects we're definitely going to modify. That way leads to
        deadlock, however. Consider this order of operations:

            1. Tx a: LOCK OBJECT 1 FOR SHARE. (Tx a will modify this.)

            2. Tx b: LOCK OBJECT 1 FOR SHARE. (Tx b is just
               readCurrent.)

            3. Tx a: Obtain commit lock.

            4. Tx b: attempt to obtain commit lock; block.

            5. Tx a: UPDATE OBJECT 1; attempt to escalate shared lock
               to exclusive lock. --> DEADLOCK with the shared lock
               from step 2.

        Tx a needs to raise the lock of object 1, but Tx b's share
        lock is preventing it. Meanwhile, Tx b wants the commit lock,
        but Tx a is holding it.

        If Tx a took an exclusive lock, it would either block Tx b
        from getting a share lock, or be blocked by Tx b's share lock;
        either way, whichever one got to the commit lock would be able
        to complete.

        Further, it is trivial to show that when using two lock
        classes, two transactions that have overlapping sets of
        objects (e.g., a wants shared on ``(1, 3, 5, 7)`` and
        exclusive on ``(2, 4, 6, 8)`` and b wants shared on ``(2, 4,
        6, 8)`` and exclusive on ``(3, 5, 7)``), can easily deadlock
        *before* taking the commit lock, no matter how we interleave
        those operations. This is true if they both take their
        exclusive locks first and then attempt share locks on the
        remainder, both take shared locks on everything and attempt to
        upgrade to exclusive on that subset, or both take just the
        shared locks and then attempt to take the exclusive locks.
        This extends to more than two processes.

        **That's perfectly fine.**

        As long as the database either supports ``NOWAIT``
        (immediately error when you fail to get a requested lock) or
        rapid deadlock detection resulting in an error, we can catch
        that error and turn it into the ``ReadConflictError`` it
        actually is.

        PostgreSQL supports ``NOWAIT`` (and deadlock detection, after
        a small but configurable delay). MySQL's InnoDB supports rapid
        deadlock detection, and starting with MySQL 8, it supports
        ``NOWAIT``.

        .. rubric:: Lock Order

        The original strategy was to first take exclusive locks of
        things we will be modifying. Once that succeeds, then we
        attempt shared locks of readCurrent using ``NOWAIT``. If that
        fails because we can't get a lock, we know someone is in the
        process of modifying it and we have a conflict. If we get the
        locks, we still have to confirm the TIDs are the things we
        expect. (A possible optimization is to do those two steps at
        once, in the database. ``SELECT FOR SHARE WHERE oid = X and
        TID = x``. If we don't get the right number of rows,
        conflict.) This prioritizes writers over readers: readers fail
        at the expense of writers.

        However, it means that if we're going to fail a transaction
        because an object we'd like to read lock has been modified, we
        have to wait until we timeout, or acquire all of our exclusive
        locks. Depending on transaction ordering, this could mean
        unnecessarily long delays. Suppose Tx a wants to write to
        objects 1 and 2, and Tx b wants to write to 1 but only read 2.
        (Recall that a ZODB connection automatically upgrades
        readCurrent() into just modifications. 1 and 2 could be bank
        accounts; Tx a is doing a transfer, but Tx b is checking
        collateral (account 2) for a loan that was approved and
        transferring that into 1.)

            1. Tx a: LOCK 1, 2 EXCLUSIVE. (No share locks.)

            2. Tx b: LOCK 1 EXCLUSIVE. -> Block; queue for lock 1.

            3. Tx a: Resolve a conflict in 1, wait for the commit
               lock, and finally commit and release the locks on 1 and

            4. Tx c: LOCK 2 EXCLUSIVE.

            5. Tx b: LOCK 2 SHARE -> wait exception.

        Here, Tx b had to wait while Tx a finished its entire
        business, only to have Tx c swoop in and get the lock first,
        leading to Tx b raising an exception. Some databases guarantee
        that locks are handed off in FIFO fashion, but not all do.
        Even if the database granted Tb b the share lock first, it
        would still discover that the TID had changed and raise an
        exception. Meanwhile, Tx b has been holding an exclusive lock
        on 1 this entire time, preventing anyone else from modifying
        it.

        If we take the share locks first, the scenario looks like
        this:

            1. Tx a: LOCK 1, 2 EXCLUSIVE. (No share locks.)

            2. Tx b: LOCK 2 SHARE -> wait exception; begin retry.

        Tx b gets much quicker notification that it won't be able to
        progress and begins a retry.

        Both orders have the problem that if Tx a takes longer to
        commit than Tx b does to begin its retry, Tx b may take the
        same action against the same state of the database several
        times in a row before giving up. What's different is that in
        the exclusive first version, that only happens if Tx a and Tx
        b have no exclusive locks in common:

            1. Tx a: Lock 1 EXCLUSIVE.

            2. Tx b: Lock 2 EXCLUSIVE.

            3. Tx b: Lock 1 SHARE -> wait exception, begin retry.

        However, this can be mitigated by introducing small backoff
        delays in the transaction retry logic.

        Suppose a different transaction already modified object 2.
        With the original lock order (exclusive first), the scenario
        doesn't change. Tx b has to wait for Tx a to finish before it
        can determine that fact (Tx a has to resolve a conflict or
        fail). If Tx b got to go first, it would relatively quickly
        discover this fact, but at the cost of waiting for an
        exclusive lock for an arbitrary amount of time:

            1. Tx b: Lock 1 EXCLUSIVE. (Potentially blocks.)

            2. Tx a: Lock 1, 2 EXCLUSIVE. -> Block; queue for lock 1.

            3. Tx b: Lock 2 SHARE.

            4. Tx b: Determine 2 has been modified; raise
               ReadConflictError.

        Taking the share lock first solves this concern; Tx b is
        immediately able to determine that 2 has been modified and
        quickly raise an exception without holding any other locks.

        :param cursor: The store cursor.
        :param read_current_oids: A mapping from oid integer to tid
            integer that the transaction expects.
        """


###
# Exceptions
###

class ReplicaClosedException(Exception):
    """The connection to the replica has been closed"""

class UnableToAcquireLockError(Exception):
    "A lock cannot be acquired."

class UnableToAcquireCommitLockError(StorageError, UnableToAcquireLockError):
    """
    The commit lock cannot be acquired due to a timeout.

    This means some other transaction had the lock we needed. Retrying
    the transaction may succeed.

    However, for historical reasons, this exception is not a ``TransientError``.
    """

# TransientError -> ConflictError -> ReadConflictError

class UnableToLockRowsToModifyError(ConflictError, UnableToAcquireLockError):
    """
    We were unable to lock one or more rows that we intend to modify
    due to a timeout.

    This means another transaction already had the rows locked,
    and thus we are in conflict with that transaction. Retrying the
    transaction may succeed.

    This is a type of ``ConflictError``, which is a transient error.
    """

class UnableToLockRowsToReadCurrentError(ReadConflictError, UnableToAcquireLockError):
    """
    We were unable to lock one or more rows that belong to an object
    that ``Connection.readCurrent()`` was called on.

    This means another transaction already had the rows locked with
    intent to modify them, and thus we are in conflict with that
    transaction. Retrying the transaction may succeed.

    This is a type of ``ReadConflictError``, which is a transient error.
    """

class UnableToAcquirePackUndoLockError(StorageError, UnableToAcquireLockError):
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
