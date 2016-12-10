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

from ZODB.POSException import StorageError
from zope.interface import Attribute
from zope.interface import Interface

#pylint: disable=inherit-non-class,no-method-argument,no-self-argument

class IRelStorageAdapter(Interface):
    """A database adapter for RelStorage"""

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
        """Return an instance for use by another RelStorage instance.

        Adapters that are stateless can simply return self.  Adapters
        that have mutable state must make a clone and return it.
        """

    def __str__():
        """Return a short description of the adapter"""

class IDBDriver(Interface):
    """
    An abstraction over the information needed for RelStorage to work
    with an arbitrary DB-API driver.
    """

    __name__ = Attribute("The name of this driver")

    disconnected_exceptions = Attribute("A tuple of exceptions this driver can raise if it is "
                                        "disconnected from the database.")
    close_exceptions = Attribute("A tuple of exceptions that we can ignore when we try to "
                                 "close the connection to the database. Often this is the same "
                                 "or an extension of `disconnected_exceptions`.")

    lock_exceptions = Attribute("A tuple of exceptions") # XXX: Document

    use_replica_exceptions = Attribute("A tuple of exceptions raised by connecting "
                                       "that should cause us to try a replica.")

    Binary = Attribute("A callable.")


class IDBDriverOptions(Interface):
    """
    Implemented by a module to provide alternative drivers.
    """

    database_type = Attribute("A string naming the type of database.")

    driver_map = Attribute("A map of driver names to IDBDriver instances. "
                           "If the map is empty, no drivers for the specified "
                           "database are available.")

    preferred_driver_name = Attribute("The name of the best driver in driver_map "
                                      "to use. None if no drivers are available.")

    def connect(*args, **kwargs):
        """
        Return a new database connection.
        """

class IConnectionManager(Interface):
    """Open and close database connections"""

    disconnected_exceptions = Attribute(
        """The tuple of exception types that might be
        raised when the connection to the database has been broken.
        """)

    def open():
        """Open a database connection and return (conn, cursor)."""

    def close(conn, cursor):
        """Close a connection and cursor, ignoring certain errors.
        """

    def open_and_call(callback):
        """Call a function with an open connection and cursor.

        If the function returns, commits the transaction and returns the
        result returned by the function.
        If the function raises an exception, aborts the transaction
        then propagates the exception.
        """

    def open_for_load():
        """Open a connection for loading objects.

        Returns (conn, cursor).
        """

    def restart_load(conn, cursor):
        """Reinitialize a connection for loading objects.

        This gets called when polling the database, so it needs to be quick.

        Raise one of self.disconnected_exceptions if the database has
        disconnected.
        """

    def open_for_store():
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """

    def restart_store(conn, cursor):
        """Rollback and reuse a store connection.

        Raise one of self.disconnected_exceptions if the database
        has disconnected.
        """

    def open_for_pre_pack():
        """Open a connection to be used for the pre-pack phase.

        Returns (conn, cursor).
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

    def hold_commit_lock(cursor, ensure_current=False, nowait=False):
        """Acquire the commit lock.

        If ensure_current is True, other tables may be locked as well, to
        ensure the most current data is available.

        May raise UnableToAcquireCommitLockError if the lock can not be acquired before
        some timeout.

        With nowait set to True, only try to obtain the lock without waiting
        and return a boolean indicating if the lock was successful.

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
        """Returns the current state and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
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
        """Returns the current {oid: tid} for specified object ids."""

    def on_store_opened(cursor, restart=False):
        """Create the temporary table for storing objects.

        This method may be None, meaning no store connection
        initialization is required.
        """

    def make_batcher(cursor, row_limit):
        """Return an object to be used for batch store operations.

        row_limit is the maximum number of rows to queue before
        calling the database.
        """

    def store_temp(cursor, batcher, oid, prev_tid, data):
        """Store an object in the temporary table.

        batcher is an object returned by self.make_batcher().
        """

    def restore(cursor, batcher, oid, tid, data, stmt_buf):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.

        batcher is an object returned by self.make_batcher().
        """

    def detect_conflict(cursor):
        """Find all conflict in the data about to be committed.

        If there is a conflict, returns a sequence of (oid, prev_tid, attempted_prev_tid).
        """

    def replace_temp(cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """

    def move_from_temp(cursor, tid, txn_has_blobs):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """

    def update_current(cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
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
    """Allocate OIDs and control future allocation"""

    def new_oids(cursor):
        """Return a sequence of new, unused OIDs."""

    def set_min_oid(cursor, oid):
        """Ensure the next OID is at least the given OID."""


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


class IPoller(Interface):
    """Poll for new data"""

    def poll_invalidations(conn, cursor, prev_polled_tid, ignore_tid):
        """Polls for new transactions.

        conn and cursor must have been created previously by open_for_load().
        prev_polled_tid is the tid returned at the last poll, or None
        if this is the first poll.  If ignore_tid is not None, changes
        committed in that transaction will not be included in the list
        of changed OIDs.

        If the database has disconnected, this method should raise one
        of the exceptions listed in the disconnected_exceptions
        attribute of the associated IConnectionManager.

        Returns (changes, new_polled_tid), where changes is either
        a list of (oid, tid) that have changed, or None to indicate
        that the changes are too complex to list.  new_polled_tid is
        never None.

        This method may raise ReadConflictError if the database has
        reverted to an earlier transaction, which can happen
        in an asynchronously replicated database.
        """

    def list_changes(cursor, after_tid, last_tid):
        """Return the (oid, tid) values changed in a range of transactions.

        The returned iterable must include all changes in the range
        after_tid < tid <= last_tid.
        """


class ISchemaInstaller(Interface):
    """Install the schema in the database, clear it, or uninstall it"""

    def create(cursor):
        """Create the database tables, sequences, etc."""

    def prepare():
        """Create the database schema if it does not already exist."""

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

    def commit_phase1(conn, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        The transaction name must not be None.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """

    def commit_phase2(conn, cursor, txn):
        """Final transaction commit.

        txn is the name returned by commit_phase1.
        """

    def abort(conn, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""


class ReplicaClosedException(Exception):
    """The connection to the replica has been closed"""

class UnableToAcquireCommitLockError(StorageError):
    """The commit lock cannot be acquired."""

class UnableToAcquirePackUndoLockError(StorageError):
    """A pack or undo operation is in progress."""
