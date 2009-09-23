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

from zope.interface import Attribute
from zope.interface import Interface

class IConnectionManager(Interface):

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

        Raise StorageError if the database has disconnected.
        """

    def open_for_store():
        """Open and initialize a connection for storing objects.

        Returns (conn, cursor).
        """

    def restart_store(conn, cursor):
        """Rollback and reuse a store connection.

        Raise StorageError if the database has disconnected.
        """


class IDatabaseIterator(Interface):

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
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """


class ILocker(Interface):

    def hold_commit_lock(cursor, ensure_current=False):
        """Acquire the commit lock.

        If ensure_current is True, other tables may be locked as well, to
        ensure the most current data is available.

        May raise StorageError if the lock can not be acquired before
        some timeout.
        """

    def release_commit_lock(cursor):
        """Release the commit lock"""

    def hold_pack_lock(cursor):
        """Try to acquire the pack lock.

        Raise StorageError if packing or undo is already in progress.
        """

    def release_pack_lock(cursor):
        """Release the pack lock."""


class IOIDAllocator(Interface):

    def set_min_oid(cursor, oid):
        """Ensure the next OID is at least the given OID."""

    def new_oid(cursor):
        """Return a new, unused OID."""


class IPackUndo(Interface):

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

    def open_for_pre_pack():
        """Open a connection to be used for the pre-pack phase.

        Returns (conn, cursor).
        """

    def fill_object_refs(conn, cursor, get_references):
        """Update the object_refs table by analyzing new transactions.
        """

    def choose_pack_transaction(pack_point):
        """Return the transaction before or at the specified pack time.

        Returns None if there is nothing to pack.
        """

    def pre_pack(pack_tid, get_references, options):
        """Decide what to pack.

        pack_tid specifies the most recent transaction to pack.

        get_references is a function that accepts a pickled state and
        returns a set of OIDs that state refers to.

        options is an instance of relstorage.Options.
        In particular, the options.pack_gc flag indicates whether
        to run garbage collection.
        """

    def pack(pack_tid, options, sleep=None, packed_func=None):
        """Pack.  Requires the information provided by pre_pack.

        packed_func, if provided, will be called for every object
        packed, just after it is removed.  The function must accept
        two parameters, oid and tid (64 bit integers).

        The sleep function defaults to time.sleep(). It can be
        overridden to do something else instead of sleep during pauses
        configured by the duty cycle.
        """


class IPoller(Interface):

    def poll_invalidations(conn, cursor, prev_polled_tid, ignore_tid):
        """Polls for new transactions.

        conn and cursor must have been created previously by open_for_load().
        prev_polled_tid is the tid returned at the last poll, or None
        if this is the first poll.  If ignore_tid is not None, changes
        committed in that transaction will not be included in the list
        of changed OIDs.

        Returns (changed_oids, new_polled_tid).
        """


class ISchemaInstaller(Interface):

    def create(cursor):
        """Create the database tables, sequences, etc."""

    def prepare():
        """Create the database schema if it does not already exist."""

    def zap_all():
        """Clear all data out of the database."""

    def drop_all():
        """Drop all tables and sequences."""


class IScriptRunner(Interface):

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


class ITransactionControl(Interface):

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

    def get_tid_and_time(cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """

    def add_transaction(cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""

