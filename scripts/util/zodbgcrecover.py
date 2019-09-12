#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A script to assist in the recovery of a history-free storage damaged
by incorrect garbage collection, such as that described in
https://github.com/zodb/relstorage/issues/344.

.. note:: This does not work with multi-databases.

The script takes as input a ZConfig configuration file just like
zodbconvert does:

    - The *source* database is the backup. Objects to be restored will
      come from here.

    - The *destination* database is the damaged database. Objects will
      be restored into this database.

It also needs a list of missing OIDs (oids that generate POSKeyError).
These may or may not exist in the *source* database. This should be
given on stdin, one missing OID per line, in integer ('u64' or
'bytes8_to_int64') format. As a special case, piping the output from
zc.zodbdgc's ``multi-zodb-check-refs`` script is supported. It may
also be given from a file specified with ``--oid-list``.

The strategy uses the same underlying infrastructure that zodbconvert
does, so it is tested and stable, if not necessarily fast. The
difference is that it inserts a filter into the process. It relies on
the fact that a history-free storage can add objects to a transaction,
and commit for the same TID, multiple times.

    1. First, open both databases.

    2. Read all input OIDs into a set.

    3. Ask the source destination to return the current state and tid
       for those OIDs. (TODO: Batches if this could be large.)

    3b. Ask the destination for the same; if anything is present here, it
        should either be the same or a higher TID. Remove duplicates.

    4. Produce an iterator of transaction records that returns this
       data, sorted and grouped by TID. Wrap this in a phony
       storage-like object.

    5. Pass this to destination.copyTransactionsFrom().

    6. Profit.

The original input list forms the starting "roots". Because the missing
objects may themselves refer to other objects which were also
incorrectly garbage collected (because their only referer was this
object) to an arbitrary depth, the above steps from 3 to 5 must be repeated
using as input the list of missing references defined by all the stats collected
in stage 3 of the previous iteration. Care is taken to avoid running into
problems with references that the source database simply doesn't have.
"""
# TODO: Should we disable the taking of the commit lock; we probably
# don't need it.

# TODO: We actually probably don't really need to even preserve the
# transaction ids and group them in batches that way. We could just
# choose the earliest TID and use that for all of them. That could
# help speed wise. But for best fidelity, it's good to restore them.
# That should keep batches small.

# TODO: When we implement https://github.com/zodb/relstorage/issues/275
# and our own check-refs, optionally use that directly.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import sys
import zlib

from ZODB.BaseStorage import DataRecord
from ZODB.BaseStorage import TransactionRecord
from ZODB.POSException import POSKeyError
from ZODB.blob import is_blob_record
# This function only returns strong same-database references,
# ignoring weak and cross-database references. We shouldn't have
# the latter, and the former were weak so if they're gone that's
# ok.
from ZODB.serialize import referencesf as get_oids_referenced_by_pickle

from relstorage.zodbconvert import open_storages
from relstorage.interfaces import IRelStorage
from relstorage._compat import OID_SET_TYPE as OidSet
from relstorage._compat import OID_OBJECT_MAP_TYPE as OidMap
from relstorage._compat import OidSet_difference
from relstorage._compat import OidSet_discard
from relstorage._util import int64_to_8bytes
from relstorage._util import bytes8_to_int64

logger = logging.getLogger(__name__)

def check_db_compat(storage):
    assert IRelStorage.providedBy(storage)
    assert not storage._options.keep_history

def read_missing_oids(oid_lines):
    """
    Parse lines into oids.

    >>> list(read_missing_oids([
    ...   "!!! Users 0 ?", "POSKeyError: foo",
    ...   "!!! Users 0 ?",
    ...   "!!! Users 1 ?",
    ...   "bad xref name, 1", "bad db",  ]))
    [0, 1]
    """
    result = OidSet()
    for line in oid_lines:
        if line.startswith('bad') or ':' in line:
            continue
        if line.startswith('!!!'):
            # zc.zodbdgc output. bad OID is always the
            # third field.
            try:
                oid = int(line.split()[2])
            except (ValueError, IndexError):
                logger.info("Malformed zc.zodbdgc input: %s", line)
                continue
            result.add(oid)
        else:
            # Just an int
            try:
                oid = int(line)
            except ValueError:
                logger.info("Malformed input: %s", line)
            else:
                result.add(oid)
    if oid_lines != sys.stdin:
        oid_lines.close()
    return result

class MissingObjectStorage(object):
    """
    The thing we pass to copyTransactionsFrom().
    """

    def __init__(self, backup_state_by_oid, tids_to_oids, backup_storage):
        self._states = backup_state_by_oid
        self._transaction_groups = tids_to_oids
        self._backup_storage = backup_storage

    def __getattr__(self, name):
        return getattr(self._backup_storage, name)

    def close(self):
        pass

    def iterator(self):
        return TransactionRecordIterator(self._states, self._transaction_groups)

class TransactionRecordIterator(object):

    def __init__(self, backup_state_by_oid, tids_to_oids):
        self._states = backup_state_by_oid
        self._len = len(tids_to_oids)
        self._transaction_group_iter = iter(tids_to_oids.items())

    def close(self):
        pass

    def __len__(self):
        return self._len

    def __iter__(self):
        return self

    def __next__(self):
        tid, oids = next(self._transaction_group_iter)
        return RelStorageTransactionRecord(tid, oids, self._states)

    next = __next__ # Python 2

class RelStorageTransactionRecord(TransactionRecord):
    def __init__(self, tid_int, oid_ints, states):
        tid = int64_to_8bytes(tid_int)
        self._oid_ints = oid_ints
        self._states = states
        TransactionRecord.__init__(self, tid, 'p', b'', b'', {})

    def __iter__(self):
        logger.info("Restoring %d oids for tid %d",
                    len(self._oid_ints),
                    bytes8_to_int64(self.tid))
        return RecordIterator(self.tid, self._oid_ints, self._states)

class RecordIterator(object):

    def __init__(self, tid, oid_ints, states):
        self._tid = tid
        self._oid_int_iter = iter(oid_ints)
        self._states = states

    def __next__(self):
        oid_int = next(self._oid_int_iter)
        state, _ = self._states[oid_int]
        logger.info("Restoring OID %d at TID %s (state size %d)",
                    oid_int, bytes8_to_int64(self._tid), len(state))
        return DataRecord(
            int64_to_8bytes(oid_int),
            self._tid,
            state,
            None
        )

    next = __next__ # Python 2

def _find_missing_references_from_pickles(destination, pickles, permanently_gone):
    # Return a set of objects missing from the database given the
    # pickle states of other objects.
    # *permanently_gone* is a set of oid byte strings that are
    # known to be missing and shouldn't be investigated and returned.
    oids = []
    for pickle in pickles:
        # Support zc.zlibstorage wrappers.
        if pickle.startswith(b'.z'):
            pickle = zlib.decompress(pickle[2:])
        get_oids_referenced_by_pickle(pickle, oids)

    logger.info(
        "Given %d pickles, there are %d unique references.",
        len(pickles), len(oids)
    )

    missing_oids = OidSet()
    destination.prefetch(oids)
    for oid in oids:
        if oid in permanently_gone:
            continue
        try:
            state, tid = destination.load(oid, b'')
            if is_blob_record(state):
                destination.loadBlob(oid, tid)
        except POSKeyError:
            missing_oids.add(bytes8_to_int64(oid))
    logger.info(
        "Given %d pickles, there are %d missing references.",
        len(pickles), len(missing_oids)
    )
    return missing_oids

def _restore_missing_oids(source, destination, missing_oids, permanently_gone):
    # Implements steps 3 through 5.

    # Returns a set of oid ints that are missing from the database
    # considering only the objects that were restored.

    # 3. Get backup data
    # (oid, state, tid)
    backup_data = source._adapter.mover.load_currents(
        source._load_connection.cursor,
        missing_oids
    )

    # {oid: (state, tid)}
    backup_state_by_oid = OidMap()
    tids_to_oids = OidMap()
    for oid, state, tid in backup_data:
        if tid not in tids_to_oids:
            tids_to_oids[tid] = OidSet()
        tids_to_oids[tid].add(oid)
        backup_state_by_oid[oid] = (state, tid)

    found_oids = OidSet(backup_state_by_oid)
    oids_required_but_not_in_backup = OidSet_difference(missing_oids, found_oids)
    if oids_required_but_not_in_backup:
        logger.warning(
            "The backup storage is missing %d OIDs needed",
            len(oids_required_but_not_in_backup)
        )
        permanently_gone.update(oids_required_but_not_in_backup)
    del found_oids
    del oids_required_but_not_in_backup

    # 3b. Compare with destination.
    current_data = destination._adapter.mover.load_currents(
        destination._load_connection.cursor,
        backup_state_by_oid
    )
    current_data = list(current_data)
    for oid, _, tid in current_data:
        if oid not in backup_state_by_oid:
            # Common, expected case.
            continue

        _, backup_tid = backup_state_by_oid[oid]

        logger.warning(
            "Destination already contains data for %d. Check your OID list. "
            "Refusing to overwrite. (Source TID: %d; Destination TID: %d)",
            oid, backup_tid, tid
        )
        # If we're doing a dry-run, it's probably in here.
        OidSet_discard(permanently_gone, oid)
        del backup_state_by_oid[oid]
        tids_to_oids[backup_tid].remove(oid)
        if not tids_to_oids[backup_tid]:
            del tids_to_oids[backup_tid]
        continue

    if not tids_to_oids:
        logger.warning("All OIDs already present in destination; nothing to do.")
        return

    # 4. Produce phony storage that iterates the backup data.
    logger.info(
        "Beginning restore of %d OIDs in %d transactions.",
        len(backup_state_by_oid),
        len(tids_to_oids)
    )
    copy_from = MissingObjectStorage(backup_state_by_oid, tids_to_oids, source)

    # 5. Hand it over to be copied.
    destination.copyTransactionsFrom(copy_from)

    # Find anything still missing, after having stored what we could.
    newly_missing = _find_missing_references_from_pickles(
        destination,
        [x[0] for x in backup_state_by_oid.values()],
        permanently_gone)
    return newly_missing

def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    argv = argv if argv is not None else sys.argv

    # 1. Open the source and destination.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        default=False,
        help="Attempt to open both storages, then explain what would be done."
    )
    parser.add_argument(
        '--oid-list', dest='oid_list', type=argparse.FileType('r'),
        default='-',
        help="Where to read the list of OIDs from. "
             "Defaults to stdin for use in a pipe from zc.zodbgc."
    )
    parser.add_argument("config_file", type=argparse.FileType('r'))

    options = parser.parse_args(argv[1:])
    permanently_gone = OidSet()

    source, destination = open_storages(options)
    check_db_compat(source)
    check_db_compat(destination)

    if options.dry_run:
        # Make sure we can't commit.
        destination.tpc_finish = destination.tpc_abort


    # 2. Read incoming OIDs.
    missing_oids = read_missing_oids(options.oid_list)
    if not missing_oids:
        sys.exit("Unable to read any missing OIDs.")

    if options.dry_run:
        # And since we can't commit, the starting set of objects
        # known to be permanently gone are...the set we start with!
        permanently_gone.update(missing_oids)

    try:
        while missing_oids:
            missing_oids = _restore_missing_oids(source, destination,
                                                 missing_oids, permanently_gone)
    finally:
        source.close()
        destination.close()

    if permanently_gone:
        logger.warning(
            "The following referenced OIDs could not be recovered: %s",
            list(permanently_gone)
        )

if __name__ == '__main__':
    main()
