#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
ZODB storage conversion utility.
"""
from __future__ import print_function

import argparse
import logging
import sys
from io import StringIO

import ZConfig
from zope.interface import implementer

from ZODB import loglevels
from ZODB.utils import p64
from ZODB.utils import readable_tid_repr
from ZODB.utils import u64
from ZODB.interfaces import IStorageIteration
from ZODB.interfaces import IStorageCurrentRecordIteration

schema_xml = u"""
<schema>
  <import package="ZODB"/>
  <import package="relstorage"/>
  <section type="ZODB.storage" name="source" attribute="source"
    required="yes" />
  <section type="ZODB.storage" name="destination" attribute="destination"
    required="yes" />
</schema>
"""

log = logging.getLogger("zodbconvert")


def storage_has_data(storage):
    i = storage.iterator()
    try:
        try:
            next(i)
        except (IndexError, StopIteration):
            return False
        return True
    finally:
        if hasattr(i, 'close'):
            i.close()

@implementer(IStorageIteration)
class _DefaultStartStorageIteration(object):
    # At IStorageIteration instance that keeps a default start value.
    # This is needed because RelStorage.iterator() does return an object with an
    # iterator() method, but that object returns itself, so it can only be iterated
    # once! This breaks some implementations of copyTransactionsFrom, notably
    # our own. See #22

    # By having our own ``@implementer`` declaration, we get our own
    # __providedBy__/__provides__ attributes. Otherwise, we would "inherit"
    # the value from the source storage, and claim to implement things we really don't.

    def __init__(self, source, start):
        self.__source = source
        self.__start = start

    def __len__(self): # pragma: no cover
        return len(self.__source)

    def __repr__(self):
        return "<%s.%s object at 0x%x wrapping %r>" % (
            type(self).__module__,
            type(self).__name__,
            id(self),
            self.__source,
        )

    def iterator(self, start=None, end=None):
        return self.__source.iterator(start or self.__start, end)

    def __getattr__(self, name):
        return getattr(self.__source, name)

def open_storages(options):
    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, _ = ZConfig.loadConfigFile(schema, options.config_file)
    source = config.source.open()
    destination = config.destination.open()

    return source, destination

def main(argv=None):
    # pylint:disable=too-many-branches,too-many-statements
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        default=False,
        help="Attempt to open both storages, then explain what would be done.")
    parser.add_argument(
        "--clear", dest="clear", action="store_true",
        default=False,
        help="Clear the contents of the destination storage before copying."
             " Only works if the destination is a RelStorage."
             " WARNING: use this only if you are certain the destination has no useful data.")
    parser.add_argument(
        "--incremental", dest="incremental", action="store_true",
        help="Assume the destination contains a partial copy of the source "
             "and resume copying from the last transaction. WARNING: no "
             "effort is made to verify that the destination holds the same "
             "transaction data before this point! Use at your own risk. ")
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument(
        '--debug', dest="log_level", action='store_const',
        const=logging.DEBUG,
        default=logging.INFO,
        help="Set the logging level to DEBUG instead of the default of INFO."
    )
    log_group.add_argument(
        '--trace', dest="log_level", action='store_const',
        const=loglevels.TRACE,
        default=logging.INFO,
        help="Set the logging level to TRACE instead of the default of INFO."
    )
    parser.add_argument("config_file", type=argparse.FileType('r'))

    options = parser.parse_args(argv[1:])

    logging.basicConfig(
        level=options.log_level,
        format="%(asctime)s [%(name)s] %(levelname)-6s %(message)s"
    )

    source, destination = open_storages(options)

    def cleanup_and_exit(exit_msg=None):
        source.close()
        destination.close()
        if exit_msg:
            sys.exit(exit_msg)

    log.info("Storages opened successfully.")

    if options.dry_run and options.clear:
        cleanup_and_exit("Cannot clear a storage during a dry-run")

    if options.clear:
        log.info("Clearing old data...")
        if hasattr(destination, 'zap_all'):
            destination.zap_all()
        else: # pragma: no cover
            msg = ("Error: no API is known for clearing this type "
                   "of storage. Use another method.")
            cleanup_and_exit(msg)
        log.info("Done clearing old data.")

    if options.incremental:
        assert hasattr(destination, 'lastTransaction'), (
            "Error: no API is known for determining the last committed "
            "transaction of the destination storage. Aborting "
            "conversion.")

        if storage_has_data(destination):
            # This requires that the storage produce a valid (not z64) value before
            # anything is loaded with it.
            last_tid = destination.lastTransaction()
            if isinstance(last_tid, bytes):
                # This *should* be a byte string.
                last_tid = u64(last_tid)

            next_tid = p64(last_tid + 1)
            # Compensate for the RelStorage bug(?) and get a reusable iterator
            # that starts where we want it to. There's no harm in wrapping it for
            # other sources like FileStorage too.
            #
            # Note that this disables access to ``record_iternext``,
            # defeating some of the optimizations our own copyTransactionsFrom
            # would like to do.
            # XXX: Figure out an incremental way to do this.
            log.info("Resuming ZODB copy from %s", readable_tid_repr(next_tid))
        else:
            log.warning("Destination empty; Forcing "
                        "incremental conversion from the beginning. "
                        "If the destination is a history-free RelStorage, this "
                        "will take much longer.")
            # Use the DefaultStartStorageIteration for its side-effect of disabling
            # record_iternext
            next_tid = None

        source = _DefaultStartStorageIteration(source, next_tid)
        assert not IStorageCurrentRecordIteration.providedBy(source)

    if options.dry_run:
        log.info("Dry run mode: not changing the destination.")
        if storage_has_data(destination):
            log.warning("The destination storage has data.")
        count = 0
        for txn in source.iterator():
            log.info('%s user=%s description=%s',
                     readable_tid_repr(txn.tid), txn.user, txn.description)
            count += 1
        log.info("Would copy %d transactions.", count)
        cleanup_and_exit()
    else:
        if storage_has_data(destination) and not options.incremental:
            msg = "Error: the destination storage has data.  Try --clear."
            cleanup_and_exit(msg)

        try:
            destination.copyTransactionsFrom(source)
        finally:
            cleanup_and_exit()


if __name__ == '__main__':
    main()
