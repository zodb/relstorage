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
"""ZODB storage conversion utility.

See README.txt for details.
"""
from __future__ import print_function

import logging
import optparse
from persistent.TimeStamp import TimeStamp
from StringIO import StringIO
import sys
import ZConfig
from ZODB.utils import p64, u64, z64, readable_tid_repr

schema_xml = """
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
        if hasattr(i, 'next'):
            # New iterator API
            i.next()
        else:
            # Old index lookup API
            i[0]
    except (IndexError, StopIteration):
        return False
    return True


class _DefaultStartStorageIteration(object):
    # At IStorageIteration instance that keeps a default start value.
    # This is needed because RelStorage.iterator() does return an object with an
    # iterator() method, but that object returns itself, so it can only be iterated
    # once! This breaks some implementations of copyTransactionsFrom, notably
    # our own. See #22

    def __init__(self, source, start):
        self._source = source
        self._start = start

    def iterator(self, start=None, end=None):
        return self._source.iterator(start or self._start, end)

    def __getattr__(self, name):
        return getattr(self._source, name)

def main(argv=sys.argv):
    parser = optparse.OptionParser(description=__doc__,
        usage="%prog [options] config_file")
    parser.add_option(
        "--dry-run", dest="dry_run", action="store_true",
        help="Attempt to open the storages, then explain what would be done")
    parser.add_option(
        "--clear", dest="clear", action="store_true",
        help="Clear the contents of the destination storage before copying")
    parser.add_option(
        "--incremental", dest="incremental", action="store_true",
        help="Assume the destination contains a partial copy of the source "
             "and resume copying from the last transaction. WARNING: no "
             "effort is made to verify that the destination holds the same "
             "transaction data before this point! Use at your own risk. "
             "Currently only supports RelStorage destinations.")
    parser.set_defaults(dry_run=False, clear=False)
    options, args = parser.parse_args(argv[1:])

    if len(args) != 1:
        parser.error("The name of one configuration file is required.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, handler = ZConfig.loadConfig(schema, args[0])
    source = config.source.open()
    destination = config.destination.open()

    log.info("Storages opened successfully.")

    if options.incremental:
        if not hasattr(destination, 'lastTransaction'):
            msg = ("Error: no API is known for determining the last committed "
                   "transaction of the destination storage. Aborting "
                   "conversion.")
            sys.exit(msg)
        if not storage_has_data(destination):
            log.warning("Destination empty, start conversion from the beginning.")
        else:
            # This requires that the storage produce a valid (not z64) value before
            # anything is loaded with it.
            last_tid = destination.lastTransaction()
            if isinstance(last_tid, bytes):
                # This *should* be a byte string.
                last_tid = u64(last_tid)

            next_tid = p64(last_tid+1)
            # Compensate for the RelStorage bug(?) and get a reusable iterator
            # that starts where we want it to. There's no harm in wrapping it for
            # other sources like FileStorage too.
            source = _DefaultStartStorageIteration(source, next_tid)
            log.info("Resuming ZODB copy from %s", readable_tid_repr(next_tid))


    if options.dry_run:
        log.info("Dry run mode: not changing the destination.")
        if storage_has_data(destination):
            log.warning("The destination storage has data.")
        count = 0
        for txn in source.iterator():
            log.info('%s user=%s description=%s' % (
                TimeStamp(txn.tid), txn.user, txn.description))
            count += 1
        log.info("Would copy %d transactions.", count)

    else:
        if options.clear:
            log.info("Clearing old data...")
            if hasattr(destination, 'zap_all'):
                destination.zap_all()
            else:
                msg = ("Error: no API is known for clearing this type "
                       "of storage. Use another method.")
                sys.exit(msg)
            log.info("Done clearing old data.")

        if storage_has_data(destination) and not options.incremental:
            msg = "Error: the destination storage has data.  Try --clear."
            sys.exit(msg)

        destination.copyTransactionsFrom(source)
        source.close()
        destination.close()


if __name__ == '__main__':
    main()
