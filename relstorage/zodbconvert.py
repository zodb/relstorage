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

import logging
import optparse
from persistent.TimeStamp import TimeStamp
from StringIO import StringIO
import sys
import ZConfig
from ZODB.utils import p64, readable_tid_repr

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
        if not hasattr(destination, '_adapter'):
            msg = ("Error: no API is known for determining the last committed "
                   "transaction of the destination storage. Aborting "
                   "conversion.")
            sys.exit(msg)
        if not storage_has_data(destination):
            log.warning("Destination empty, start conversion from the beginning.")
        else:
            last_tid = destination._adapter.txncontrol.get_tid(
                destination._load_cursor)
            next_tid = p64(last_tid+1)
            source = source.iterator(start=next_tid)
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

        if storage_has_data(destination):
            msg = "Error: the destination storage has data.  Try --clear."
            sys.exit(msg)

        destination.copyTransactionsFrom(source)
        source.close()
        destination.close()


if __name__ == '__main__':
    main()
