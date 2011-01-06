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
from relstorage.storage import RelStorage
from StringIO import StringIO
import sys
import ZConfig
from ZODB.utils import oid_repr

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

log = logging.getLogger("relstorage.zodbconvert")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s")


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


def main(argv=sys.argv, write=sys.stdout.write):
    parser = optparse.OptionParser(description=__doc__,
        usage="%prog [options] config_file")
    parser.add_option(
        "--dry-run", dest="dry_run", action="store_true",
        help="Attempt to open the storages, then explain what would be done")
    parser.add_option(
        "--clear", dest="clear", action="store_true",
        help="Clear the contents of the destination storage before copying")
    parser.add_option(
        "--single-transaction", dest="single_transaction", action="store_true",
        help="Convert the source into the destination in a single transaction")
    parser.add_option(
        "--batch-size", dest="batch_size", type="int", action="store",
        help="Batch size to use when converting")
    parser.set_defaults(dry_run=False, clear=False, single_transaction=False, batch_size=250)
    options, args = parser.parse_args(argv[1:])

    if len(args) != 1:
        parser.error("The name of one configuration file is required.")


    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, handler = ZConfig.loadConfig(schema, args[0])
    source = config.source.open()
    destination = config.destination.open()
    destination._batcher_row_limit = options.batch_size

    #write("Storages opened successfully.\n")
    log.info("Storages opened successfully.")

    if options.dry_run:
        #write("Dry run mode: not changing the destination.\n")
        log.info("Dry run mode: not changing the destination.")
        if storage_has_data(destination):
            #write("Warning: the destination storage has data\n")
            log.warning("The destination storage has data.")
        count = 0
        for txn in source.iterator():
            write('%s user=%s description=%s\n' % (
                TimeStamp(txn.tid), txn.user, txn.description))
            count += 1
        #write("Would copy %d transactions.\n" % count)
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

        copy_args = [source]
        if issubclass(destination.__class__, RelStorage):
            copy_args.append(options.single_transaction)
        log.info("Started copying transactions...")
        log.info("This will take long...")
        num_txns, size, elapsed = destination.copyTransactionsFrom(*copy_args)
        log.info("Done copying transactions.")
        log.info("Closing up...")

        try:
            source.close()
        except IOError:
            #We don't mind if the source throws errors like:
            #ERROR:ZODB.FileStorage:Error saving index on close()
            #IOError: [Errno 13] Permission denied: '/path/to/db'
            pass
        destination.close()

        rate = (size/float(1024*1024)) / elapsed
        #write('All %d transactions copied successfully in %4.1f minutes at %1.3fmB/s.\n' %
        #      (num_txns, elapsed/60, rate))
        log.info('All %d transactions copied successfully in %4.1f minutes at %1.3fmB/s.',
                 num_txns, elapsed/60, rate)


if __name__ == '__main__':
    main()
