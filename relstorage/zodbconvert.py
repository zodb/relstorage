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

import optparse
from persistent.TimeStamp import TimeStamp
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
    parser.set_defaults(dry_run=False, clear=False)
    options, args = parser.parse_args(argv[1:])

    if len(args) != 1:
        parser.error("The name of one configuration file is required.")

    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, handler = ZConfig.loadConfig(schema, args[0])
    source = config.source.open()
    destination = config.destination.open()

    write("Storages opened successfully.\n")

    if options.dry_run:
        write("Dry run mode: not changing the destination.\n")
        if storage_has_data(destination):
            write("Warning: the destination storage has data\n")
        count = 0
        for txn in source.iterator():
            write('%s user=%s description=%s\n' % (
                TimeStamp(txn.tid), txn.user, txn.description))
            count += 1
        write("Would copy %d transactions.\n" % count)

    else:
        if options.clear:
            if hasattr(destination, 'zap_all'):
                destination.zap_all()
            else:
                msg = ("Error: no API is known for clearing this type "
                       "of storage. Use another method.")
                sys.exit(msg)

        if storage_has_data(destination):
            msg = "Error: the destination storage has data.  Try --clear."
            sys.exit(msg)

        destination.copyTransactionsFrom(source)

        source.close()
        destination.close()

        write('All transactions copied successfully.\n')


if __name__ == '__main__':
    main()
