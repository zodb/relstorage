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

See http://wiki.zope.org/ZODB/ZODBConvert for details.
"""

import logging
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
        i[0]
    except IndexError:
        return False
    return True


def main():
    logging.basicConfig()

    parser = optparse.OptionParser(description=__doc__,
        usage="%prog [options] config_file")
    parser.add_option(
        "--dry-run", dest="dry_run", action="store_true",
        help="Attempt to open the storages, then explain what would be done")
    parser.add_option(
        "--clear", dest="clear", action="store_true",
        help="Clear the contents of the destination storage before copying")
    parser.add_option(
        "-v", "--verbose", dest="verbose", action="store_true",
        help="Show verbose information while copying")
    parser.set_defaults(dry_run=False, clear=False, verbose=False)
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.error("The name of one configuration file is required.")

    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, handler = ZConfig.loadConfig(schema, args[0])
    source = config.source.open()
    destination = config.destination.open()

    print "Storages opened successfully."

    if options.dry_run:
        print "Dry run mode: not changing the destination."
        if storage_has_data(destination):
            print "Warning: the destination storage has data"
        count = 0
        for txn in source.iterator():
            print '%s user=%s description=%s' % (
                TimeStamp(txn.tid), txn.user, txn.description)
            for rec in txn:
                print '  oid=%s length=%d' % (oid_repr(rec.oid), len(rec.data))
            count += 1
        print "Would copy %d transactions." % count
        sys.exit(0)

    if options.clear:
        if hasattr(destination, 'zap_all'):
            destination.zap_all()
        else:
            msg = ("Error: no API is known for clearing this type of storage."
                " Use another method.")
            sys.exit(msg)

    if storage_has_data(destination):
        msg = "Error: the destination storage has data.  Try --clear."
        sys.exit(msg)

    destination.copyTransactionsFrom(source, verbose=options.verbose)

    source.close()
    destination.close()

    print 'All transactions copied successfully.'


if __name__ == '__main__':
    main()

