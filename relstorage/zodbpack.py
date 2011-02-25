#!/usr/bin/env python
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
"""ZODB storage packing utility.
"""

from StringIO import StringIO
import logging
import optparse
import sys
import time
import ZConfig
import ZODB.serialize

schema_xml = """
<schema>
  <import package="ZODB"/>
  <import package="relstorage"/>
  <multisection type="ZODB.storage" attribute="storages" />
</schema>
"""

log = logging.getLogger("zodbpack")


def main(argv=sys.argv):
    parser = optparse.OptionParser(description=__doc__,
        usage="%prog [options] config_file")
    parser.add_option(
        "-d", "--days", dest="days", default="0",
        help="Days of history to keep (default 0)",
    )
    parser.add_option(
        "--prepack", dest="prepack", default=False,
        action="store_true",
        help="Perform only the pre-pack preparation stage of a pack. "
        "(Only works with some storage types)",
    )
    parser.add_option(
        "--use-prepack-state", dest="reuse_prepack", default=False,
        action="store_true",
        help="Skip the preparation stage and go straight to packing. "
        "Requires that a pre-pack has been run, or that packing was aborted "
        "before it was completed.",
    )
    options, args = parser.parse_args(argv[1:])

    if len(args) != 1:
        parser.error("The name of one configuration file is required.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, handler = ZConfig.loadConfig(schema, args[0])

    t = time.time() - float(options.days) * 86400.0
    for s in config.storages:
        name = '%s (%s)' % ((s.name or 'storage'), s.__class__.__name__)
        log.info("Opening %s...", name)
        storage = s.open()
        log.info("Packing %s.", name)
        if options.prepack or options.reuse_prepack:
            storage.pack(t, ZODB.serialize.referencesf,
                prepack_only=options.prepack,
                skip_prepack=options.reuse_prepack)
        else:
            # Be non-relstorage Storages friendly
            storage.pack(t, ZODB.serialize.referencesf)
        storage.close()
        log.info("Packed %s.", name)

if __name__ == '__main__':
    main()
