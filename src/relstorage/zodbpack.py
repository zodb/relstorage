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
"""
ZODB storage packing utility.
"""

import argparse
import logging
import sys
import time
from io import StringIO

import ZConfig
import ZODB.serialize

schema_xml = u"""
<schema>
  <import package="ZODB"/>
  <import package="relstorage"/>
  <multisection type="ZODB.storage" attribute="storages" />
</schema>
"""

logger = logging.getLogger("zodbpack")


def main(argv=None):
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "-d", "--days", dest="days", default=1,
        help="Days of history to keep (default: %(default)s)",
        type=float,
    )
    parser.add_argument(
        "--prepack", dest="prepack", default=False,
        action="store_true",
        help="Perform only the pre-pack preparation stage of a pack. "
        "(Only works with some storage types)",
    )
    parser.add_argument(
        "--use-prepack-state", dest="reuse_prepack", default=False,
        action="store_true",
        help="Skip the preparation stage and go straight to packing. "
        "Requires that a pre-pack has been run, or that packing was aborted "
        "before it was completed.",
    )
    parser.add_argument("config_file", type=argparse.FileType('r'))
    options = parser.parse_args(argv[1:])

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s"
    )

    schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
    config, _ = ZConfig.loadConfigFile(schema, options.config_file)

    t = time.time() - options.days * 86400.0
    for s in config.storages:
        name = '%s (%s)' % ((s.name or 'storage'), s.__class__.__name__)
        logger.info("Opening %s...", name)
        storage = s.open()
        logger.info("Packing %s.", name)
        if options.prepack or options.reuse_prepack:
            storage.pack(t, ZODB.serialize.referencesf,
                         prepack_only=options.prepack,
                         skip_prepack=options.reuse_prepack)
        else:
            # Be non-relstorage Storages friendly
            storage.pack(t, ZODB.serialize.referencesf)
        storage.close()
        logger.info("Packed %s.", name)

if __name__ == '__main__':
    main()
