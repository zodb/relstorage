
"""Test a big tree of random OID references.

Run like this: bin/py -m cProfile -- -s tottime relstorage/tests/bigmark.py
"""

import logging
from random import randint
from random import random

from relstorage._compat import xrange
from relstorage.treemark import TreeMarker

log = logging.getLogger('bigmark')


def bigmark():
    """Follow ~20M references between 10M objects."""

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s')

    oid_count = 10 * 1000 * 1000
    k = 10000

    log.info("Generating random references.")

    marker = TreeMarker()
    marker.add_refs([(0, i * k) for i in range(20)])
    refcount = 20
    for i in xrange(1, oid_count):
        if random() < 0.2:
            refs = []
            for _ in range(randint(0, 20)):
                refs.append((i * k, randint(0, oid_count) * k))
            marker.add_refs(refs)
            refcount += len(refs)

    log.info("Generated %d references.", refcount)

    log.info("Finding reachable objects.")

    pass_count = marker.mark([0])

    log.info(
        "Found %d reachable objects in %d passes.",
        marker.reachable_count, pass_count)


if __name__ == '__main__':
    bigmark()
