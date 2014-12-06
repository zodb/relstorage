
"""OID tree traversal for the garbage collection phase of packing.

Optimized for memory efficiency.  Uses sets and arrays of 31 bit integers
rather than Python integers because native integers take up a lot less
room in RAM.

This could probably be dramatically faster in C or Cython.  In particular,
the bisect_left and bisect_right functions are implemented in C, but they
require many unnecessary integer conversions.  Replacing those two
functions would probably make a significant difference.  Also,
collections.defaultdict and IISet have unknown performance characteristics.
Patches welcome. :-)
"""

from bisect import bisect_left
from bisect import bisect_right
from itertools import islice
import array
import BTrees
import collections
import gc
import logging


IISet32 = BTrees.family32.II.Set
log = logging.getLogger(__name__)


def get_typecode32():
    for c in ('i', 'l', 'h'):
        if array.array(c).itemsize == 4:
            return c
    raise ValueError("No array typecode provides an array of 32 bit integers")


class TreeMarker(object):
    """Find all OIDs reachable from a set of root OIDs.
    """

    # This class groups OIDs by their upper 33 bits.  Why 33 instead
    # of 32?  Because IISet is signed, it won't accept positive integers with
    # the high bit set, so we simply add an extra grouping bit.
    hi = 0xffffffff80000000  # 33 high bits
    lo = 0x000000007fffffff  # 31 low bits

    def __init__(self):
        code32 = get_typecode32()
        # self._refs:
        # {from_oid_hi: {to_oid_hi: (from_oids_lo, to_oids_lo)}}
        self._refs = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: (array.array(code32), array.array(code32))))
        # self._reachable: {oid_hi: IISet32}
        self._reachable = collections.defaultdict(IISet32)
        self.reachable_count = 0

    def add_refs(self, pairs):
        """Add a list of (from_oid, to_oid) reference pairs.

        `from_oid` and `to_oid` must be 64 bit integers.
        The pairs *must be sorted already* by `from_oid`!
        If they aren't, mark() will produce incorrect results.
        """
        refs = self._refs
        hi = self.hi
        lo = self.lo
        for from_oid, to_oid in pairs:
            from_oids_lo, to_oids_lo = refs[from_oid & hi][to_oid & hi]
            from_oids_lo.append(from_oid & lo)
            to_oids_lo.append(to_oid & lo)

    def mark(self, oids):
        """Mark specific OIDs and descendants of those OIDs as reachable."""
        hi = self.hi
        lo = self.lo
        pass_count = 1

        this_pass = collections.defaultdict(IISet32)  # {oid_hi: IISet32}
        for oid in oids:
            this_pass[oid & hi].add(int(oid & lo))

        while this_pass:
            gc.collect()
            found, next_pass = self._mark_pass(this_pass)
            log.debug(
                "Found %d more referenced object(s) in pass %d",
                found, pass_count)
            if not found:
                break
            self.reachable_count += found
            pass_count += 1
            this_pass = next_pass

        return pass_count

    def _mark_pass(self, this_pass):
        """Mark OIDs as reachable. Produce an OID set for the next pass.

        Return (found, next_pass), where `found` is the number of
        new OIDs marked and `next_pass` is the collection of OIDs to
        follow in the next pass.
        """
        next_pass = collections.defaultdict(IISet32)  # {oid_hi: IISet32}
        found = 0
        refs = self._refs
        reachable = self._reachable

        for oid_hi, oids_lo in this_pass.iteritems():
            from_reachable_set = reachable[oid_hi]

            for oid_lo in oids_lo:
                if oid_lo in from_reachable_set:
                    # This OID is already known to be reachable.
                    continue

                found += 1
                from_reachable_set.add(oid_lo)

                if oid_hi not in refs:
                    # This OID doesn't reference anything.
                    continue

                # Add the children of this OID to next_pass.
                for item in refs[oid_hi].iteritems():
                    to_oid_hi, (from_oids_lo, to_oids_lo) = item
                    left = bisect_left(from_oids_lo, oid_lo)
                    right = bisect_right(from_oids_lo, oid_lo)
                    if left >= right:
                        # No references found here.
                        continue
                    to_reachable_set = reachable[to_oid_hi]
                    child_oids_lo = islice(to_oids_lo, left, right)
                    next_pass[to_oid_hi].update(
                        x for x in child_oids_lo if x not in to_reachable_set)

        return found, next_pass

    def free_refs(self):
        """Free the collection of refs to save RAM."""
        self._refs = None
        gc.collect()

    @property
    def reachable(self):
        """Iterate over all the reachable OIDs."""
        for oid_hi, oids_lo in self._reachable.iteritems():
            for oid_lo in oids_lo:
                # Decode the OID.
                yield oid_hi | oid_lo
