
"""OID tree traversal for the garbage collection phase of packing.

Optimized for memory efficiency.  Uses sets of native integers
rather than Python integers because native integers take up a lot less
room in RAM.
"""

from __future__ import absolute_import

import collections
import gc
import logging

import BTrees

from relstorage._compat import iteritems

IIunion32 = BTrees.family32.II.union # pylint:disable=no-member
IISet32 = BTrees.family32.II.Set
IISet64 = BTrees.family64.II.Set
log = logging.getLogger(__name__)


class IISet32X(object):
    """An IISet32 extended with a Python set layer for efficient inserts."""

    def __init__(self):
        self._base = IISet32()
        self._layer = set()

    def add(self, key):
        layer = self._layer
        if key not in layer and key not in self._base:
            layer.add(key)
            if len(layer) > 10000:
                self._apply()

    def _apply(self):
        """Add the layer to the base and reset the layer."""
        if self._layer:
            self._base = IIunion32(self._base, IISet32(self._layer))
            self._layer.clear()

    def __iter__(self):
        self._apply()
        return iter(self._base)

    def __contains__(self, key):
        return key in self._layer or key in self._base


class TreeMarker(object):
    """Finds all OIDs reachable from a set of root OIDs."""

    # This class groups OIDs by their upper 33 bits.  Why 33 instead
    # of 32?  Because IISet and IIBucket are signed, they can not accept
    # positive integers >= (1 << 31). The solution is to simply
    # add an extra grouping bit.
    hi = 0xffffffff80000000  # 33 high bits
    lo = 0x000000007fffffff  # 31 low bits

    def __init__(self):
        # self._refs:
        # {from_oid_hi: {to_oid_hi: IISet64([from_oid_lo << 32 | to_oid_lo])}}
        self._refs = collections.defaultdict(
            lambda: collections.defaultdict(IISet64))
        # self._reachable: {oid_hi: IISet32X}
        self._reachable = collections.defaultdict(IISet32X)
        self.reachable_count = 0

    def add_refs(self, pairs):
        """Add a list of (from_oid, to_oid) reference pairs.

        `from_oid` and `to_oid` must be 64 bit integers.
        """
        refs = self._refs
        hi = self.hi
        lo = self.lo
        for from_oid, to_oid in pairs:
            s = refs[from_oid & hi][to_oid & hi]
            s.add(((from_oid & lo) << 32) | (to_oid & lo))

    def mark(self, oids):
        """Mark specific OIDs and descendants of those OIDs as reachable."""
        hi = self.hi
        lo = self.lo
        pass_count = 1

        # this_pass: {oid_hi: IISet32X}
        this_pass = collections.defaultdict(IISet32X)
        for oid in sorted(oids):
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
        # pylint:disable=too-many-locals
        # next_pass: {oid_hi: IISet32X}
        next_pass = collections.defaultdict(IISet32X)
        found = 0
        refs = self._refs
        reachable = self._reachable
        lo = self.lo

        for oid_hi, oids_lo in iteritems(this_pass):
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
                for to_oid_hi, s in iteritems(refs[oid_hi]):
                    min_key = oid_lo << 32
                    max_key = min_key | 0xffffffff
                    keys = s.keys(min=min_key, max=max_key)
                    if not keys:
                        # No references found here.
                        continue
                    to_reachable_set = reachable[to_oid_hi]
                    next_pass_add = next_pass[to_oid_hi].add
                    for key in keys:
                        child_oid_lo = int(key & lo)
                        if child_oid_lo not in to_reachable_set:
                            next_pass_add(child_oid_lo)

        return found, next_pass

    def free_refs(self):
        """Free the collection of refs to save RAM."""
        self._refs = None
        gc.collect()

    @property
    def reachable(self):
        """Iterate over all the reachable OIDs."""
        for oid_hi, oids_lo in iteritems(self._reachable):
            for oid_lo in oids_lo:
                # Decode the OID.
                yield oid_hi | oid_lo
