/*****************************************************************************

  Copyright (c) 2003 Zope Foundation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

#define RING_C "$Id$\n"

/* Support routines for the doubly-linked list of cached objects.

The cache stores a doubly-linked list of persistent objects, with
space for the pointers allocated in the objects themselves.  The cache
stores the distinguished head of the list, which is not a valid
persistent object.

The next pointers traverse the ring in order starting with the least
recently used object.  The prev pointers traverse the ring in order
starting with the most recently used object.

*/

#include <stddef.h>
#include <assert.h>
#include <stdint.h>
#ifndef __RING_H
#include "ring.h"
#endif

/* The LRU ring heads use `len` to record the number of items,
 * and `frequency` to record the sum of the `len` of the members.
 */

void
ring_add(CPersistentRing *ring, CPersistentRing *elt)
{
    assert(!elt->r_next);
    elt->r_next = ring;
    elt->r_prev = ring->r_prev;
    ring->r_prev->r_next = elt;
    ring->r_prev = elt;

    ring->frequency += elt->len;
    ring->len++;
}

void
ring_del(CPersistentRing* ring, CPersistentRing *elt)
{
	if( elt->r_next == NULL && elt->r_prev == NULL)
		return;

    elt->r_next->r_prev = elt->r_prev;
    elt->r_prev->r_next = elt->r_next;
    elt->r_next = NULL;
    elt->r_prev = NULL;

    ring->len -= 1;
    ring->frequency -= elt->len;
}

void
ring_move_to_head(CPersistentRing *ring, CPersistentRing *elt)
{
    elt->r_prev->r_next = elt->r_next;
    elt->r_next->r_prev = elt->r_prev;
    elt->r_next = ring;
    elt->r_prev = ring->r_prev;
    ring->r_prev->r_next = elt;
    ring->r_prev = elt;
}

void
ring_move_to_head_from_foreign(CPersistentRing* current_ring,
                               CPersistentRing* new_ring,
                               CPersistentRing* elt)
{
	ring_del(current_ring, elt);
	ring_add(new_ring, elt);
}


void lru_probation_on_hit(CPersistentRing* probation_ring,
                          CPersistentRing* protected_ring,
                          CPersistentRing* entry)
{
    entry->frequency++;

    ring_move_to_head_from_foreign(probation_ring, protected_ring, entry);
}

void lru_update_mru(CPersistentRing* ring,
                    CPersistentRing* entry,
                    uint_fast64_t old_entry_size,
                    uint_fast64_t new_entry_size)
{
    entry->frequency++;
    ring->frequency -= old_entry_size;
    ring->frequency += new_entry_size;
    ring_move_to_head(ring, entry);
}
