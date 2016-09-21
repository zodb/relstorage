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

/**
 * The LRU ring heads use `len` to record the number of items,
 * and `frequency` to record the sum of the `len` of the members.
 * They also use the r_parent member to be the void* CDATA pointer;
 * this is copied to the children when they move.
 */

static int ring_oversize(CPersistentRing* ring)
{
    return ring->frequency > ring->max_len;
}

static int ring_is_empty(CPersistentRing* ring)
{
    return ring == NULL || ring->r_next == ring || ring->r_next == NULL;
}

void
ring_add(CPersistentRing *ring, CPersistentRing *elt)
{
    elt->r_next = ring;
    elt->r_prev = ring->r_prev;
    ring->r_prev->r_next = elt;
    ring->r_prev = elt;
    elt->r_parent = ring->r_parent;

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
    // Leave the parent so the node can be reused; it gets reset
    //anyway when it goes to a different list.
    //elt->r_parent = NULL;

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

static int
ring_move_to_head_from_foreign(CPersistentRing* current_ring,
                               CPersistentRing* new_ring,
                               CPersistentRing* elt)
{
	ring_del(current_ring, elt);
	ring_add(new_ring, elt);
    return ring_oversize(new_ring);
}

static CPersistentRing* ring_lru(CPersistentRing* ring)
{
    if(ring->r_next == ring) {
        // empty list
        return NULL;
    }
    return ring->r_next;
}

void lru_on_hit(CPersistentRing* ring, CPersistentRing* entry)
{
    entry->frequency++;
    ring_move_to_head(ring, entry);
}

void lru_probation_on_hit(CPersistentRing* probation_ring,
                         CPersistentRing* protected_ring,
                         CPersistentRing* entry)
{
    entry->frequency++;
    int protected_oversize = ring_move_to_head_from_foreign(probation_ring, protected_ring, entry);
	if( !protected_oversize ) {
		return;
	}

	// Protected got too big. Demote entries back to probation until
	// protected is the right size (or we happen to hit the entry we
	// just added, or the ring only has one item left)
	while( ring_oversize(protected_ring) && protected_ring->len > 1 ) {
        CPersistentRing* protected_lru = ring_lru(protected_ring);
		if( protected_lru == entry || protected_lru == NULL) {
            break;
		}
        ring_move_to_head_from_foreign(protected_ring, probation_ring, protected_lru);
	}


}

void lru_update_mru(CPersistentRing* ring,
                    CPersistentRing* entry,
                    rs_counter_t old_entry_size,
                    rs_counter_t new_entry_size)
{
    entry->frequency++;
    ring->frequency -= old_entry_size;
    ring->frequency += new_entry_size;
    ring_move_to_head(ring, entry);
    // XXX TODO: Rebalance all the rings!
}

static int lru_will_fit(CPersistentRing* ring, CPersistentRing* entry)
{
    return ring->max_len >= (entry->len + ring->frequency);
}

/**
 * When `allow_victims` is False, then we stop once we fill up all
 * three rings and we avoid producing any victims. If we *would*
 * have produced victims, we return with rejects.frequency = 1 so the
 * caller can know to stop feeding us.
 */
static
CPersistentRing _eden_add(CPersistentRing* eden_ring,
                          CPersistentRing* protected_ring,
                          CPersistentRing* probation_ring,
                          CPersistentRing* entry,
                          int allow_victims)
{
    CPersistentRing rejects = {};
    rejects.r_next = rejects.r_prev = NULL;

    ring_add(eden_ring, entry);
    if(!ring_oversize(eden_ring)) {
        return rejects;
    }

    // Ok, we have to move things. Begin by filling up the
    // protected space
    if(ring_is_empty(probation_ring) && !ring_oversize(protected_ring)) {
        /*
          # This is a modification of the algorithm. When we start out
          # go ahead and populate the protected_lru directly
          # from eden; only when its full do we start doing the probationary
          # dance. This helps mitigate any issues with choosing segment sizes;
          # we're going to occupy all the memory anyway, why not, it's reserved for us,
          # so go ahead and fill it.
        */
        while(ring_oversize(eden_ring)) {
            CPersistentRing* eden_oldest = ring_lru(eden_ring);
            if(!eden_oldest || eden_oldest == entry) {
                break;
            }
            if( !lru_will_fit(protected_ring, eden_oldest) ) {
                /*
                    # This would oversize protected. Move it to probation instead,
                    # which is currently empty, so there's no need to choose a victim.
                    # This may temporarily oversize us in the aggregate of the three.
                */
                ring_move_to_head_from_foreign(eden_ring, probation_ring, eden_oldest);
                // Signal whether we would need to cull something.
                rejects.frequency = ring_oversize(probation_ring);
                break;
            }
            else {
                ring_move_to_head_from_foreign(eden_ring, protected_ring, eden_oldest);
            }
        }
        return rejects;
    }

    // OK, we've already filled protected and have started putting
    // things in probation. So we may need to choose victims.
    // Initialize the list to hold them.
    rejects.r_next = rejects.r_prev = &rejects;
    while(ring_oversize(eden_ring)) {
        CPersistentRing* eden_oldest = ring_lru(eden_ring);
        if(!eden_oldest || eden_oldest == entry) {
            break;
        }

        if(lru_will_fit(probation_ring, eden_oldest)){
            // Good, there's room. No victims to choose.
            ring_move_to_head_from_foreign(eden_ring, probation_ring, eden_oldest);
        }
        else {
            // Darn, we're too big. We must choose (and record) a
            // victim.

            if(!allow_victims) {
                // set the signal and quit.
                rejects.frequency = 1;
                break;
            }

            CPersistentRing* probation_oldest = ring_lru(probation_ring);
            if(!probation_oldest) {
                //Hmm, the ring got emptied, but there's also no space
                //in protected. This must be a very large object. Take
                //ownership of it anyway, but quite trying.
               ring_move_to_head_from_foreign(eden_ring, probation_ring, eden_oldest);
               break;
            }

            if(probation_oldest->frequency > eden_oldest->frequency) {
                // Discard the eden entry, it's used less than the
                // probation entry.
                ring_move_to_head_from_foreign(eden_ring, &rejects, eden_oldest);
            }
            else {
                // good bye to the item on probation.
                ring_move_to_head_from_foreign(probation_ring, &rejects, probation_oldest);
                // hello to eden item, who is now on probation
                ring_move_to_head_from_foreign(eden_ring, probation_ring, eden_oldest);
            }
        }
    }

    // we return rejects by value, so the next/prev pointers that were
    // initialized to its address here are now junk. break the link so
    // we don't run into them.
    rejects.r_prev->r_next = NULL;

    return rejects;
}

CPersistentRing eden_add(CPersistentRing* eden_ring,
                         CPersistentRing* protected_ring,
                         CPersistentRing* probation_ring,
                         CPersistentRing* entry)
{
    return _eden_add(eden_ring, protected_ring, probation_ring, entry, 1);
}


int eden_add_many(CPersistentRing* eden_ring,
                  CPersistentRing* protected_ring,
                  CPersistentRing* probation_ring,
                  CPersistentRing* entry_array,
                  int entry_count)
{
    int i = 0;
    for (i = 0; i < entry_count; i++) {
        CPersistentRing add_rejects = _eden_add(eden_ring, protected_ring, probation_ring, entry_array + i, 0);
        if (add_rejects.frequency) {
             // We would have rejected something, so     we must be full.
             // XXX: This isn't strictly true. It could be one really
             // large item in the middle that we can't fit, but we
             // might be able to fit items after it.
             break;
        }
    }

    return i;
}




static void lru_age_list(CPersistentRing* ring)
{
    if (ring_is_empty(ring)) {
        return;
    }

    CPersistentRing* here = ring->r_next;
    while (here != ring) {
        here->frequency = here->frequency / 2;
        here = here->r_next;
    }
}

void lru_age_lists(CPersistentRing* ring1, CPersistentRing* ring2, CPersistentRing* ring3)
{
    lru_age_list(ring1);
    lru_age_list(ring2);
    lru_age_list(ring3);
}
