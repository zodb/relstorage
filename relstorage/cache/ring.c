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

static int ring_oversize(RSRing ring)
{
    return ring->frequency > ring->max_len;
}

static int ring_is_empty(RSRing ring)
{
    return ring == NULL || ring->r_next == ring || ring->r_next == NULL;
}

void
ring_add(RSRing ring, RSRingNode *elt)
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
ring_del(RSRing ring, RSRingNode *elt)
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
ring_move_to_head(RSRing ring, RSRingNode *elt)
{
    elt->r_prev->r_next = elt->r_next;
    elt->r_next->r_prev = elt->r_prev;
    elt->r_next = ring;
    elt->r_prev = ring->r_prev;
    ring->r_prev->r_next = elt;
    ring->r_prev = elt;
}

static int
ring_move_to_head_from_foreign(RSRing current_ring,
                               RSRing new_ring,
                               RSRingNode* elt)
{
    //ring_del(current_ring, elt);
    elt->r_next->r_prev = elt->r_prev;
    elt->r_prev->r_next = elt->r_next;
    current_ring->len -= 1;
    current_ring->frequency -= elt->len;

    //ring_add(new_ring, elt);
    elt->r_next = new_ring;
    elt->r_prev = new_ring->r_prev;
    new_ring->r_prev->r_next = elt;
    new_ring->r_prev = elt;
    elt->r_parent = new_ring->r_parent;

    new_ring->frequency += elt->len;
    new_ring->len++;

    //return ring_oversize(new_ring);
    return new_ring->frequency > new_ring->max_len;
}

static RSRingNode* ring_lru(RSRing ring)
{
    if(ring->r_next == ring) {
        // empty list
        return NULL;
    }
    return ring->r_next;
}

void lru_on_hit(RSRing ring, RSRingNode* entry)
{
    entry->frequency++;
    ring_move_to_head(ring, entry);
}

static int lru_will_fit(RSRingNode* ring, RSRingNode* entry)
{
    return ring->max_len >= (entry->len + ring->frequency);
}

/**
 * Call when `updated_ring` has gotten too big, and we should transfer
 * items to `destination_ring`. The item `ignore_me` is the item that
 * caused the `updated_ring` to get too big, so if it's the only thing
 * in the ring, we don't move it.
 *
 * When `allow_victims` is False, then we stop once we fill up all
 * three rings and we avoid producing any victims. If we *would*
 * have produced victims, we return with rejects.frequency = 1 so the
 * caller can know to stop feeding us.
 *
 * When `overfill_destination` is True, then we will move all the
 * items we have to in order to make updated_ring its correct size,
 * even though this could cause the destination to become too large.
 * There will never be victims in this case. `allow_victims` is
 * ignored in this case.
 *
 * You must only call this when the ring is already full.
 */
#define _SPILL_OVERFILL 1
#define _SPILL_FIT 0
#define _SPILL_VICTIMS 1
#define _SPILL_NO_VICTIMS 0
static
RSRingNode _spill_from_ring_to_ring(RSRing updated_ring,
                                    RSRing destination_ring,
                                    RSRingNode* ignore_me,
                                    int allow_victims,
                                    int overfill_destination)
{
    RSRingNode rejects = {};
    if(overfill_destination) {
        rejects.r_next = rejects.r_prev = NULL;
    }
    else {
        rejects.r_next = rejects.r_prev = &rejects;
    }

    while(updated_ring->len > 1 && ring_oversize(updated_ring)) {
        RSRingNode* eden_oldest = ring_lru(updated_ring);
        if(!eden_oldest || eden_oldest == ignore_me) {
            break;
        }

        if(overfill_destination || lru_will_fit(destination_ring, eden_oldest)){
            // Good, there's room. No victims to choose.
            ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
        }
        else {
            // Darn, we're too big. We must choose (and record) a
            // victim.

            if(!allow_victims) {
                // set the signal and quit.
                rejects.frequency = 1;
                break;
            }

            RSRingNode* probation_oldest = ring_lru(destination_ring);
            if(!probation_oldest) {
                //Hmm, the ring got emptied, but there's also no space
                //in protected. This must be a very large object. Take
                //ownership of it anyway, but quite trying.
               ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
               break;
            }

            if(probation_oldest->frequency > eden_oldest->frequency) {
                // Discard the eden entry, it's used less than the
                // probation entry.
                ring_move_to_head_from_foreign(updated_ring, &rejects, eden_oldest);
            }
            else {
                // good bye to the item on probation.
                ring_move_to_head_from_foreign(destination_ring, &rejects, probation_oldest);
                // hello to eden item, who is now on probation
                ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
            }
        }
    }

    /**
     * This can happen, but it's tricky to write a test for. It also
     * slows down the 'mixed' benchmark from 2.6 to 3.5s or so
     * (because it reduces hit rates?). It's probably not a big deal.
     */
    /*
    if (allow_victims && !overfill_destination && ring_oversize(destination_ring)) {
        // Trim the destination. It may have been oversize when we got
        // here.
        int oversize = 1;
        while (destination_ring->len > 1 && oversize) {
            RSRingNode* dest_oldest = destination_ring->r_next; //ring_lru(destination_ring);
            oversize = ring_move_to_head_from_foreign(destination_ring, &rejects, dest_oldest);
        }
    }
    */

    // we return rejects by value, so the next/prev pointers that were
    // initialized to its address here are now junk. break the link so
    // we don't run into them.
    if(rejects.r_prev) {
        rejects.r_prev->r_next = NULL;
    }
    rejects.r_prev = NULL;
    return rejects;
}


void lru_probation_on_hit(RSCache* cache,
                          RSRingNode* entry)
{
    entry->frequency++;
    RSRing protected_ring = cache->protected;
    RSRing probation_ring = cache->probation;
    int protected_oversize = ring_move_to_head_from_foreign(probation_ring, protected_ring, entry);
    if( !protected_oversize ) {
        return;
    }

    // Protected got too big. Demote entries back to probation until
    // protected is the right size (or we happen to hit the entry we
    // just added, or the ring only has one item left)
    _spill_from_ring_to_ring(protected_ring, probation_ring, entry,
                             _SPILL_NO_VICTIMS,
                             _SPILL_OVERFILL);

}

/**
 * When `allow_victims` is False, then we stop once we fill up all
 * three rings and we avoid producing any victims. If we *would*
 * have produced victims, we return with rejects.frequency = 1 so the
 * caller can know to stop feeding us.
 */
static
RSRingNode _eden_add(RSCache* cache,
                     RSRingNode* entry,
                     int allow_victims)
{
    RSRingNode* eden_ring = cache->eden;
    RSRingNode* protected_ring = cache->protected;
    RSRingNode* probation_ring = cache->probation;

    RSRingNode rejects = {};
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
            RSRingNode* eden_oldest = ring_lru(eden_ring);
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
    return _spill_from_ring_to_ring(eden_ring, probation_ring, entry, allow_victims, _SPILL_FIT);
}



RSRingNode eden_add(RSCache* cache,
                    RSRingNode* entry)
{
    return _eden_add(cache, entry, _SPILL_VICTIMS);
}


int eden_add_many(RSCache* cache,
                  RSRingNode* entry_array,
                  int entry_count)
{
    int i = 0;
    for (i = 0; i < entry_count; i++) {
        RSRingNode add_rejects = _eden_add(cache, entry_array + i, 0);
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



RSRingNode lru_update_mru(RSCache* cache,
                          RSRing home_ring,
                          RSRingNode* entry,
                          rs_counter_t old_entry_size,
                          rs_counter_t new_entry_size)
{
    // XXX: All this checking of ring equality isn't very elegant.
    // Should we have three functions? But then we'd have three places
    // to remember to resize the ring
    RSRing protected_ring = cache->protected;
    RSRing probation_ring = cache->probation;
    RSRing eden_ring = cache->eden;

    // Always update the frequency
    entry->frequency++;
    // always resize the ring because the entry size changed behind
    // our back
    assert(entry->len == new_entry_size);
    home_ring->frequency -= old_entry_size;
    home_ring->frequency += new_entry_size;


    if (home_ring == eden_ring) {
        // The simplest thing to do is to act like a delete and an
        // addition, since adding to eden always rebalances the rings
        // in addition to moving it to head.

        // This might be ever-so-slightly slower in the case where the size
        // went down or there was still room.
        ring_del(home_ring, entry);
        return eden_add(cache, entry);
    }

    int protected_ring_oversize = 0;
    if (home_ring == probation_ring) {
        protected_ring_oversize = ring_move_to_head_from_foreign(home_ring, protected_ring, entry);
    }
    else {
        assert(home_ring == protected_ring);
        ring_move_to_head(home_ring, entry);
        protected_ring_oversize = ring_oversize(home_ring);
    }

    if (protected_ring_oversize) {
        // bubble down, rejecting as needed
        return _spill_from_ring_to_ring(protected_ring, probation_ring, entry,
                                        _SPILL_VICTIMS, _SPILL_FIT);
    }

    RSRingNode result = {};
    result.r_next = result.r_prev = NULL;
    return result;
}

static void lru_age_list(RSRingNode* ring)
{
    if (ring_is_empty(ring)) {
        return;
    }

    RSRingNode* here = ring->r_next;
    while (here != ring) {
        here->frequency = here->frequency / 2;
        here = here->r_next;
    }
}

void lru_age_lists(RSCache* cache)
{
    lru_age_list(cache->eden);
    lru_age_list(cache->probation);
    lru_age_list(cache->protected);
}
