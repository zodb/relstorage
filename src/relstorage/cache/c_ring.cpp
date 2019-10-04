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
#include "c_ring.h"


void
rsc_ring_add(RSRing& ring, RSRingNode *elt)
{
    elt->r_next = &ring;
    elt->r_prev = ring.r_prev;
    ring.r_prev->r_next = elt;
    ring.r_prev = elt;
    elt->generation = ring.generation;

    ring.sum_weights += elt->weight();
    ring.len++;

}

void
rsc_ring_del(RSRing& ring, RSRingNode *elt)
{
    if( elt->r_next == NULL && elt->r_prev == NULL) {
        return;
    }

    elt->r_next->r_prev = elt->r_prev;
    elt->r_prev->r_next = elt->r_next;
    elt->r_next = NULL;
    elt->r_prev = NULL;
    // Leave the parent so the node can be reused; it gets reset
    //anyway when it goes to a different list.
    //elt->r_parent = NULL;

    ring.len -= 1;
    ring.sum_weights -= elt->weight();
}

void
rsc_ring_move_to_head(RSRing& ring, RSRingNode *elt)
{
    elt->r_prev->r_next = elt->r_next;
    elt->r_next->r_prev = elt->r_prev;
    elt->r_next = &ring;
    elt->r_prev = ring.r_prev;
    ring.r_prev->r_next = elt;
    ring.r_prev = elt;
}

bool
ring_move_to_head_from_foreign(RSRing& current_ring,
                               RSRing& new_ring,
                               RSRingNode* elt)
{
    //ring_del(current_ring, elt);
    elt->r_next->r_prev = elt->r_prev;
    elt->r_prev->r_next = elt->r_next;
    current_ring.len -= 1;
    current_ring.sum_weights -= elt->weight();

    //ring_add(new_ring, elt);
    elt->r_next = &new_ring;
    elt->r_prev = new_ring.r_prev;
    new_ring.r_prev->r_next = elt;
    new_ring.r_prev = elt;
    elt->generation = new_ring.generation;

    new_ring.sum_weights += elt->weight();
    new_ring.len++;

    //return ring_oversize(new_ring);
    return new_ring.sum_weights > new_ring.max_weight;
}

RSRingNode* ring_lru(RSRing& ring)
{
    if(ring.r_next == &ring) {
        // empty list
        return NULL;
    }
    return static_cast<RSRingNode*>(ring.r_next);
}

void rsc_on_hit(RSRing& ring, RSRingNode* entry)
{
    entry->frequency++;
    rsc_ring_move_to_head(ring, entry);
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

RSR_SINLINE
RSRing _spill_from_ring_to_ring(RSRing& updated_ring,
                                RSRing& destination_ring,
                                RSRingNode* ignore_me,
                                int allow_victims,
                                int overfill_destination)
{
    RSRingNode* eden_oldest = NULL;
    RSRingNode* probation_oldest = NULL;
    RSRing rejects;

    if(overfill_destination) {
        rejects.r_next = rejects.r_prev = NULL;
    }
    else {
        rejects.r_next = rejects.r_prev = &rejects;
    }

    while(updated_ring.sum_weights > 1 && updated_ring.ring_oversize()) {
        eden_oldest = ring_lru(updated_ring);
        if(!eden_oldest || eden_oldest == ignore_me) {
            break;
        }

        if(overfill_destination || destination_ring.lru_will_fit(eden_oldest)){
            // Good, there's room. No victims to choose.
            ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
        }
        else {
            // Darn, we're too big. We must choose (and record) a
            // victim.

            if(!allow_victims) {
                // set the signal and quit.
                rejects.max_weight = 1;
                break;
            }

            probation_oldest = ring_lru(destination_ring);
            if(!probation_oldest) {
                //Hmm, the ring got emptied, but there's also no space
                //in protected. This must be a very large object. Take
                //ownership of it anyway, but quite trying.
               ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
               break;
            }

            if (eden_oldest->frequency >= probation_oldest->frequency) {
                // good bye to the item on probation.
                ring_move_to_head_from_foreign(destination_ring, rejects, probation_oldest);
                // hello to eden item, who is now on probation
                ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
            }
            else {
                // Discard the eden entry, it's used less than the
                // probation entry.
                ring_move_to_head_from_foreign(updated_ring, rejects, eden_oldest);
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


void rsc_probation_on_hit(RSCache& cache,
                          RSRingNode* entry)
{
    RSRing* protected_ring = cache.ring_protected;
    RSRing* probation_ring = cache.ring_probation;
    int protected_oversize = ring_move_to_head_from_foreign(*probation_ring, *protected_ring, entry);

    entry->frequency++;

    if( !protected_oversize ) {
        return;
    }

    // Protected got too big. Demote entries back to probation until
    // protected is the right size (or we happen to hit the entry we
    // just added, or the ring only has one item left)
    _spill_from_ring_to_ring(*protected_ring, *probation_ring, entry,
                             _SPILL_NO_VICTIMS,
                             _SPILL_OVERFILL);

}

/**
 * When `allow_victims` is False, then we stop once we fill up all
 * three rings and we avoid producing any victims. If we *would*
 * have produced victims, we return with rejects.frequency = 1 so the
 * caller can know to stop feeding us.
 */
RSRing _eden_add(RSCache& cache,
                 RSRingNode* entry,
                 int allow_victims)
{
    RSRing* eden_ring = cache.ring_eden;
    RSRing* protected_ring = cache.ring_protected;
    RSRing* probation_ring = cache.ring_probation;
    RSRingNode* eden_oldest = NULL;

    RSRing rejects;

    rsc_ring_add(*eden_ring, entry);
    if(!eden_ring->ring_oversize()) {
        return rejects;
    }

    // Ok, we have to move things. Begin by filling up the
    // protected space
    if(probation_ring->ring_is_empty() && !protected_ring->ring_oversize()) {
        /*
          # This is a modification of the algorithm. When we start out
          # go ahead and populate the protected_lru directly
          # from eden; only when its full do we start doing the probationary
          # dance. This helps mitigate any issues with choosing segment sizes;
          # we're going to occupy all the memory anyway, why not, it's reserved for us,
          # so go ahead and fill it.
        */
        while(eden_ring->ring_oversize()) {
            eden_oldest = ring_lru(*eden_ring);
            if(!eden_oldest || eden_oldest == entry) {
                break;
            }
            if( !protected_ring->lru_will_fit(eden_oldest) ) {
                /*
                    # This would oversize protected. Move it to probation instead,
                    # which is currently empty, so there's no need to choose a victim.
                    # This may temporarily oversize us in the aggregate of the three.
                */
                ring_move_to_head_from_foreign(*eden_ring, *probation_ring, eden_oldest);
                // Signal whether we would need to cull something.
                rejects.max_weight = probation_ring->ring_oversize();
                break;
            }
            else {
                ring_move_to_head_from_foreign(*eden_ring, *protected_ring, eden_oldest);
            }
        }
        return rejects;
    }

    // OK, we've already filled protected and have started putting
    // things in probation. So we may need to choose victims.
    rejects = _spill_from_ring_to_ring(*eden_ring, *probation_ring, entry, allow_victims, _SPILL_FIT);
    if (!allow_victims && rejects.max_weight && !protected_ring->ring_oversize()) {
        // We were asked to spill, and we wanted to go to probation,
        // but we couldn't because that would fill it up. There's room
        // in protected, though, so go there instead.
        rejects = _spill_from_ring_to_ring(*eden_ring, *protected_ring, entry, allow_victims, _SPILL_FIT);
    }
    return rejects;
}



RSRing rsc_eden_add(RSCache& cache,
                    RSRingNode* entry)
{
    return _eden_add(cache, entry, _SPILL_VICTIMS);
}


RSRing rsc_update_mru(RSCache& cache,
                     RSRing& home_ring,
                     RSRingNode* entry,
                     rs_counter_t old_entry_size,
                     rs_counter_t new_entry_size)
{
    // XXX: All this checking of ring equality isn't very elegant.
    // Should we have three functions? But then we'd have three places
    // to remember to resize the ring
    RSRing* protected_ring = cache.ring_protected;
    RSRing* probation_ring = cache.ring_probation;
    RSRing* eden_ring = cache.ring_eden;
    int protected_ring_oversize = 0;
    RSRing result;

    // Always update the frequency
    entry->frequency++;
    // always resize the ring because the entry size changed behind
    // our back
    assert(entry->weight() == new_entry_size);
    home_ring.sum_weights -= old_entry_size;
    home_ring.sum_weights += new_entry_size;


    if (&home_ring == eden_ring) {
        // The simplest thing to do is to act like a delete and an
        // addition, since adding to eden always rebalances the rings
        // in addition to moving it to head.

        // This might be ever-so-slightly slower in the case where the size
        // went down or there was still room.
        rsc_ring_del(home_ring, entry);
        return rsc_eden_add(cache, entry);
    }

    if (&home_ring == probation_ring) {
        protected_ring_oversize = ring_move_to_head_from_foreign(home_ring, *protected_ring, entry);
    }
    else {
        assert(&home_ring == protected_ring);
        rsc_ring_move_to_head(home_ring, entry);
        protected_ring_oversize = home_ring.ring_oversize();
    }

    if (protected_ring_oversize) {
        // bubble down, rejecting as needed
        return _spill_from_ring_to_ring(*protected_ring, *probation_ring, entry,
                                        _SPILL_VICTIMS, _SPILL_FIT);
    }

    result.r_next = result.r_prev = NULL;
    return result;
}

static void lru_age_list(RSRing* ring)
{
    RSRingNode* here = nullptr;
    void* ring_as_node = static_cast<void*>(ring);
    if (ring->ring_is_empty()) {
        return;
    }

    here = static_cast<RSRingNode*>(ring->r_next);
    while (here != ring_as_node) {
        here->frequency = here->frequency / 2;
        here = static_cast<RSRingNode*>(here->r_next);
    }
}

void rsc_age_lists(RSCache& cache)
{
    lru_age_list(cache.ring_eden);
    lru_age_list(cache.ring_probation);
    lru_age_list(cache.ring_protected);
}
