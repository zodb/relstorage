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

#ifndef __RING_H
#include "cache_ring.h"
#endif

/*
 * No version of MSVC properly supports inline. Sigh.
 */
#ifdef _MSC_VER
#define RSR_SINLINE static
#define RSR_INLINE
#else
#define RSR_SINLINE static inline
#define RSR_INLINE inline
#endif

RSR_SINLINE int ring_oversize(RSRing ring)
{
    return ring->u.head.sum_weights > ring->u.head.max_weight;
}

RSR_SINLINE int ring_is_empty(RSRing ring)
{
    return ring == NULL || ring->r_next == ring || ring->r_next == NULL;
}

RSR_SINLINE int cache_oversize(RSCache* cache)
{
    return ring_oversize(cache->eden) && ring_oversize(cache->probation) && ring_oversize(cache->protected);
}

RSR_SINLINE int lru_will_fit(RSRingNode* ring, RSRingNode* entry)
{
    return ring->u.head.max_weight >= (entry->u.entry.weight + ring->u.head.sum_weights);
}

RSR_SINLINE int cache_will_fit(RSCache* cache, RSRingNode* entry)
{
    return lru_will_fit(cache->eden, entry) || lru_will_fit(cache->probation, entry) || lru_will_fit(cache->protected, entry);
}

RSR_INLINE void
rsc_ring_add(RSRing ring, RSRingNode *elt)
{
    elt->r_next = ring;
    elt->r_prev = ring->r_prev;
    ring->r_prev->r_next = elt;
    ring->r_prev = elt;
    elt->u.entry.r_parent = ring->u.head.generation;

    ring->u.head.sum_weights += elt->u.entry.weight;
    ring->u.head.len++;

}

RSR_INLINE void
rsc_ring_del(RSRing ring, RSRingNode *elt)
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

    ring->u.head.len -= 1;
    ring->u.head.sum_weights -= elt->u.entry.weight;
}

RSR_INLINE void
rsc_ring_move_to_head(RSRing ring, RSRingNode *elt)
{
    elt->r_prev->r_next = elt->r_next;
    elt->r_next->r_prev = elt->r_prev;
    elt->r_next = ring;
    elt->r_prev = ring->r_prev;
    ring->r_prev->r_next = elt;
    ring->r_prev = elt;
}

RSR_SINLINE int
ring_move_to_head_from_foreign(RSRing current_ring,
                               RSRing new_ring,
                               RSRingNode* elt)
{
    //ring_del(current_ring, elt);
    elt->r_next->r_prev = elt->r_prev;
    elt->r_prev->r_next = elt->r_next;
    current_ring->u.head.len -= 1;
    current_ring->u.head.sum_weights -= elt->u.entry.weight;

    //ring_add(new_ring, elt);
    elt->r_next = new_ring;
    elt->r_prev = new_ring->r_prev;
    new_ring->r_prev->r_next = elt;
    new_ring->r_prev = elt;
    elt->u.entry.r_parent = new_ring->u.head.generation;

    new_ring->u.head.sum_weights += elt->u.entry.weight;
    new_ring->u.head.len++;

    //return ring_oversize(new_ring);
    return new_ring->u.head.sum_weights > new_ring->u.head.max_weight;
}

RSR_SINLINE RSRingNode* ring_lru(RSRing ring)
{
    if(ring->r_next == ring) {
        // empty list
        return NULL;
    }
    return ring->r_next;
}

void rsc_on_hit(RSRing ring, RSRingNode* entry)
{
    entry->u.entry.frequency++;
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
#define _SPILL_OVERFILL 1
#define _SPILL_FIT 0
#define _SPILL_VICTIMS 1
#define _SPILL_NO_VICTIMS 0
RSR_SINLINE
RSRingNode _spill_from_ring_to_ring(RSRing updated_ring,
                                    RSRing destination_ring,
                                    RSRingNode* ignore_me,
                                    int allow_victims,
                                    int overfill_destination)
{
    RSRingNode* eden_oldest = NULL;
    RSRingNode* probation_oldest = NULL;
    RSRingNode rejects = {0};

    if(overfill_destination) {
        rejects.r_next = rejects.r_prev = NULL;
    }
    else {
        rejects.r_next = rejects.r_prev = &rejects;
    }

    while(updated_ring->u.head.sum_weights > 1 && ring_oversize(updated_ring)) {
        eden_oldest = ring_lru(updated_ring);
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
                rejects.u.entry.frequency = 1;
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

            if (eden_oldest->u.entry.frequency >= probation_oldest->u.entry.frequency) {
                // good bye to the item on probation.
                ring_move_to_head_from_foreign(destination_ring, &rejects, probation_oldest);
                // hello to eden item, who is now on probation
                ring_move_to_head_from_foreign(updated_ring, destination_ring, eden_oldest);
            }
            else {
                // Discard the eden entry, it's used less than the
                // probation entry.
                ring_move_to_head_from_foreign(updated_ring, &rejects, eden_oldest);
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


void rsc_probation_on_hit(RSCache* cache,
                          RSRingNode* entry)
{
    RSRing protected_ring = cache->protected;
    RSRing probation_ring = cache->probation;
    int protected_oversize = ring_move_to_head_from_foreign(probation_ring, protected_ring, entry);

    entry->u.entry.frequency++;

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
RSR_SINLINE
RSRingNode _eden_add(RSCache* cache,
                     RSRingNode* entry,
                     int allow_victims)
{
    RSRingNode* eden_ring = cache->eden;
    RSRingNode* protected_ring = cache->protected;
    RSRingNode* probation_ring = cache->probation;
    RSRingNode* eden_oldest = NULL;

    RSRingNode rejects = {0};
    rejects.r_next = rejects.r_prev = NULL;

    rsc_ring_add(eden_ring, entry);
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
            eden_oldest = ring_lru(eden_ring);
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
                rejects.u.entry.frequency = ring_oversize(probation_ring);
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
    rejects = _spill_from_ring_to_ring(eden_ring, probation_ring, entry, allow_victims, _SPILL_FIT);
    if (!allow_victims && rejects.u.entry.frequency && !ring_oversize(protected_ring)) {
        // We were asked to spill, and we wanted to go to probation,
        // but we couldn't because that would fill it up. There's room
        // in protected, though, so go there instead.
        rejects = _spill_from_ring_to_ring(eden_ring, protected_ring, entry, allow_victims, _SPILL_FIT);
    }
    return rejects;
}



RSRingNode rsc_eden_add(RSCache* cache,
                        RSRingNode* entry)
{
    return _eden_add(cache, entry, _SPILL_VICTIMS);
}


int rsc_eden_add_many(RSCache* cache,
                      RSRingNode* entry_array,
                      int entry_count)
{
    int added_count = 0;
    int i = 0;
    RSRingNode add_rejects = {0};
    RSRingNode* incoming = NULL;
    RSRingNode* rejected = NULL;

    if (cache_oversize(cache) || !entry_count || !cache_will_fit(cache, entry_array)) {
        return 0;
    }

    for (i = 0; i < entry_count; i++) {
        // Don't try if we know we won't find a place for it.
        incoming = entry_array + i;
        if (!cache_will_fit(cache, incoming)) {
            incoming->u.entry.r_parent = -1;
            continue;
        }

        // _eden_add *always* adds, but it may or may not be able to
        // rebalance.
        added_count += 1;
        add_rejects = _eden_add(cache, incoming, _SPILL_NO_VICTIMS);
        if (add_rejects.u.entry.frequency) {
            // We would have rejected something, so we must be full.
            // Well, this isn't strictly true. It could be one really
            // large item in the middle that we can't fit, but we
            // might be able to fit items after it.
            // However, we *thought* we could fit this one in the
            // cache, but we couldn't. So we really are full. THis
            // one's already in here, so quit.
            break;
        }
    }

    // Anything left is because we broke out of the loop. They're
    // rejects and need to be marked as such.
    for (i += 1; i < entry_count; i++) {
        rejected = entry_array + i;
        rejected->u.entry.r_parent = -1;
    }

    return added_count;
}



RSRingNode rsc_update_mru(RSCache* cache,
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
    int protected_ring_oversize = 0;
    RSRingNode result = {0};

    // Always update the frequency
    entry->u.entry.frequency++;
    // always resize the ring because the entry size changed behind
    // our back
    assert(entry->u.entry.weight == new_entry_size);
    home_ring->u.head.sum_weights -= old_entry_size;
    home_ring->u.head.sum_weights += new_entry_size;


    if (home_ring == eden_ring) {
        // The simplest thing to do is to act like a delete and an
        // addition, since adding to eden always rebalances the rings
        // in addition to moving it to head.

        // This might be ever-so-slightly slower in the case where the size
        // went down or there was still room.
        rsc_ring_del(home_ring, entry);
        return rsc_eden_add(cache, entry);
    }

    if (home_ring == probation_ring) {
        protected_ring_oversize = ring_move_to_head_from_foreign(home_ring, protected_ring, entry);
    }
    else {
        assert(home_ring == protected_ring);
        rsc_ring_move_to_head(home_ring, entry);
        protected_ring_oversize = ring_oversize(home_ring);
    }

    if (protected_ring_oversize) {
        // bubble down, rejecting as needed
        return _spill_from_ring_to_ring(protected_ring, probation_ring, entry,
                                        _SPILL_VICTIMS, _SPILL_FIT);
    }

    result.r_next = result.r_prev = NULL;
    return result;
}

RSR_SINLINE void lru_age_list(RSRingNode* ring)
{
    RSRingNode* here = NULL;
    if (ring_is_empty(ring)) {
        return;
    }

    here = ring->r_next;
    while (here != ring) {
        here->u.entry.frequency = here->u.entry.frequency / 2;
        here = here->r_next;
    }
}

void rsc_age_lists(RSCache* cache)
{
    lru_age_list(cache->eden);
    lru_age_list(cache->probation);
    lru_age_list(cache->protected);
}
