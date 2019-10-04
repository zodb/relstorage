/*****************************************************************************

  Copyright (c) 2019 Zope Foundation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/
#ifndef CACHE_RING_H
#define CACHE_RING_H

/**
 * Support routines for the doubly-linked lists of cached objects
 * and their grouping into three generations.
 *
 * Each ring stores a headed, doubly-linked, circular list of nodes;
 * the nodes don't store any data other than statistics, the Python
 * wrapper objects do. The ring stores the distinguished head of the
 * list. The other list members are linked in LRU (least-recently
 * used) order.
 *
 * The r_next pointers traverse the ring starting with the least
 * recently used object. The r_prev pointers traverse the ring
 * starting with the most recently used object.
*/
#include <exception>
#include <stdexcept>
#include <stddef.h>
typedef size_t rs_counter_t; // For old CFFI versions.

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


class RSRingEntry {
    public:
    RSRingEntry* r_prev;
    RSRingEntry* r_next;

    RSRingEntry() : r_prev(nullptr), r_next(nullptr) {

    }

    virtual ~RSRingEntry() {
        // remove from the ring.
        this->r_prev = nullptr;
        this->r_next = nullptr;
    }

};

typedef enum {
  GEN_UNKNOWN = -1,
  GEN_EDEN = 1,
  GEN_PROTECTED = 2,
  GEN_PROBATION = 3
} generation_num;

/**
 * All entries are of this type.
 * On a 64-bit platform, this is 56 bytes in size.
 */
class RSRingNode : public RSRingEntry {
    public:
    // How popular this item is.
    rs_counter_t frequency;
    generation_num generation;


    RSRingNode() : RSRingEntry(), frequency(1), generation(GEN_UNKNOWN) {

    }

    virtual size_t weight() {
        throw std::runtime_error("Must provide weight");
    }

};

class RSRing : public RSRingEntry {
    public:
    // How many items are in this list
    rs_counter_t len;
    // The sum of their weights.
    size_t sum_weights;
    // The maximum allowed weight
    size_t max_weight;
    // The generation number. We don't make any use of this
    // number other than ensuring it is copied to r_parent
    // as needed. You can use any value except for negative
    // values in your RSCache generations.
    generation_num generation;

    RSRing() : RSRingEntry(), len(0), sum_weights(0), max_weight(0), generation(GEN_UNKNOWN) {
        this->r_next = this->r_prev = nullptr;
    }

    RSRing(size_t limit, generation_num generation) : RSRingEntry(), len(0), sum_weights(0), max_weight(limit), generation(generation) {
        this->r_next = this->r_prev = this;
    }

    RSR_INLINE bool ring_oversize() {
        return this->sum_weights > this->max_weight;
    }

    RSR_INLINE bool ring_is_empty() {
        return this->r_next == this || this->r_next == nullptr;
    }

    RSR_INLINE int lru_will_fit(RSRingNode* entry)
    {
        return this->max_weight >= (entry->weight() + this->sum_weights);
    }

};



/**
 * The cache itself is three generations. See individual methods
 * or the Python code for information about how items move between rings.
 */
class RSCache {
    public:
    RSRing* ring_eden;
    RSRing* ring_protected;
    RSRing* ring_probation;

    RSCache()
        : ring_eden(nullptr),
          ring_protected(nullptr),
          ring_probation(nullptr)
    {

    }

    virtual ~RSCache() {}


    RSR_INLINE bool cache_oversize()
    {
        return this->ring_eden->ring_oversize() && this->ring_protected->ring_oversize() && this->ring_probation->ring_oversize();
    }


    RSR_INLINE int cache_will_fit(RSRingNode* entry)
    {
        return this->ring_eden->lru_will_fit(entry)
            || this->ring_probation->lru_will_fit(entry)
            || this->ring_protected->lru_will_fit(entry);
    }

};

/*********
 * Generic ring operations.
 **********/

/**
 * Add elt as the most recently used object.  elt must not already be
 * in the list, although this isn't checked.
 *
 * Constant time.
 */
void rsc_ring_add(RSRing& ring, RSRingNode *elt);

/**
 * Remove elt from the list.  elt must already be in the list, although
 * this isn't checked.
 *
 * Constant time.
 */
void rsc_ring_del(RSRing& ring, RSRingNode *elt);

/**
 * elt must already be in the list, although this isn't checked.  It's
 * unlinked from its current position, and relinked into the list as the
 * most recently used object (which is arguably the tail of the list
 * instead of the head -- but the name of this function could be argued
 * either way).  This is equivalent to
 *
 *     ring_del(elt);
 *     ring_add(ring, elt);
 *
 * but may be a little quicker.
 *
 * Constant time.
 */
void rsc_ring_move_to_head(RSRing& ring, RSRingNode *elt);

/**********
 * Operations on the Eden ring.
 **********/

/**
 * Add the node to the eden ring, and flow entries through
 * the rest of the cache generations as needed, possibly rejecting
 * some if the cache is too full.
 *
 * This operation takes linear time in the number of entries that
 * have to be moved.
 *
 * The return value is a RSRingNode containing all the rejected
 * entries:
 *
 * - r_prev is always NULL;
 * - If r_next is NULL, there were no rejected entries;
 * - The final entry in the chain has an r_next of NULL;
 */
RSRing rsc_eden_add(RSCache& cache,
                    RSRingNode* entry);

/**
 * Add as many entries as possible from the array
 *
 * This will traverse the full array. Entries that don't fit will have
 * their r_parent set to -1.
 *
 * Returns the number of entries actually added.
 */
template <typename T>
int rsc_eden_add_many(RSCache& cache,
                      T* entry_array,
                      int entry_count);

/**********
 * Operations on the probation ring.
 **********/

/**
 * Move the `entry` from the probation ring of the `cache`
 * to become the MRU of the protected ring of the cache.
 *
 * If the protected ring was full, demote as many of its least
 * recently used objects to the probation ring as needed. This may
 * cause the probation ring to become over full (but the total size of
 * the cache remains unchanged), a condition that won't be corrected
 * until new entries are added to the eden ring that flow down, or
 * until an entry is resized with `lru_update_mru`.
 *
 * This operation takes linear time in the number of entries that must
 * be moved.
 */
void rsc_probation_on_hit(RSCache& cache,
                          RSRingNode* entry);
/**********
 * General caching operations.
 **********/

/**
 * Change the weight of the `entry` (and thus its containing `ring`),
 * while also acting as a "hit" on the entry (increasing its frequency
 * and causing it to become the MRU element in the appropriate ring).
 *
 * The `len` of the `entry` should already be `new_entry_size`.
 *
 * If this action causes any ring to become over full,
 * entries are shifted to correct this:
 *
 * - If the entry was in the eden ring, then older eden entries will move to
 *   probation or protected rings, as needed;
 * - If the entry was in the protected ring, entries may shift to the
 *   probation ring, and if the probation ring is over full, the least
 *   frequently used of the entries will be rejected.
 * - If the entry was in the probation ring, it is now in the
 *   protected ring, which may have caused the protection ring to
 *   become over full, so proceed as above.
 *
 * The return value is a RSRingNode containing all the rejected
 * entries:
 *
 * - r_prev is always NULL;
 * - If r_next is NULL, there were no rejected entries;
 * - The final entry in the chain has an r_next of NULL;
 */
RSRing rsc_update_mru(RSCache& cache,
                      RSRing& ring,
                      RSRingNode* entry,
                      rs_counter_t old_entry_size,
                      rs_counter_t new_entry_size);

/**
 * Record that the entry has been used.
 * This updates its popularity counter  and makes it the
 * most recently used item in its ring. This is constant time.
 */
void rsc_on_hit(RSRing& ring, RSRingNode* entry);

/**
 * Decrease the popularity of all the items in the cache by half.
 *
 * Do this periodically to allow newer entries to compete with older
 * entries that were popular but no longer are.
 */
void rsc_age_lists(RSCache& cache);

#define _SPILL_OVERFILL 1
#define _SPILL_FIT 0
#define _SPILL_VICTIMS 1
#define _SPILL_NO_VICTIMS 0

RSRing _eden_add(RSCache& cache, RSRingNode* entry, int allow_victims);

template <typename T>
int rsc_eden_add_many(RSCache& cache,
                      T* entry_array,
                      int entry_count)
{
    int added_count = 0;
    int i = 0;
    RSRing add_rejects;
    T* incoming = NULL;
    T* rejected = NULL;

    if (cache.cache_oversize() || !entry_count || !cache.cache_will_fit(entry_array)) {
        return 0;
    }

    for (i = 0; i < entry_count; i++) {
        // Don't try if we know we won't find a place for it.
        incoming = entry_array + i;
        if (!cache.cache_will_fit(incoming)) {
            incoming->generation = GEN_UNKNOWN;
            continue;
        }

        // _eden_add *always* adds, but it may or may not be able to
        // rebalance.
        added_count += 1;
        add_rejects = _eden_add(cache, incoming, _SPILL_NO_VICTIMS);
        if (add_rejects.max_weight) {
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
        rejected->generation = GEN_UNKNOWN;
    }

    return added_count;
}


#endif
