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

typedef unsigned long long rs_counter_t; // For old CFFI versions.

/**
 * All entries are of this type.
 * On a 64-bit platform, this is 56 bytes in size.
 */
typedef struct RSRingNode_struct {
    struct RSRingNode_struct* r_prev;
    struct RSRingNode_struct* r_next;
    void* user_data;

    union entry_or_head_t {
        struct entry_t {
            // How popular this item is.
            rs_counter_t frequency;
            // The weight of this item.
            rs_counter_t weight;
            // The number of the parent (containing) generation
            int r_parent;
        } entry;
        struct head_t {
            // How many items are in this list
            rs_counter_t len;
            // The sum of their weights.
            rs_counter_t sum_weights;
            // The maximum allowed weight
            rs_counter_t max_weight;
            // The generation number. We don't make any use of this
            // number other than ensuring it is copied to r_parent
            // as needed. You can use any value except for negative
            // values in your RSCache generations.
            int generation;
        } head;
    } u;

} RSRingNode;

/**
 * A typedef for the distinguished head of the list.
 * Use the 'head' union member for this item.
 */
typedef RSRingNode* RSRing;

/**
 * The cache itself is three generations. See individual methods
 * or the Python code for information about how items move between rings.
 */
typedef struct RSCache_struct {
    RSRing eden;
    RSRing protected;
    RSRing probation;
} RSCache;

/*********
 * Generic ring operations.
 **********/

/**
 * Add elt as the most recently used object.  elt must not already be
 * in the list, although this isn't checked.
 *
 * Constant time.
 */
void rsc_ring_add(RSRing ring, RSRingNode *elt);

/**
 * Remove elt from the list.  elt must already be in the list, although
 * this isn't checked.
 *
 * Constant time.
 */
void rsc_ring_del(RSRing ring, RSRingNode *elt);

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
void rsc_ring_move_to_head(RSRing ring, RSRingNode *elt);

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
RSRingNode rsc_eden_add(RSCache* cache,
                        RSRingNode* entry);

/**
 * Add as many entries as possible from the array
 *
 * This will traverse the full array. Entries that don't fit will have
 * their r_parent set to -1.
 *
 * Returns the number of entries actually added.
 */
int rsc_eden_add_many(RSCache* cache,
                      RSRingNode* entry_array,
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
void rsc_probation_on_hit(RSCache* cache,
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
RSRingNode rsc_update_mru(RSCache* cache,
                          RSRing ring,
                          RSRingNode* entry,
                          rs_counter_t old_entry_size,
                          rs_counter_t new_entry_size);

/**
 * Record that the entry has been used.
 * This updates its popularity counter  and makes it the
 * most recently used item in its ring. This is constant time.
 */
void rsc_on_hit(RSRing ring, RSRingNode* entry);

/**
 * Decrease the popularity of all the items in the cache by half.
 *
 * Do this periodically to allow newer entries to compete with older
 * entries that were popular but no longer are.
 */
void rsc_age_lists(RSCache* cache);
