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

#include <memory>
#include <vector>
#include "c_cache.h"

using namespace relstorage::cache;


//********************
// Functions

// std::ostream& operator<<(std::ostream& s, const ICacheEntry& entry)
// {
//     s << "(Key: " << entry.key << " Generation: " << entry.generation() << ")";
//     return s;
// }

// std::ostream& operator<<(std::ostream& s, const Generation& gen)
// {
//     s << "(Generation: " << gen.generation << "; weight=" << gen.sum_weights() << "; len="
//         << gen.size() << "; max_weight=" << gen.max_weight()
//       << ")";
//     return s;
// }


/**
 * Call when `updated_ring` has gotten too big, and we should transfer
 * items to `destination_ring`. The item `ignore_me` is the item that
 * caused the `updated_ring` to get too big, so if it's the only thing
 * in the ring, we don't move it.
 *
 * We keep going until the *updated_ring* is the correct size.
 *
 * Items may be removed from both *updated_ring* and *destination_ring*.
 * If so, they are removed from the index and deleted if not in use
 *
 *
 * Returns true if items were (or would have been) deleted from the index.
 */

RSR_SINLINE
size_t _spill_from_ring_to_ring(Generation& updated_ring,
                                Generation& destination_ring,
                                const ICacheEntry* updated_ignore_me=nullptr,
                                bool allow_rejects=true)
{
    ICacheEntry* updated_oldest = nullptr;
    ICacheEntry* destination_oldest = nullptr;
    size_t deleted_from_index = 0;

    while (updated_ring.size() > 1 && updated_ring.oversize()) {
        updated_oldest = updated_ring.lru();
        if (!updated_oldest || updated_oldest == updated_ignore_me) {
            break;
        }

        if (destination_ring.will_fit(*updated_oldest)) {
            // Good, there's room. No victims to choose.
            destination_ring.adopt(*updated_oldest);
        }
        // Darn, we're too big. We must choose a
        // victim. If we can't destroy it, at least move it to the next ring.
        else if (!allow_rejects) {
            destination_ring.adopt(*updated_oldest);
            deleted_from_index += 1;
            break;
        }
        else {
            destination_oldest = destination_ring.lru();
            ICacheEntry* removed;
            if (!destination_oldest) {
                // Hmm, the ring got emptied, but there's also no space
                // in it for the new one. Discard it.
                removed = updated_oldest;
                updated_ring.remove(*updated_oldest);
            }
            else if (updated_oldest->frequency >= destination_oldest->frequency) {
                // good bye to the item on probation.
                removed = destination_oldest;
                destination_ring.remove(*destination_oldest);
                // hello to eden item, who is now on probation
                destination_ring.adopt(*updated_oldest);
            }
            else {
                // Discard the eden entry, it's used less than the
                // probation entry.
                removed = updated_oldest;
                updated_ring.remove(*updated_oldest);
            }

            deleted_from_index += 1;
            removed->remove_from_index();
            if (removed->can_delete())
                delete removed;
            else
                assert(removed->in_python());
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
    return deleted_from_index;
}

//********************
// ICacheEntry

void ICacheEntry::_remove_from_generation()
{
    this->_generation->remove(*this);
}

void ICacheEntry::_remove_from_generation_and_index()
{
    this->_remove_from_generation();
    EntryMapHook::unlink();
}


void ICacheEntry::_replace_with(ICacheEntry* new_entry)
{
    this->_generation->_sum_weights -= this->weight();
    this->_generation->_sum_weights += new_entry->weight();
    new_entry->_generation = this->_generation;
    this->_generation = nullptr;
    EntryListHook::swap_nodes(*new_entry);
    EntryMapHook::swap_nodes(*new_entry);
}


//********************
// SingleValueEntry
void* SVCacheEntry::operator new(size_t count)
{
    UNUSED(count);
    assert(count == sizeof(SVCacheEntry));
    return Cache::allocator.allocate(1);
}

void SVCacheEntry::operator delete(void* ptr)
{
    Cache::allocator.deallocate(static_cast<SVCacheEntry*>(ptr), 1);
}

ICacheEntry* SVCacheEntry::adding_value(const ProposedCacheEntry& proposed)
{
    const bool tid_equal = proposed.tid() == this->tid();
    if (unlikely(tid_equal)) {
        // If Pickle_t is PyObject*, this is pretty cheap. But if it's
        // std::string, this is an extra copy. This situation should rarely arise.
        Pickle_t other_pickle = this->owning_state(proposed.borrow_pickle());
        const bool state_equal = this->state_eq(other_pickle);
        this->decref(other_pickle);

        if (state_equal && tid_equal) {
            return this;
        }
        throw std::logic_error("Detected two different values for the same TID.");
    }

    MVCacheEntry* mve = new MVCacheEntry(*this, proposed);
    this->_replace_with(mve);
    return mve;
}

ICacheEntry* SVCacheEntry::freeze_to_tid(const TID_t tid)
{
    if (this->_tid > tid) {
        return this;
    }
    if (this->_tid == tid) {
        // We are discarding ourself now, but preserving this item's
        // location in the generations.
        this->_frozen = true;
        return this;
    }
    // We're older, we should be discarded.
    this->_remove_from_generation_and_index();
    return nullptr;
}

ICacheEntry* SVCacheEntry::discarding_tids_before(const TID_t tid)
{
    if (tid <= this->_tid) {
        this->_remove_from_generation_and_index();
        return nullptr;
    }
    return this;
}


//********************
// Generation

bool Generation::operator==(const Generation& other) const
{
    return this == &other;
}

void Generation::on_hit(ICacheEntry& entry)
{
    entry.frequency++;
    this->move_to_head(entry);
}


//********************
// Probation

void Probation::on_hit(ICacheEntry& entry)
{
    entry.frequency++;
    this->_protected.adopt(entry); // guaranteed not to spill

    if( !this->_protected.oversize() ) {
        return;
    }

    // Protected got too big. Demote entries back to probation until
    // protected is the right size (or we happen to hit the entry we
    // just added, or the ring only has one item left)
    _spill_from_ring_to_ring(this->_protected,
                             *this, &entry);

}

//********************
// Protected

void Protected::on_hit(ICacheEntry& entry)
{
    Generation::on_hit(entry);
    if (this->oversize()) {
        // Demote to probation, rejecting as needed.
        _spill_from_ring_to_ring(*this, this->_probation, &entry);
    }
}

//********************
// Eden
bool Eden::_balance_rings(ICacheEntry* added_or_changed, bool allow_rejects)
{
    if(!this->oversize()) {
        return false;
    }

    // Ok, we have to move things. Begin by filling up the
    // protected space
    if (this->cache.ring_probation.empty()
        && !this->cache.ring_protected.oversize()) {
        /*
         * This is a modification of the algorithm. When we start out
         * go ahead and populate the protected_lru directly
         * from eden; only when its full do we start doing the probationary
         * dance. This helps mitigate any issues with choosing segment sizes;
         * we're going to occupy all the memory anyway, why not, it's reserved for us,
         * so go ahead and fill it.
         */
        while (this->oversize()) {
            // This cannot be NULL if we're oversize.
            ICacheEntry* eden_oldest = this->lru();
            if(eden_oldest == added_or_changed)
                break;

            if (!this->cache.ring_protected.will_fit(*eden_oldest)) {
                /*
                 * This would oversize protected. Move it to probation instead,
                 * which is currently empty, so there's no need to choose a victim.
                 * This may temporarily oversize us in the aggregate of the three.
                 */
                this->cache.ring_probation.adopt(*eden_oldest);
                return true;
            }
            else
                this->cache.ring_protected.adopt(*eden_oldest);
        }
        return false;
    }

    // OK, we've already filled protected and have started putting
    // things in probation. So we may need to choose victims.
    // Begin by taking eden and moving to probation, evicting from probation
    // if needed.
    return _spill_from_ring_to_ring(*this,
                                    this->cache.ring_probation,
                                    added_or_changed,
                                    allow_rejects);
}

void Eden::on_hit(ICacheEntry& entry)
{
    Generation::on_hit(entry);
    _balance_rings(&entry);
}


bool Eden::add(ICacheEntry& entry, bool allow_rejects)
{
    Generation::add(entry);
    if (allow_rejects)
        return _balance_rings(&entry, allow_rejects);
    return false;
}


//********************
// MVCacheEntry
PythonAllocator<MVCacheEntry> MVCacheEntry::allocator;
PythonAllocator<MVCacheEntry::Entry> MVCacheEntry::Entry::allocator;

void* MVCacheEntry::Entry::operator new(size_t count)
{
    UNUSED(count);
    assert(count == sizeof(MVCacheEntry::Entry));
    return allocator.allocate(1);
}

void MVCacheEntry::Entry::operator delete(void* ptr)
{
    allocator.deallocate(static_cast<MVCacheEntry::Entry*>(ptr), 1);
}


void* MVCacheEntry::operator new(size_t count)
{
    UNUSED(count);
    assert(count == sizeof(MVCacheEntry));
    return allocator.allocate(1);
}

void MVCacheEntry::operator delete(void* ptr)
{
    allocator.deallocate(static_cast<MVCacheEntry*>(ptr), 1);
}

void MVCacheEntry::remove_tids_lte(TID_t tid)
{
    // bounded_range(lower, upper, left_closed, right_closed)
    // upper_bound(k) : first element greater than k.
    // lower_bound(k) : first element not less than k: which is first element >= k.
    // closed: Differs depending on whether its left or right. For right,
    // if it's closed it's upper_bound, otherwise its lower_bound.
    // thus, right_closed=True means the range includes (goes past) the upper bound;
    // right_closed=False means the range stops at k.
    // erase(b, e) removes things from b, stopping at e (it never removes e).
    // Thus to remove everything less than tid, we need to return a second iterator
    // greater than tid,
    std::pair<iterator, iterator> range = this->p_values.bounded_range(0, tid,
                                                                       false, true);
    this->p_values.erase_and_dispose(range.first, range.second, Disposer());
}

void MVCacheEntry::remove_tids_lt(TID_t tid)
{
    std::pair<iterator, iterator> range = this->p_values.bounded_range(0, tid,
                                                                       false, false);
    this->p_values.erase_and_dispose(range.first, range.second, Disposer());
}

size_t MVCacheEntry::weight() const
{
    size_t overhead = ICacheEntry::weight();
    size_t result = 0;
    for (const_iterator it = this->p_values.begin();
         it != this->p_values.end();
         it++ ) {
        result += it->weight();
    }
    return result + overhead;
}

ICacheEntry* MVCacheEntry::adding_value(const ProposedCacheEntry& proposed)
{
    this->insert(proposed);
    return this;
}

ICacheEntry* MVCacheEntry::freeze_to_tid(const TID_t tid)
{
    this->remove_tids_lt(tid);
    if (this->empty()) {
        // We should be discarded.
        this->_remove_from_generation_and_index();
        return nullptr;
    }
    if (this->degenerate()) {
        // One item left, either it matches or it doesn't.
        SVCacheEntry* new_entry = this->to_single();
        if (!new_entry->freeze_to_tid(tid)) {
            delete new_entry;
            this->_remove_from_generation_and_index();
            return nullptr;
        }
        this->_replace_with(new_entry);
        return new_entry;
    }

    // Multiple items left, all of which are at least == tid
    // but could be greater.
    for(iterator it = this->p_values.begin();
        it != this->p_values.end(); ++it) {
        Entry& entry = *it;
        if (entry.tid == tid) {
            entry.frozen = true;
        }
    }
    return this;
}

ICacheEntry* MVCacheEntry::discarding_tids_before(const TID_t tid)
{
    this->remove_tids_lte(tid);
    if (this->empty()) {
        // We should be discarded.
        this->_remove_from_generation_and_index();
        return nullptr;
    }
    if (this->degenerate()) {
        // One item left, must be greater.
        SVCacheEntry* only_entry = this->to_single();
        this->_replace_with(only_entry);
        return only_entry;
    }
    return this;
}

//********************
// Cache
PythonAllocator<SVCacheEntry> Cache::allocator;
PythonAllocator<ICacheEntry> Cache::deallocator;


void Cache::add_to_eden(const ProposedCacheEntry& proposed)
{
    if (unlikely(this->data.count(proposed.oid()))) {
        throw std::runtime_error("Key already present");
    }

    SVCacheEntry* entry = new SVCacheEntry(proposed);
    this->data.insert(*entry);
    this->ring_eden.add(*entry);
}

OidList Cache::add_many(TempCacheFiller& temp_filler)
{
    OidList added;

    if (this->oversize() || temp_filler.entries.empty()) {
        return added;
    }

    // We used to allocate a single large array for all the
    // SVCacheEntry objects and insert individual offsets (array + i)
    // to them. Back in the CFFI days, this was much faster than
    // allocating individual CFFI-wrapped objects. But in C++, it
    // turns out to be both faster (26ms vs 42ms for 90,000 entries)
    // and more memory efficient (between 5 and 14MB vs 15-20MB) to
    // allocate individually using new(). This saves the extra
    // constructor time for the empty entries, and it saves the
    // memory-management complexity of carving individual items out of
    // a bulk array. This is especially true because we don't
    // implement a node freelist to be able to re-use these individual
    // items that get carved out of the array when they get evicted or
    // replaced with a newer version.


    // BIT::insert is "Logarithmic in general, but it is amortized constant time
    // (two comparisons in the worst case) if t is inserted
    // immediately before hint."
    // So we need the array sorted in *descending* order for boost::set so that
    // each successive element is less than the previous element. However, that messes
    // with having the values come in in LRU - to - MRU order. The gains are modest:
    // that 26ms becomes 35ms.
    /*
    std::sort(temp_filler.entries.begin(), temp_filler.entries.end(),
              _ProposedCacheEntryCompare);
    */

    // The numbers above are for if we quit early, as soon as we're full.

    // We put all the data into eden. We then manually rebalance the rings to get the
    // best rejections when we have all the frequency information.
    for (TempCacheFiller::iterator it = temp_filler.begin(), end = temp_filler.end();
         it != end;
         ++it ) {
        // Don't try if we know we won't find a place for it.
        SVCacheEntry* incoming = new SVCacheEntry(*it);
        if (!this->will_fit(*incoming)) {
            delete incoming;
            continue;
        }

        this->data.insert(*incoming); // This fails if it's already present.
        this->ring_eden.add(*incoming, false);
        if (this->ring_eden.sum_weights() > this->max_weight()) {
            break;
        }
    }

    assert(this->ring_probation.empty());
    assert(this->ring_protected.empty());
    // First, spill everything from eden down to protected, without discarding anything.
    // Then, if protected is too full, spill from it to probation, discarding those entries
    // that won't fit.
    // Because we don't allow evictions going from eden to protected, this may take a few rounds.
    int would_evict = 1;

    while(this->oversize() && would_evict) {
        would_evict = _spill_from_ring_to_ring(this->ring_eden, this->ring_protected, nullptr, false);
        would_evict += _spill_from_ring_to_ring(this->ring_protected, this->ring_probation);
    }

    // Now, only what's left is added.
    for (TempCacheFiller::iterator it = temp_filler.begin(), end = temp_filler.end();
         it != end;
         ++it ) {
        if (this->data.count(it->oid()) != 0)
            added.push_back(it->oid());
    }
    return added;
}

#define if_existing(K,V) OidEntryMap::iterator it(this->data.find(K)); do { \
    if (it == this->data.end()) \
        return V; \
    } while (0); \
    ICacheEntry& existing_entry = *it;

void Cache::store_and_make_MRU(const ProposedCacheEntry& proposed)
{
    if_existing(proposed.oid(), );

    ICacheEntry* new_entry = existing_entry.adding_value(proposed);

    assert(new_entry);
    assert(new_entry->generation());
    new_entry->generation()->on_hit(*new_entry);
}

/**
 * Remove an existing key.
 */
void Cache::delitem(OID_t key)
{
    if_existing(key,)

    assert(existing_entry.generation());
    existing_entry.generation()->remove(existing_entry);
    assert(existing_entry.generation() == nullptr);
    this->data.erase(it);

    if (existing_entry.can_delete()) {
        delete &existing_entry;
    }
}

static void maybe_delete_existing_entry(ICacheEntry* new_entry,
                                        ICacheEntry& existing_entry)
{
    if (new_entry && new_entry != &existing_entry && existing_entry.can_delete()) {
        delete &existing_entry;
    }

}

void Cache::delitem(OID_t key, TID_t tid)
{
    if_existing(key,);

    ICacheEntry* new_entry = existing_entry.discarding_tids_before(tid);
    maybe_delete_existing_entry(new_entry, existing_entry);
}

void Cache::freeze(OID_t key, TID_t tid)
{
    if_existing(key,);

    ICacheEntry* new_entry = existing_entry.freeze_to_tid(tid);
    maybe_delete_existing_entry(new_entry, existing_entry);
}

bool Cache::contains(const OID_t key) const
{
    return this->data.count(key) == 1;
}

ICacheEntry* Cache::get(const OID_t key)
{
    if_existing(key, nullptr);
    return &existing_entry;
}

SVCacheEntry* Cache::_get_or_peek(const OID_t key, const TID_t tid, const bool peek)
{
    if_existing(key, nullptr);
    SVCacheEntry* matching = existing_entry.matching_tid(tid);
    if (matching && !peek)
        existing_entry.generation()->on_hit(existing_entry);
    return matching;
}

SVCacheEntry* Cache::peek(const OID_t key, const TID_t tid)
{
    return _get_or_peek(key, tid, true);
}

SVCacheEntry* Cache::get(const OID_t key, const TID_t tid)
{
    return _get_or_peek(key, tid, false);
}

void Cache::age_frequencies()
{
    OidEntryMap::iterator end = this->data.end();
    for (OidEntryMap::iterator it = this->data.begin(); it != end; it++) {
        it->frequency = it->frequency / 2;
        // TODO: Shouldn't we remove them if they hit 0?
    }
}

size_t Cache::weight() const
{
    return this->ring_eden.sum_weights()
        + this->ring_protected.sum_weights()
        + this->ring_probation.sum_weights()
        + sizeof(Cache);
}


// Local Variables:
// flycheck-clang-include-path: ("../../../include" "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/include/python2.7")
// End:
