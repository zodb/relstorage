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
#include <iostream>
#include "c_cache.h"


using namespace relstorage::cache;

//********************
// Functions

std::ostream& operator<<(std::ostream& s, const ICacheEntry& entry)
{
    s << "(Key: " << entry.key << " Generation: " << entry.generation() << ")";
    return s;
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
void _spill_from_ring_to_ring(Generation& updated_ring,
                              Generation& destination_ring,
                              const ICacheEntry* updated_ignore_me,
                              const bool allow_victims,
                              const bool overfill_destination,
                              OidList& rejects)
{
    ICacheEntry* updated_oldest = NULL;
    ICacheEntry* destination_oldest = NULL;
    rejects.clear();


    while(!updated_ring.empty() && updated_ring.oversize()) {
        updated_oldest = updated_ring.lru();
        if(!updated_oldest || updated_oldest == updated_ignore_me) {
            break;
        }

        if(overfill_destination || destination_ring.will_fit(*updated_oldest)){
            // Good, there's room. No victims to choose.
            destination_ring.adopt(*updated_oldest);
        }
        else {
            // Darn, we're too big. We must choose (and record) a
            // victim.

            if(!allow_victims) {
                break;
            }

            destination_oldest = destination_ring.lru();
            if(!destination_oldest) {
                //Hmm, the ring got emptied, but there's also no space
                //in protected. This must be a very large object. Take
                //ownership of it anyway, but quit trying.
                destination_ring.adopt(*updated_oldest);
               break;
            }

            if (updated_oldest->frequency >= destination_oldest->frequency) {
                // good bye to the item on probation.
                rejects.push_back(destination_oldest->key);
                destination_ring.remove(*destination_oldest);
                // hello to eden item, who is now on probation
                destination_ring.adopt(*updated_oldest);
            }
            else {
                // Discard the eden entry, it's used less than the
                // probation entry.
                rejects.push_back(updated_oldest->key);
                updated_ring.remove(*updated_oldest);
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
}

//********************
// ICacheEntry

// bool ICacheEntry::operator==(const ICacheEntry& other) const
// {
//     return this == &other;
// }


//********************
// SingleValueEntry
ICacheEntry* SVCacheEntry::with_later(const Pickle_t& state,
                                      const TID_t new_tid)
{
    const bool state_equal = this->state_eq(state);
    const bool tid_equal = new_tid == this->tid();
    if (state_equal && tid_equal) {
        return this;
    }

    if (!state_equal && tid_equal) {
        throw std::logic_error("Detected two different values for the same TID.");
    }

    SVCacheEntry* new_entry = new SVCacheEntry(this->key,
                                               state,
                                               new_tid);
    MVCacheEntry* mve = new MVCacheEntry(this->key);
    mve->frequency = this->frequency;
    mve->push_back(*new_entry);
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
    return nullptr;
}

ICacheEntry* SVCacheEntry::discarding_tids_before(const TID_t tid)
{
    if (tid <= this->_tid) {
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

void Generation::on_hit(Cache& cache, ICacheEntry& entry)
{
    UNUSED(cache);
    entry.frequency++;
    this->move_to_head(entry);
}

void Generation::add(ICacheEntry& elt)
{
    assert(elt.generation() == nullptr);
    this->_entries.push_front(elt);
    elt.generation() = this;
    this->_sum_weights += elt.weight();
}

void Generation::adopt(ICacheEntry& elt)
{
    elt.generation()->remove(elt);
    assert(elt.generation() == nullptr);
    this->add(elt);
}

void Generation::remove(ICacheEntry& elt)
{
    assert(elt.generation() == this);
    elt.unlink_from_list();
    elt.generation() = nullptr;
    this->_sum_weights -= elt.weight();
}



//********************
// Probation

void Probation::on_hit(Cache& cache, ICacheEntry& entry)
{
    Protected& protected_ring = cache.ring_protected;
    Probation& probation_ring = cache.ring_probation;
    entry.frequency++;
    protected_ring.adopt(entry); // guaranteed not to spill

    if( !protected_ring.oversize() ) {
        return;
    }

    // Protected got too big. Demote entries back to probation until
    // protected is the right size (or we happen to hit the entry we
    // just added, or the ring only has one item left)
    _spill_from_ring_to_ring(protected_ring, probation_ring, &entry,
                             false, // No victims
                             true,
                             this->no_rejects); // let destination get too big

}

//********************
// Eden
OidList& Eden::add_and_evict(ICacheEntry& entry)
{
    Eden& eden_ring = *this;
    Cache& cache = eden_ring.cache;
    Protected& protected_ring = cache.ring_protected;
    Probation& probation_ring = cache.ring_probation;
    this->rejects.clear();
    Generation::add(entry);

    if(!eden_ring.oversize()) {
        return this->rejects;
    }

    // Ok, we have to move things. Begin by filling up the
    // protected space
    if(probation_ring.empty() && !protected_ring.oversize()) {
        /*
          # This is a modification of the algorithm. When we start out
          # go ahead and populate the protected_lru directly
          # from eden; only when its full do we start doing the probationary
          # dance. This helps mitigate any issues with choosing segment sizes;
          # we're going to occupy all the memory anyway, why not, it's reserved for us,
          # so go ahead and fill it.
        */
        while(eden_ring.oversize()) {
            // This cannot be NULL if we're oversize.
            ICacheEntry& eden_oldest = *eden_ring.lru();
            if(eden_oldest.key == entry.key) {
                break;
            }
            if( !protected_ring.will_fit(eden_oldest) ) {
                /*
                    # This would oversize protected. Move it to probation instead,
                    # which is currently empty, so there's no need to choose a victim.
                    # This may temporarily oversize us in the aggregate of the three.
                */
                probation_ring.adopt(eden_oldest);
                break;
            }
            else {
                protected_ring.adopt(eden_oldest);
            }
        }
        return this->rejects;
    }

    // OK, we've already filled protected and have started putting
    // things in probation. So we may need to choose victims.
    // Begin by taking eden and moving to probation, evicting from probation
    // if needed.
    _spill_from_ring_to_ring(
                             eden_ring, probation_ring, &entry,
                             true, // all   ow_victims
                             false,  // don't overfill
                             this->rejects);

    if (protected_ring.oversize()) {
        // If protected is oversize, also move from them to probation.
        // What about from eden to protected?
        _spill_from_ring_to_ring(
               eden_ring, protected_ring, &entry,
               true,
               false,
               this->rejects
        );
    }
    return this->rejects;
}


//********************
// MVCacheEntry

struct _LTE {
    TID_t tid;
    _LTE(TID_t t) : tid(t) {}
    bool operator()(const SVCacheEntry& p) {
        return p.tid() <= this->tid;
    }
};

struct _LT {
    TID_t tid;
    _LT(TID_t t) : tid(t) {}
    bool operator()(const SVCacheEntry& p) {
        return p.tid() < this->tid;
    }
};

void MVCacheEntry::remove_tids_lte(TID_t tid) {
    this->p_values.remove_if(_LTE(tid));
}

void MVCacheEntry::remove_tids_lt(TID_t tid) {
    this->p_values.remove_if(_LT(tid));
}

size_t MVCacheEntry::weight() const
{
    size_t overhead = ICacheEntry::weight();
    size_t result = 0;
    for (SVEntryList::const_iterator it = this->p_values.begin();
         it != this->p_values.end();
         it++ ) {
        result += it->weight();
    }
    return result + overhead;
}

ICacheEntry* MVCacheEntry::with_later(const Pickle_t& state,
                                      const TID_t new_tid)
{
    this->push_back(
        *new SVCacheEntry(this->key, state, new_tid)
    );
    return this;
}

ICacheEntry* MVCacheEntry::freeze_to_tid(const TID_t tid)
{
    this->remove_tids_lt(tid);
    if (this->empty()) {
        // We should be discarded.
        return nullptr;
    }
    if (this->degenerate()) {
        // One item left, either it matches or it doesn't.
        ICacheEntry& front = this->front();
        // TODO: Lifetimes.
        front.unlink_from_list();
        return front.freeze_to_tid(tid);
    }

    // Multiple items left, all of which are at least == tid
    // but could be greater.
    for(SVEntryList::iterator it = this->p_values.begin();
        it != this->p_values.end(); ++it) {
        SVCacheEntry& entry = *it;
        if (entry.tid() == tid) {
            entry.frozen() = true;
        }
    }
    return this;
}

ICacheEntry* MVCacheEntry::discarding_tids_before(const TID_t tid)
{
    this->remove_tids_lte(tid);
    if (this->empty()) {
        // We should be discarded.
        return nullptr;
    }
    if (this->degenerate()) {
        // One item left, must be greater.
        return &this->front();
    }
    return this;
}

//********************
// Cache

void Cache::_handle_evicted(OidList& evicted) {

    OidList::const_iterator end = evicted.end();
    for(OidList::const_iterator it = evicted.begin(); it != end; it++) {
        // things were evicted, must be removed from our data, which
        // is the only place holding a reference to them, aside from transient
        // SingleValue Python nodes.
        this->data.erase(*it);
    }
    evicted.clear();

}


void Cache::add_to_eden(OID_t key, const Pickle_t& state, TID_t tid)
{
    if (this->data.count(key)) {
        throw std::runtime_error("Key already present");
    }

    // keep with the shared ownership.
    // TODO: Avoid copy here. We want to move.
    ICacheEntry* sve_p = new SVCacheEntry(key, state, tid);
    this->data.insert(*sve_p);
    this->_handle_evicted(
       this->ring_eden.add_and_evict(*sve_p)
    );
}


int Cache::add_many(SVCacheEntry* entry_array, // this type will change
                    int entry_count)
{
    int added_count = 0;
    if (this->oversize() || !entry_count) {
        return 0;
    }

    for (int i = 0; i < entry_count; i++) {
        // Don't try if we know we won't find a place for it.
        SVCacheEntry* incoming = (entry_array + i);
        if (!this->will_fit(*incoming)) {
            incoming->generation(nullptr);
            continue;
        }

        // "Logarithmic in general, but it is amortized constant time
        // (two comparisons in the worst case) if t is inserted
        // immediately before hint."
        // So we need the array sorted in *descending* order for boost::set
        this->data.insert(*incoming); // This fails if it's already present.

        // _eden_add *always* adds, but it may or may not be able to
        // rebalance.
        added_count += 1;
        const OidList add_rejects = this->ring_eden.add_and_evict(*incoming);
        if (!add_rejects.empty()) {
            // We started rejecting stuff, so we must be full.
            // Well, this isn't strictly true. It could be one really
            // large item in the middle that we can't fit, but we
            // might be able to fit items after it.
            // However, we *thought* we could fit this one in the
            // cache, but we couldn't. So we really are full.
            // Put everything that we rejected back in probation.
            for(OidList::const_iterator it = add_rejects.begin(); it != add_rejects.end(); it++) {
                const OID_t oid = *it;
                ICacheEntry& cached = *this->data.find(oid); // fails if oid not present
                this->ring_probation.add(cached);
            }
            break;
        }
    }

    return added_count;
}

/**
 * Does not rebalance rings. Use only when no evictions are necessary.
 */
void Cache::replace_entry(ICacheEntry& new_entry, ICacheEntry& prev_entry,
                          size_t prev_weight )
{
    assert(!new_entry.in_cache());
    assert(prev_entry.in_cache());

    Generation& generation = *prev_entry.generation();
    // Must replace in our generation ring...
    generation.replace_entry(new_entry, prev_weight, prev_entry);
    // ... and our map
    if (&new_entry != &prev_entry) {
        this->data.replace_node(this->data.iterator_to(prev_entry), new_entry);
    }
}

void Cache::update_mru(ICacheEntry& entry)
{
    // XXX: All this checking of ring equality isn't very elegant.
    // Should we have three functions? But then we'd have three places
    // to remember to resize the ring
    Protected& protected_ring = this->ring_protected;
    Probation& probation_ring = this->ring_probation;
    Eden& eden_ring = this->ring_eden;
    Generation& home_ring = *entry.generation();
    this->rejects.clear();

    // Always update the frequency
    entry.frequency++;

    if (home_ring == eden_ring) {
        // The simplest thing to do is to act like a delete and an
        // addition, since adding to eden always rebalances the rings
        // in addition to moving it to head.

        // This might be ever-so-slightly slower in the case where the size
        // went down or there was still room.
        home_ring.remove(entry);
        eden_ring.add_and_evict(entry);
        return;
    }

    if (home_ring == probation_ring) {
        protected_ring.adopt(entry);
    }
    else {
        assert(home_ring == protected_ring);
        home_ring.move_to_head(entry);
    }

    if (protected_ring.oversize()) {
        // bubble down, rejecting as needed
        _spill_from_ring_to_ring(protected_ring, probation_ring, &entry,
                                 true, /*victims*/ false /*don't oversize*/,
                                 this->rejects);
    }
}


void Cache::store_and_make_MRU(OID_t oid,
                               const Pickle_t& state,
                               const TID_t new_tid)
{
    // better be there
    OidEntryMap::iterator it(this->data.find(oid));
    ICacheEntry& existing_entry = *it;

    size_t old_weight = existing_entry.weight();

    // TODO: Memory management
    // TODO: Wouldn't it be better to use a multimap and keep the equal keys?
    // that way we don't have to mess with any of this.
    // of course the downside is we're back to tracking LRU status by OID/TID pairs
    // instead of just OID.
    ICacheEntry* new_entry = existing_entry.with_later(state, new_tid);

    assert(new_entry);
    if (new_entry != &existing_entry) {
        // We want to grow. To do that we need to move the existing entry
        // to the new one and put the new one in our cache.
        assert(existing_entry.in_cache());
        assert(!new_entry->in_cache());

        MVCacheEntry* mve = dynamic_cast<MVCacheEntry*>(new_entry);
        existing_entry.generation()->remove(existing_entry);
        mve->push_back(dynamic_cast<SVCacheEntry&>(existing_entry));

        this->ring_protected.add(*new_entry);
        this->data.replace_node(it, *new_entry);
        // TODO: def ejecting here.
    }
    else {
        existing_entry.generation()->notice_weight_change(existing_entry, old_weight);
    }
    if (new_entry->weight() > old_weight) {
        this->update_mru(*new_entry);
        this->_handle_evicted(this->rejects);
    }
    else {
        assert(new_entry->weight() == old_weight);
    }

}

#define if_existing(K,V) OidEntryMap::iterator it(this->data.find(K)); do { \
    if (it == this->data.end()) \
        return V; \
    } while (0); \
    ICacheEntry& existing_entry = *it;


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
    // TODO: Memory management
}

RSR_SINLINE
void _update(Cache* cache,
             ICacheEntry& existing_entry,
             size_t prev_weight,
             ICacheEntry* new_entry)
{
    if (!new_entry) {
        cache->delitem(existing_entry.key);
    }
    else if (new_entry == &existing_entry) {
        existing_entry.generation()->notice_weight_change(existing_entry, prev_weight);
    }
    else {
        cache->replace_entry(*new_entry, existing_entry, prev_weight);
    }

}

void Cache::delitem(OID_t key, TID_t tid)
{
    if_existing(key,);

    const size_t old_weight = existing_entry.weight();
    ICacheEntry* new_entry = existing_entry.discarding_tids_before(tid);
    _update(this, existing_entry, old_weight, new_entry);
}

void Cache::freeze(OID_t key, TID_t tid)
{
    if_existing(key,);
    const size_t old_weight = existing_entry.weight();
    ICacheEntry* new_entry = existing_entry.freeze_to_tid(tid);
    _update(this, existing_entry, old_weight, new_entry);
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

void Cache::age_frequencies()
{
    OidEntryMap::iterator end = this->data.end();
    for (OidEntryMap::iterator it = this->data.begin(); it != end; it++) {
        it->frequency = it->frequency / 2;
        // TODO: Shouldn't we remove them if they hit 0?
    }
}

size_t Cache::len() {
    return this->data.size();
}

void Cache::on_hit(OID_t key)
{
    ICacheEntry& entry = *this->data.find(key);
    entry.generation()->on_hit(*this, entry);
}

size_t Cache::weight() const
{
    return this->ring_eden.sum_weights()
        + this->ring_protected.sum_weights()
        + this->ring_probation.sum_weights()
        + sizeof(Cache);
}

// Local Variables:
// flycheck-clang-include-path: ("/opt/local/include" "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/include/python2.7")
// End:
