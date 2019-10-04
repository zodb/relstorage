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

#include "c_cache.h"
#include "c_ring.h"

using namespace relstorage::cache;

#define UNUSED(expr) do { (void)(expr); } while (0)

struct _LTE {
    TID_t tid;
    _LTE(TID_t t) : tid(t) {}
    bool operator()(SingleValueEntry_p p) {
        return p.get()->tid <= this->tid;
    }
};

struct _LT {
    TID_t tid;
    _LT(TID_t t) : tid(t) {}
    bool operator()(SingleValueEntry_p p) {
        return p.get()->tid < this->tid;
    }
};

void MultipleValueEntry::remove_tids_lte(TID_t tid) {
    this->p_values.remove_if(_LTE(tid));
}

void MultipleValueEntry::remove_tids_lt(TID_t tid) {
    this->p_values.remove_if(_LT(tid));
}

size_t MultipleValueEntry::weight()
{
    size_t result = 0;
    for (std::list<SingleValueEntry_p>::iterator it = this->p_values.begin();
         it != this->p_values.end();
         it++ ) {
        result += (*it)->weight();
    }
    return result;
}


void Generation::on_hit(Cache* cache, AbstractEntry* entry)
{
    UNUSED(cache);
    rsc_on_hit(*this, entry);
}

void Probation::on_hit(Cache* cache, AbstractEntry* entry)
{
    rsc_probation_on_hit(*cache, entry);
}

void Cache::_handle_evicted(RSRing& evicted) {
    if (!evicted.r_next) {
        // nothing evicted.
        return;
    }
    // From now on we're dealing with raw pointers to our AbstractEntry
    // subclass that were kept alive with shared pointers in data. As we remove
    // them, their r_next entry may go bad, so we need to grab that first.
    AbstractEntry* next = static_cast<AbstractEntry*>(evicted.r_next);
    while (next) {
        const OID_t key = next->key;
        next = static_cast<AbstractEntry*>(next->r_next);
        // things were evicted, must be removed from our data, which
        // is the only place holding a reference to them, aside from transient
        // SingleValue Python nodes.
        this->data.erase(key);
    }
}

void Cache::add_to_eden(SingleValueEntry_p sve_p)
{
    OID_t key = sve_p->key;
    if (this->data.count(key)) {
        throw std::runtime_error("Key already present");
    }

    // keep with the shared ownership.
    this->data[key] = sve_p;
    RSRing evicted = rsc_eden_add(*this, sve_p.get());
    this->_handle_evicted(evicted);
}

Generation* Cache::generation_for_entry(AbstractEntry* entry) {
    switch (entry->generation) {
    case GEN_EDEN:
        return static_cast<Generation*>(this->ring_eden);
        break;
    case GEN_PROBATION:
        return static_cast<Generation*>(this->ring_probation);
        break;
    case GEN_PROTECTED:
        return static_cast<Generation*>(this->ring_protected);
        break;
    default:
        throw std::domain_error("Invalid generation");

    }
}

/**
 * Does not rebalance rings. Use only when no evictions are necessary.
 */
void Cache::replace_entry(AbstractEntry_p new_entry, AbstractEntry_p prev_entry,
                          size_t prev_weight ) {
    Generation* generation = this->generation_for_entry(prev_entry.get());
    // Need the prev_weight incase new_entry is prev_entry and mutated
    // in place.
    generation->sum_weights -= prev_weight;
    generation->sum_weights += new_entry->weight();

    if (new_entry != prev_entry) {
        // Must replace in our ring and our map.

        // link in the new one
        new_entry->r_next = prev_entry->r_next;
        new_entry->r_prev = prev_entry->r_prev;
        // remove the old one
        prev_entry->r_prev->r_next = new_entry.get();
        prev_entry->r_next->r_prev = new_entry.get();
        // copy frequency and generation pointers
        new_entry->frequency = prev_entry->frequency;
        new_entry->generation = prev_entry->generation;

        // Replace the shared pointer, possibly causing
        // prev_entry to go invalid.
        this->data[new_entry->key] = new_entry;
    }
}

/**
 * Update an existing entry, replacing its value contents
 * and making it most-recently-used. The key must already
 * be present. Possibly evicts items if the entry grew.
 */
void Cache::update_MRU(AbstractEntry_p new_entry) { // R.34: will retain refcount
    // TODO: copying state here.
    AbstractEntry_p prev_entry = this->data.at(new_entry->key);
    size_t old_weight = prev_entry->weight();
    Generation* generation = this->generation_for_entry(prev_entry.get());
    this->replace_entry(new_entry, prev_entry, old_weight);

    if (old_weight >= new_entry->weight() ) {
        // If we shrunk, we couldn't evict anything, so
        // no need to shuffle around the complexities of that,
        // just act like a hit.
        generation->on_hit(this, new_entry.get());
    }
    else {
        RSRing evicted = rsc_update_mru(*this, *generation, new_entry.get(),
                                        old_weight, new_entry->weight());
        this->_handle_evicted(evicted);
    }
}

/**
 * Remove an existing key.
 */
void Cache::delitem(OID_t key) {
    if (!this->data.count(key)) {
        return;
    }

    AbstractEntry* entry = this->data[key].get();
    rsc_ring_del(*this->generation_for_entry(entry), entry);
    this->data.erase(key);
}

bool Cache::contains(const OID_t key) const {
    return this->data.count(key) == 1;
}

const AbstractEntry_p& Cache::get(const OID_t key) const {
    return this->data.at(key);
}

void Cache::age_frequencies() {
    rsc_age_lists(*this);
}

size_t Cache::len() {
    return this->data.size();
}

void Cache::on_hit(OID_t key) {
    AbstractEntry* entry = this->data[key].get();
    this->generation_for_entry(entry)->on_hit(this, entry);
}

size_t Cache::weight() const
{
    return this->ring_eden->sum_weights
        + this->ring_protected->sum_weights
        + this->ring_probation->sum_weights;
}
