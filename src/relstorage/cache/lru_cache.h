#ifndef LRU_CACHE_H
#define LRU_CACHE_H

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

/**
 * Support for implementing a LRU generational cache using
 * Cython and C++.
 *
 * One of the primary goals is memory efficiency; a secondary goal
 * is to keep the majority of data off the Python heap, specifically
 * so that it doesn't interact with the garbage collector.
 *
 * To that end, the actual cached data will be owned by C++. Python
 * will copy in and out of it as necessary on access.
 *
 * The vast majority of the work will be done in cython cdef functions
 * operating only on C++ data structures. They must hold the GIL and they
 * must appear to be atomic; this allows the Python level to avoid extra locks.
 */

/** Basic types */
/* The compiler used for Python 2.7 on Windows doesn't include
   either stdint.h or cstdint.h. Sigh. */
#if defined(_MSC_VER) and  _MSC_VER <= 1500
typedef unsigned long long uint64_t;
typedef signed long long int64_t;
#else
#include <cstdint>
#endif
#include <string>
#include <utility>
#include <memory>
#include <list>
#include <unordered_map>
#include <functional>

#include "cache_ring.h"

typedef int64_t TID_t;
// OIDs start at zero and go up from there. We use
// a signed type though to distinguish uninitialized values:
// they'll be less than 0.
typedef int64_t OID_t;
typedef std::string Pickle_t;

namespace relstorage {

    namespace cache {

        class AbstractEntry : public RSRingNode {
            public:
                const OID_t key;
                AbstractEntry(const OID_t key) : key(key) {
                    this->r_prev = nullptr;
                    this->r_next = nullptr;
                    this->u.entry.r_parent = -1;
                }
                virtual ~AbstractEntry() {
                    // remove from the ring.
                    this->r_prev = nullptr;
                    this->r_next = nullptr;
                }
                virtual size_t weight() { return 0; }
        };

        typedef std::shared_ptr<AbstractEntry> AbstractEntry_p;

        class SingleValueEntry : public AbstractEntry {
            public:
                const Pickle_t state;
                const TID_t tid;
                const bool frozen;

                SingleValueEntry(OID_t key, Pickle_t state, TID_t tid, bool frozen)
                 : AbstractEntry(key), state(state), tid(tid), frozen(frozen)
                {}
                size_t weight() { return this->state.size(); }
        };

        typedef std::shared_ptr<SingleValueEntry> SingleValueEntry_p;

        class _LTE {
            public:
                TID_t tid;
                _LTE(TID_t t) : tid(t) {}
                bool operator()(SingleValueEntry_p p) {
                    return p.get()->tid <= this->tid;
                }
        };
        class _LT {
            public:
                TID_t tid;
                _LT(TID_t t) : tid(t) {}
                bool operator()(SingleValueEntry_p p) {
                    return p.get()->tid < this->tid;
                }
        };

        class MultipleValueEntry : public AbstractEntry {
            private:

            public:
                std::list<SingleValueEntry_p> p_values;
                MultipleValueEntry(const OID_t key) : AbstractEntry(key) {}
                void push_back(SingleValueEntry_p entry) {this->p_values.push_back(entry);}
                void remove_tids_lte(TID_t tid) {
                    this->p_values.remove_if(_LTE(tid));
                }

                void remove_tids_lt(TID_t tid) {
                    this->p_values.remove_if(_LT(tid));
                }

                size_t weight() {
                    size_t result = 0;
                    for (std::list<SingleValueEntry_p>::iterator it = this->p_values.begin();
                         it != this->p_values.end();
                         it++ ) {
                        result += (*it)->weight();
                    }
                    return result;
                }
        };

        typedef enum { // avoid 0 to catch initialization issues
           GEN_EDEN = 1,
           GEN_PROTECTED = 2,
           GEN_PROBATION = 3
        } generation_num;

        class Cache;

        class Generation : public RSRingNode {
            public:

            Generation(rs_counter_t limit, generation_num generation_num) {
                this->u.head.max_weight = limit;
                this->r_next = this->r_prev = this;
                this->u.head.generation = generation_num;
            }

            void on_hit(Cache* _, AbstractEntry* entry) {
                rsc_on_hit(this, entry);
            }
        };

        class Probation : public Generation {
            void on_hit(Cache* cache, AbstractEntry* entry) {
                rsc_probation_on_hit((RSCache*)(cache), entry);
            }
        };

        /**
         * The cache itself is three generations. See individual methods
         * or the Python code for information about how items move between rings.
         */
        class Cache : public RSCache {
            private:
            std::unordered_map<OID_t, AbstractEntry_p> data;
            public:
            Cache(rs_counter_t eden_limit, rs_counter_t protected_limit, rs_counter_t probation_limit) {
                this->ring_eden = new Generation(eden_limit, GEN_EDEN);
                this->ring_protected = new Generation(protected_limit, GEN_PROTECTED);
                this->ring_probation = new Generation(probation_limit, GEN_PROBATION);
            }

            virtual ~Cache() {
                delete this->ring_eden;
                delete this->ring_protected;
                delete this->ring_probation;
                this->ring_eden = this->ring_protected = this->ring_probation = nullptr;
            }

            void _handle_evicted(RSRingNode& evicted) {
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

            /**
             * Add a new entry for the given state if one does not already exist.
             * It becomes the first entry in eden. If this causes the cache to be oversized,
             * entries are freed.
             */
            void add_to_eden(SingleValueEntry* sve) {
                OID_t key = sve->key;
                if (this->data.count(key)) {
                    // Probably not the best. Maybe we should take a ref?
                    delete sve;
                    return;
                }

                this->data[key] = SingleValueEntry_p(sve);
                RSRingNode evicted = rsc_eden_add(this, sve);
                this->_handle_evicted(evicted);
            }

            Generation* generation_for_entry(AbstractEntry* entry) {
                Generation* generation;
                switch (entry->u.entry.r_parent) {
                    case GEN_EDEN:
                        generation = static_cast<Generation*>(this->ring_eden);
                    break;
                    case GEN_PROBATION:
                        generation = static_cast<Generation*>(this->ring_probation);
                    break;
                    default:
                        generation = static_cast<Generation*>(this->ring_protected);
                }
                return generation;
            }

            /**
             * Update an existing entry, replacing its value contents. The key must already be present.
             */
            void update_MRU(AbstractEntry* new_entry) {
                // TODO: copying state here.
                AbstractEntry* prev_entry = this->data[new_entry->key].get();
                size_t old_weight = prev_entry->weight();
                // link in the new one
                new_entry->r_next = prev_entry->r_next;
                new_entry->r_prev = prev_entry->r_prev;
                // remove the old one
                prev_entry->r_prev->r_next = new_entry;
                prev_entry->r_next->r_prev = new_entry;
                // copy frequency and generation pointers
                new_entry->u.entry.frequency = prev_entry->u.entry.frequency;
                new_entry->u.entry.r_parent = prev_entry->u.entry.r_parent;

                // Replace the shared pointer, possibly causing
                // prev_entry to go invalid.
                this->data[new_entry->key].reset(new_entry);
                Generation* generation = this->generation_for_entry(new_entry);

                if (old_weight >= new_entry->weight() ) {
                    // If we shrunk, we couldn't evict anything, so
                    // no need to shuffle around the complexities of that,
                    // just act like a hit.
                    generation->on_hit(this, new_entry);
                }
                else {
                    RSRingNode evicted = rsc_update_mru(this, generation, new_entry,
                                                        old_weight, new_entry->weight());
                    this->_handle_evicted(evicted);
                }

            }

            /**
             * Remove an existing key.
             */
            void delitem(OID_t key) {
                if (!this->data.count(key)) {
                    return;
                }

                AbstractEntry* entry = this->data[key].get();
                rsc_ring_del(this->generation_for_entry(entry), entry);
                this->data.erase(key);
            }

            bool contains(OID_t key) {
                return this->data.count(key) == 1;
            }

            AbstractEntry_p get(OID_t key) {
                return this->data.at(key);
            }

            void age_frequencies() {
                rsc_age_lists(this);
            }

            size_t len() {
                return this->data.size();
            }

            void on_hit(OID_t key) {
                AbstractEntry* entry = this->data[key].get();
                this->generation_for_entry(entry)->on_hit(this, entry);
            }
        };

    };

}

#endif
