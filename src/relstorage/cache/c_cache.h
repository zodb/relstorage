#ifndef C_CACHE_H
#define C_CACHE_H

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
 *
 * All data will be owned by the Cache object, and when it is evicted
 * it may be removed from memory. If it is currently being used from
 * python, it must be protected from that happening. To that end, we
 * use shared_ptr to allow for reference counting. See
 * http://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#Rr-smartptrparam
 * for guidelines on using shared_ptr as parameters:
 *
 * - Take smart pointers as parameters only to explicitly express
 * lifetime semantics
 *
 * - R.36 Take a const shared_ptr<widget>& parameter to express that
 * it might retain a reference count to the object
 */

/** Basic types */
/* The compiler used for Python 2.7 on Windows doesn't include
   either stdint.h or cstdint.h. Nor does it understand nullptr or have
   std::shared_ptr. Sigh. */
#if defined(_MSC_VER) &&  _MSC_VER <= 1500
typedef unsigned long long uint64_t;
typedef signed long long int64_t;
#define nullptr NULL
#else
#include <cstdint>
#endif
#include <string>
#include <utility>
#include <memory>
#include <list>
#include <unordered_map>
#include <functional>

#include "c_ring.h"


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
                OID_t key;
                AbstractEntry(const OID_t key) : key(key)  {

                }

                AbstractEntry() : key(-1) {}

                virtual size_t weight() { return 0; }
                virtual size_t len() { return 0; }

        };

        typedef std::shared_ptr<AbstractEntry> AbstractEntry_p;

        class SingleValueEntry : public AbstractEntry {
            public:
                // these are only modified when we construct an array.
                // maybe an assignment operator?
                Pickle_t state;
                TID_t tid;
                // This is only modified in one special circumstance,
                // to avoid any copies. But maybe move semantics take care
                // of that for us?
                bool frozen;

                // Some C++ libraries don't support variardics to
                // make_shared(), topping out at 3 arguments. Those
                // are the ones that also tend not to fully support
                // C++ 11 and its delegating constructors, so we use a
                // default argument to make it possible to create
                // these.
                SingleValueEntry(OID_t key, Pickle_t state, TID_t tid, bool frozen=false)
                 : AbstractEntry(key), state(state), tid(tid), frozen(frozen)
                {}
                SingleValueEntry(OID_t key, std::pair<Pickle_t, TID_t> state, bool frozen)
                    : AbstractEntry(key), state(state.first), tid(state.second), frozen(frozen)
                {}
                SingleValueEntry() : AbstractEntry(), state(), tid(-1), frozen(false) {}
                virtual size_t weight() { return this->state.size(); }
        };

        typedef std::shared_ptr<SingleValueEntry> SingleValueEntry_p;

        class MultipleValueEntry : public AbstractEntry {
            private:

            public:
                std::list<SingleValueEntry_p> p_values;
                MultipleValueEntry(const OID_t key) : AbstractEntry(key) {}
                void push_back(SingleValueEntry_p entry) {this->p_values.push_back(entry);}
                void remove_tids_lte(TID_t tid);
                void remove_tids_lt(TID_t tid);

                virtual size_t len() { return this->p_values.size(); }
                virtual size_t weight();
        };

        typedef std::shared_ptr<MultipleValueEntry> MultipleValueEntry_p;

        class Cache;

        class Generation : public RSRing {
            public:

            Generation(rs_counter_t limit, generation_num generation)
                : RSRing(limit, generation)
            {}

            virtual void on_hit(Cache* cache, AbstractEntry* entry);
        };

        class Probation : public Generation {
            public:
            Probation(rs_counter_t limit)
                : Generation(limit, GEN_PROBATION)
            {}
            virtual void on_hit(Cache* cache, AbstractEntry* entry);
        };

        /**
         * The cache itself is three generations. See individual methods
         * or the Python code for information about how items move between rings.
         */
        class Cache : public RSCache {
            private:
            std::unordered_map<OID_t, AbstractEntry_p> data;
            void _handle_evicted(RSRing& evicted);
            Generation* generation_for_entry(AbstractEntry* entry);

            public:

            Cache(rs_counter_t eden_limit, rs_counter_t protected_limit, rs_counter_t probation_limit)
                : RSCache()
            {
                 this->ring_eden = new Generation(eden_limit, GEN_EDEN);
                 this->ring_protected = new Generation(protected_limit, GEN_PROTECTED);
                 this->ring_probation = new Probation(probation_limit);
            }

            virtual ~Cache() {
                delete this->ring_eden;
                delete this->ring_protected;
                delete this->ring_probation;
            }

            // We'd much prefer to return const map&, but Cython fails to iterate
            // that for some reason.
            std::unordered_map<OID_t, AbstractEntry_p>& getData() {
                return this->data;
            }

            virtual size_t weight() const;

            /**
             * Add a new entry for the given state if one does not already exist.
             * It becomes the first entry in eden. If this causes the cache to be oversized,
             * entries are freed.
             */
            void add_to_eden(SingleValueEntry_p sve_p);


            /**
             * Does not rebalance rings. Use only when no evictions are necessary.
             */
            void replace_entry(AbstractEntry_p new_entry, AbstractEntry_p prev_entry,
                               size_t prev_weight );

            /**
             * Update an existing entry, replacing its value contents
             * and making it most-recently-used. The key must already
             * be present. Possibly evicts items if the entry grew.
             */
            void update_MRU(AbstractEntry_p new_entry);  // R.34: will retain refcount

            /**
             * Remove an existing key.
             */
            void delitem(OID_t key);

            bool contains(const OID_t key) const;

            const AbstractEntry_p& get(const OID_t key) const;

            void age_frequencies();
            size_t len();
            void on_hit(OID_t key);
        };

    };

}

#endif
