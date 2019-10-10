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
 * it might retain a reference count to the object.
 *
 * Previously, we rolled our own doubly-linked list implementation.
 * But in C++ that's not needed, we can just use std::list:
 * - iterators are never invalidated (except for erased elements),
 *   so if an entry knows its own iterator (which it can capture at insertion time)
 *   then it can remove itself from the list with no further access.
 * - The list node type just directly embeds the list value type, so there's
 *   no extra overhead there.
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
#define UNUSED(expr) do { (void)(expr); } while (0)

#include <string>
#include <list>
#include <vector>
#include <unordered_map>
#include <assert.h>
#include <string.h>
#include <iostream>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
using namespace boost::intrusive;

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

extern "C" {
    #include "Python.h"
}

#if RS_COPY_STRING
typedef std::string rs_string;
#else
typedef PyObject* rs_string;
#endif


namespace relstorage {
namespace cache {
    typedef int64_t TID_t;
    // OIDs start at zero and go up from there. We use
    // a signed type though to distinguish uninitialized values:
    // they'll be less than 0.
    typedef int64_t OID_t;
    typedef rs_string Pickle_t;

    typedef
    enum {
      GEN_UNKNOWN = -1,
      GEN_EDEN = 1,
      GEN_PROTECTED = 2,
      GEN_PROBATION = 3
    } generation_num;

    struct EntryListTag;
    struct EntryMapTag;

    class Cache;
    class Generation;
    class ICacheEntry;
    class SVCacheEntry;

    // Compact storage of OIDs. Expected to remain small.
    typedef std::vector<OID_t> OidList;
    // A general list of cache entries. Used for generations and
    // multiple entries for the same key. A given ICacheEntry can be
    // in exactly one such list at a time.
    typedef list_base_hook<tag<EntryListTag>, link_mode<auto_unlink> > EntryListHook;
    typedef boost::intrusive::list<
        ICacheEntry,
        // no constant time size saves space, and lets items auto-unlink
        // if they get deleted.
        constant_time_size<false>,
        // any given entry can be in the main generation list,
        // or in the list for a MultipleValue
        base_hook<EntryListHook>
        > EntryList;
    typedef boost::intrusive::list<
        SVCacheEntry,
        constant_time_size<false>,
        base_hook<EntryListHook>
        > SVEntryList;

    // A map[oid] = ICacheEntry, as maintained by the cache for lookup.
    // Entries can be in exactly one such map at a time.
    struct OID_is_key;
    typedef set_base_hook<tag<EntryMapTag>, link_mode<auto_unlink> > EntryMapHook;
    typedef set<ICacheEntry,
                key_of_value<OID_is_key>,
                base_hook<EntryMapHook>,
                constant_time_size<false>
                > OidEntryMap;

    /**
     * Base interface for cache entries.
     * A cache entry associates an OID/TID pair with a string value.
     * There are different ways to do that, this just defines the OID.
     *
     * Cache values may not be copied. They are semi reference-counted
     * from Python so when one is removed it can be destroyed.
     */
    class ICacheEntry
        : public EntryListHook,
          public EntryMapHook {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(ICacheEntry)
        Generation* _generation;
    public:
        // The key for this entry.
        OID_t key; //FIXME: Make const.
        // How popular this item is.
        int frequency;


        ICacheEntry()
            : EntryListHook(),
              EntryMapHook(),
              _generation(nullptr),
              key(-1),
              frequency(1)
        {
        }

        ICacheEntry(OID_t key)
            : EntryListHook(),
              EntryMapHook(),
              _generation(nullptr),
              key(key),
              frequency(1)
        {
        }

        // FIXME: Probably want to disable copy.

        virtual ~ICacheEntry() {} // nothing to do .

        RSR_INLINE bool in_cache() const
        {
            return this->_generation;
        }

        RSR_INLINE Generation*& generation()
        {
            return this->_generation;
        }

        RSR_INLINE Generation* generation() const
        {
            return this->_generation;
        }

        RSR_INLINE void generation(Generation* p)
        {
            this->_generation = p;
        }

        virtual size_t overhead() const
        {
            return sizeof(ICacheEntry);
        }

        /**
         * The total cost of this object.
         */
        virtual size_t weight() const
        {
            // include:
            // the size of this object, which should be the only overhead
            // we actually have due to the use of intrusive containers.
            return this->overhead();
        }

        /**
         * How many values this entry  is tracking.
         */
        virtual size_t value_count() const
        {
            return 0;
        }

        void unlink_from_list()
        {
            EntryListHook::unlink();
        }

        /**
         * Return either this or a new value.
         */
        virtual ICacheEntry* with_later(const Pickle_t&,
                                        const TID_t new_tid) = 0;

        virtual ICacheEntry* freeze_to_tid(const TID_t tid) = 0;
        virtual ICacheEntry* discarding_tids_before(const TID_t tid) = 0;
        virtual std::vector<TID_t> all_tids() const = 0;
    };

    // key_of_value function object, must:
    //- be default constructible if the container constructor requires it
    //- define the key type using "type"
    //- define an operator() taking "const value_type&" and
    //    returning "type" or "const type &"
    struct OID_is_key
    {
        typedef OID_t type;

        const type& operator()(const ICacheEntry& v) const
        {
            return v.key;
        }
    };



    class SVCacheEntry : public ICacheEntry {
    private:
        Pickle_t _pickle; // FIXME: Make const
        TID_t _tid;
        bool _frozen;
        BOOST_MOVABLE_BUT_NOT_COPYABLE(SVCacheEntry)
#if RS_COPY_STRING
        inline Pickle_t& _incref() const { return this->_pickle; }
        inline void _decref() {}
#else
        inline const Pickle_t& _incref() const
        {
            Py_INCREF(this->_pickle);
            return this->_pickle;
        }

        inline void _decref()
        {
            Py_DECREF(this->_pickle);
        }
#endif

    public:
        SVCacheEntry(OID_t key, const Pickle_t& pickle,TID_t tid)
            : ICacheEntry(key),
              _pickle(pickle),
              _tid(tid),
              _frozen(false)
        {
            _incref();
        }


        SVCacheEntry()
            : ICacheEntry(),
              _pickle(),
              _tid(-1),
              _frozen(false)
        {
        }

        virtual ~SVCacheEntry()
        {
            _decref();
        }

        SVCacheEntry& operator=(const SVCacheEntry& other);

        virtual std::vector<TID_t> all_tids() const
        {
            // initialize with 1 element, each having the value _tid
            return std::vector<TID_t>(1, _tid);
        }

        // Copies
        void force_update_during_bulk_load(OID_t key, Pickle_t pickle,TID_t tid)
        {
            assert(!_pickle);
            this->key = key;
            this->_tid = tid;
            this->_pickle = pickle;
            _incref();
        }

#if RS_COPY_STRING
        PyObject* as_object()
        {
            return PyBytes_FromStringAndSize(this->_pickle.data(),
                                             this->_pickle.size());
        }

        size_t size() const
        {
            return this->_pickle.size();
        }

        bool state_eq(const Pickle_t& other) const
        {
            return _pickle == other;
        }
#else
        // Cython leaves refcounting to us. It assumes we return
        // an incremented reference and that we hold a reference when we're
        // constructed until we're destructed.
        PyObject* as_object() const
        {
            return _incref();
        }

        size_t size() const
        {
            ssize_t s = PyBytes_Size(this->_pickle);
            if (s < 0) {
                throw std::runtime_error("Size not valid");
            }
            return static_cast<size_t>(s);
        }

        bool state_eq(const Pickle_t& other) const
        {
            // Note: Ignoring error return here.
            // Temporarily borrowing the reference to other.
            return PyObject_RichCompareBool(_pickle, other, Py_EQ);
        }

#endif
        const Pickle_t& state() const
        {
            return _incref();
        }

        TID_t tid() const
        {
            return this->_tid;
        }

        bool& frozen()
        {
            return this->_frozen;
        }

        bool frozen() const
        {
            return this->_frozen;
        }

        virtual size_t weight() const
        {
            // include: the bytes for the cached object,
            return ICacheEntry::weight() + this->size();
        }

        virtual size_t overhead() const
        {
            return sizeof(SVCacheEntry);
        }

        virtual size_t value_count() const
        {
            return 1;
        }

        // Use a value less than 0 for what would be None in Python.
        virtual bool tid_matches(TID_t tid) const
        {
            return this->_tid == tid || (tid < 0 && this->_frozen);
        }

        virtual ICacheEntry* with_later(const Pickle_t&,
                                        const TID_t new_tid);
        virtual ICacheEntry* freeze_to_tid(const TID_t tid);
        virtual ICacheEntry* discarding_tids_before(const TID_t tid);

    };



    class MVCacheEntry : public ICacheEntry {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(MVCacheEntry)
        SVEntryList p_values;
    public:
        // Types
        typedef SVEntryList::const_iterator iterator;
        MVCacheEntry(const OID_t key) : ICacheEntry(key) {}
        void push_back(SVCacheEntry& entry) {this->p_values.push_back(entry);}
        void remove_tids_lte(TID_t tid);
        void remove_tids_lt(TID_t tid);

        virtual size_t value_count() const
        {
            return this->p_values.size();
        }
        virtual size_t weight() const;
        virtual size_t overhead() const
        {
            return sizeof(MVCacheEntry);
        }

        const SVCacheEntry& front() const
        {
            return this->p_values.front();
        }

        SVCacheEntry& front()
        {
            return this->p_values.front();
        }

        bool empty() const
        {
            return this->p_values.empty();
        }

        bool degenerate() const
        {
            return this->p_values.size() == 1;
        }

        iterator begin() const
        {
            return this->p_values.cbegin();
        }

        iterator end() const
        {
            return this->p_values.cend();
        }

        virtual std::vector<TID_t> all_tids() const
        {
            std::vector<TID_t> result;
            for(iterator it = begin(), _end = end(); it != _end; ++it) {
                result.push_back(it->tid());
            }
            return result;
        }

        virtual ICacheEntry* with_later(const Pickle_t&,
                                        const TID_t new_tid);
        virtual ICacheEntry* freeze_to_tid(const TID_t tid);
        virtual ICacheEntry* discarding_tids_before(const TID_t tid);
    };

    class Cache;

    class Generation {
    private:
        // The sum of their weights.
        size_t _sum_weights;
        size_t _max_weight;
        friend class Cache;
        BOOST_MOVABLE_BUT_NOT_COPYABLE(Generation)
    protected:
        EntryList _entries;
        void change_max_weight(size_t new_limit)
        {
            _max_weight = new_limit;
        }
    public:
        // types
        typedef EntryList::const_iterator iterator;
        const generation_num generation;

        Generation(size_t limit, generation_num generation)
            : _sum_weights(0),
              _max_weight(limit),
              generation(generation)
        {

        }
        size_t max_weight() const
        {
            return _max_weight;
        }
        iterator begin() const
        {
            return this->_entries.begin();
        }

        iterator end() const
        {
            return this->_entries.end();
        }

        RSR_INLINE size_t sum_weights() const
        {
            return this->_sum_weights;
        }

        RSR_INLINE bool oversize() const
        {
            return this->_sum_weights > this->_max_weight;
        }

        RSR_INLINE bool empty() const
        {
            return this->_entries.empty();
        }

        size_t len() const
        {
            return this->_entries.size();
        }

        RSR_INLINE int will_fit(const ICacheEntry& entry)
        {
            return this->_max_weight >= (entry.weight() + this->_sum_weights);
        }

        RSR_INLINE const ICacheEntry* lru() const
        {
            if (this->empty())
                return nullptr;
            return &this->_entries.back();
        }

        RSR_INLINE ICacheEntry* lru()
        {
            if (this->empty())
                return nullptr;
            return &this->_entries.back();
        }

        EntryList& iter()
        {
            return this->_entries;
        }

        RSR_INLINE void notice_weight_change(ICacheEntry& entry,
                                             size_t old_weight)
        {
            assert(entry.generation() == this);
            this->_sum_weights -= old_weight;
            this->_sum_weights += entry.weight();
        }

        /**
         * elt must already be in the list. It's
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
        RSR_INLINE void move_to_head(ICacheEntry& elt)
        {
            elt.unlink_from_list();
            this->_entries.push_front(elt);
        }

        /**
         * Accept elt, which must be in another node, to be the new
         * head of this ring. This may oversize this node.
         */
        RSR_INLINE void adopt(ICacheEntry& elt);

        /**
         * Remove elt from the list. elt must already be in the list.
         *
         * Constant time.
         */
        RSR_INLINE void remove(ICacheEntry& elt);


        RSR_INLINE void replace_entry(ICacheEntry& incoming,
                                      size_t old_weight,
                                      ICacheEntry& old)
        {
            assert(!incoming.in_cache());
            assert(old.generation() == this);

            this->_sum_weights -= old_weight;
            this->_sum_weights += incoming.weight();
            // When we erase, the iterator goes invalid, so we must put it in
            // first
            if (&old != &incoming) {
                incoming.generation() = this;
                dynamic_cast<EntryListHook&>(incoming).swap_nodes(dynamic_cast<EntryListHook&>(old));
                old.unlink_from_list();
                // FIXME: Possibly need to delete here.
                old.generation() = nullptr;
            }
        }

        /**
         * Add elt as the most recently used object.  elt must not already be
         * in any list.
         *
         * Constant time.
         */
        virtual void add(ICacheEntry& elt);

        /**
         * Record that the entry has been used.
         * This updates its popularity counter  and makes it the
         * most recently used item in its ring. This is constant time.
         */
        virtual void on_hit(Cache& cache, ICacheEntry& entry);

        // Two rings are equal iff they are the same object.
        virtual bool operator==(const Generation& other) const;

    };

    class Eden : public Generation {
    private:
        OidList rejects;
        Cache& cache;
        Eden(size_t limit, Cache& cache) : Generation(limit, GEN_EDEN), cache(cache) {}
        friend class Cache;
    public:

        /**
         * Add elt as the most recently used object.  elt must not already be
         * in any list.
         *
         * Evict items from the cache, if needed, to get all the rings
         * down to size. Return a list of the keys evicted.
         */
        OidList& add_and_evict(ICacheEntry& elt);

    };

    class Protected : public Generation {
    private:
        Protected(size_t limit) : Generation(limit, GEN_PROTECTED) {}
        friend class Cache;
    public:
        const static generation_num generation = GEN_PROTECTED;
    };

    class Probation : public Generation {
    private:
        OidList no_rejects;
        Probation(size_t limit) : Generation(limit, GEN_PROBATION) {}
        friend class Cache;
    public:

        virtual void on_hit(Cache& cache, ICacheEntry& entry);
    };


    /**
     * The cache itself is three generations. See individual methods
     * or the Python code for information about how items move between rings.
     */
    class Cache {
    private:
        OidEntryMap data;
        OidList rejects;
        void update_mru(ICacheEntry& entry);
        void _handle_evicted(OidList& evicted);
        BOOST_MOVABLE_BUT_NOT_COPYABLE(Cache)
    public:
        // types
        typedef OidEntryMap::const_iterator iterator;

        Eden ring_eden;
        Protected ring_protected;
        Probation ring_probation;

        Cache(size_t eden_limit=0, size_t protected_limit=0, size_t probation_limit=0)
            : ring_eden(eden_limit, *this),
              ring_protected(protected_limit),
              ring_probation(probation_limit)
        {
        }

        void resize(size_t eden, size_t protected_limit, size_t probation_limit)
        {
            ring_eden.change_max_weight(eden);
            ring_protected.change_max_weight(protected_limit);
            ring_probation.change_max_weight(probation_limit);
        }

        virtual ~Cache()
        {
        }

        size_t max_weight() const
        {
            return ring_eden.max_weight() + ring_probation.max_weight() + ring_protected.max_weight();
        }

        iterator begin() const
        {
            return this->data.begin();
        }

        iterator end() const
        {
            return this->data.end();
        }

        RSR_INLINE bool oversize()
        {
            return this->ring_eden.oversize()
                || this->ring_protected.oversize()
                || this->ring_probation.oversize(); // this used to be &&
        }


        RSR_INLINE int will_fit(const ICacheEntry& entry)
        {
            return this->ring_eden.will_fit(entry)
                || this->ring_probation.will_fit(entry)
                || this->ring_protected.will_fit(entry);
        }

        size_t weight() const;

        /**
         * Add a new entry for the given state if one does not already exist.
         * It becomes the first entry in eden. If this causes the cache to be oversized,
         * entries are freed.
         */
        void add_to_eden(OID_t key, const Pickle_t& state,TID_t tid);

        /**
         * Does not rebalance rings. Use only when no evictions are necessary.
         */
        void replace_entry(ICacheEntry& new_entry, ICacheEntry& prev_entry,
                           size_t prev_weight );

        /**
         * Update an existing entry, replacing its value contents
         * and making it most-recently-used. The key must already
         * be present. Possibly evicts items if the entry grew.
         */
        void store_and_make_MRU(OID_t oid,
                                const Pickle_t& state,
                                const TID_t new_tid);

        /**
         * Remove an existing key.
         */
        void delitem(OID_t key);
        /**
          * Remove entries older than the tid.
          */
        void delitem(OID_t key, TID_t tid);

        /**
          * Freeze the entry for the tid.
          */
        void freeze(OID_t key, TID_t tid);

        bool contains(const OID_t key) const;

        ICacheEntry* get(const OID_t key);

        void age_frequencies();
        size_t len();
        void on_hit(OID_t key);
        int add_many(SVCacheEntry* shared_ptr_to_array,
                     int entry_count);

    };

} // namespace cache

} // namespace relstorage

#endif

// Local Variables:
// flycheck-clang-include-path: ("/opt/local/include" "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/include/python2.7")
// End:
