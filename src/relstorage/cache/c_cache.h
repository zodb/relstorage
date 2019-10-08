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
    void* PyObject_Malloc(size_t);
    void PyObject_Free(void* p);
#ifndef Py_PYTHON_H
    struct PyObject;
    PyObject* PyBytes_FromStringAndSize(char* buf, size_t len);
    void Py_INCREF(const PyObject* o);
    void Py_XDECREF(const PyObject* o);
    char* PyBytes_AsString(const PyObject* o);
#endif
}



typedef int64_t TID_t;
// OIDs start at zero and go up from there. We use
// a signed type though to distinguish uninitialized values:
// they'll be less than 0.
typedef int64_t OID_t;
typedef std::string Pickle_t;

namespace relstorage
{
namespace cache
{
    typedef enum
        {
         GEN_UNKNOWN = -1,
         GEN_EDEN = 1,
         GEN_PROTECTED = 2,
         GEN_PROBATION = 3
        }
        generation_num;
    class Generation;
    class AbstractEntry;
    typedef AbstractEntry* AbstractEntry_raw_p;
    class SingleValueEntry;

    typedef std::vector<OID_t> OidList;
    typedef std::list<AbstractEntry* > EntryList;
    typedef EntryList::iterator EntryListIterator;
    typedef std::shared_ptr<AbstractEntry> AbstractEntry_p;
    typedef std::shared_ptr<SingleValueEntry> SingleValueEntry_p;

    /**
     * All entries are of this type.
     * On a 64-bit platform, this is XX bytes in size,
     * pluss about two pointers (16 bytes) for the list node.
     */
    class AbstractEntry {
    private:
        Generation* _generation;
        EntryListIterator _position;
    public:
        // The key for this entry.
        OID_t key;
        // How popular this item is.
        int frequency;


        AbstractEntry()
            : _generation(nullptr),
              key(-1),
              frequency(1)
        {
        }

        AbstractEntry(OID_t key)
            : _generation(nullptr),
              key(key),
              frequency(1)
        {
        }

        virtual ~AbstractEntry() {} // nothing to do .

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

        RSR_INLINE EntryListIterator& position()
        {
            return this->_position;
        }

        virtual size_t overhead() const
        {
            return sizeof(AbstractEntry);
        }

        /**
         * The total cost of this object.
         */
        virtual size_t weight() const
        {
            // include:
            // the overhead of this object,
            // the approximate size of the linked list entry
            // in our generation (two pointers plus our object entry plus alignment == 32),
            // the size of our map entry,
            // and the size of the shared pointer.

            return this->overhead()
                + (sizeof(void*) * 4)
                + sizeof(std::pair<OID_t, AbstractEntry_p>)
                + sizeof(AbstractEntry_p);

        }

        /**
         * How many values this entry  is tracking.
         */
        virtual size_t value_count() const
        {
            return 0;
        }

        // These functions all need to accept the current pointer.
        // using enable_shared_from_this and shared_from_this() doesn't work
        // when the pointer we're sharing was the bulk array we were bulk-loaded into.

        /**
         * Return a shared pointer to an entry holding the current
         * value and the new value, or raise an exception if that's inconsistent.
         */
        virtual AbstractEntry_p with_later(AbstractEntry_p& current_pointer,
                                           char* buf, size_t len,
                                           const TID_t new_tid) = 0;

        virtual AbstractEntry_p freeze_to_tid(AbstractEntry_p& current_pointer,
                                              const TID_t tid) = 0;
        virtual AbstractEntry_p discarding_tids_before(AbstractEntry_p& current_pointer,
                                                       const TID_t tid) = 0;
    };



    class SingleValueEntry : public AbstractEntry {
    private:
        // If the offset is 0, we own the buffer, otherwise we're sharing.
        char* buf;
        size_t len, offset;
        TID_t _tid;
        bool _frozen;
    public:
        // Some C++ libraries don't support variardics to
        // make_shared(), topping out at 3 arguments. Those
        // are the ones that also tend not to fully support
        // C++ 11 and its delegating constructors, so we use a
        // default argument to make it possible to create
        // these.

        SingleValueEntry(OID_t key, char* buf, size_t len, TID_t tid)
            : AbstractEntry(key),
              buf(new char[len]),
              len(len),
              offset(0),
              _tid(tid),
              _frozen(false)
        {
            memcpy(this->buf, buf, len);
        }

        // Construct from an offset; does not own the pointer.
        SingleValueEntry(OID_t key, char* buf, size_t len, TID_t tid, size_t offset)
            : AbstractEntry(key),
              buf(buf + offset),
              len(len - offset),
              offset(offset),
              _tid(tid),
              _frozen(false)
        {
            assert(offset > 0);
        }


        SingleValueEntry()
            : AbstractEntry(),
              buf(nullptr), len(0),
              offset(0),
              _tid(-1), _frozen(false)
        {
        }

        virtual ~SingleValueEntry()
        {
            if (!this->offset) {
                delete[] this->buf;
            }
        }

        SingleValueEntry& operator=(const SingleValueEntry& other);

        // Copies
        void force_update_during_bulk_load(OID_t key, char* buf, size_t len, TID_t tid)
        {
           this->key = key;
           this->_tid = tid;
           this->len = len;
           assert(this->buf == nullptr);
           this->buf = new char[len];
           memcpy(this->buf, buf, len);
        }

        PyObject* as_object()
        {
            return PyBytes_FromStringAndSize(this->buf, this->len);
        }

        PyObject* first_two_as_object()
        {
            if (this->len >= 2) {
                return PyBytes_FromStringAndSize(this->buf, 2);
            }
            return this->as_object();
        }

        size_t size()
        {
            return this->len;
        }

        char* c_data()
        {
            return this->buf;
        }

        SingleValueEntry* from_offset(size_t offset)
        {
            return new SingleValueEntry(
                this->key,
                this->buf,
                this->len,
                this->_tid,
                offset
            );
        }

        const Pickle_t state() const
        {
            return Pickle_t(this->buf, this->len);
        }

        TID_t tid() const
        {
            return this->_tid;
        }

        bool& frozen()
        {
            return this->_frozen;
        }

        virtual size_t weight() const
        {
            // include: the bytes for the cached object,
            return AbstractEntry::weight() + this->len;
        }

        virtual size_t overhead() const
        {
            return sizeof(SingleValueEntry);
        }

        virtual size_t value_count() const
        {
            return 1;
        }

        bool eq_for_python(const SingleValueEntry* other) const
        {
            return this->_tid == other->_tid
                && this->_frozen == other->_frozen
                && this->len == other->len
                && memcmp(this->buf, other->buf, this->len) == 0;
        }

        // Use a value less than 0 for what would be None in Python.
        virtual bool tid_matches(TID_t tid) const
        {
            return this->_tid == tid || (tid < 0 && this->_frozen);
        }

        virtual AbstractEntry_p with_later(AbstractEntry_p& current_pointer,
                                           char* buf,
                                           size_t len,
                                           const TID_t new_tid);
        virtual AbstractEntry_p freeze_to_tid(AbstractEntry_p& current_pointer, const TID_t tid);
        virtual AbstractEntry_p discarding_tids_before(AbstractEntry_p& current_pointer, const TID_t tid);

    };



    class MultipleValueEntry : public AbstractEntry {
    private:

    public:
        typedef std::list<SingleValueEntry_p> EntryList;
        EntryList p_values;
        MultipleValueEntry(const OID_t key) : AbstractEntry(key) {}
        void push_back(SingleValueEntry_p entry) {this->p_values.push_back(entry);}
        void remove_tids_lte(TID_t tid);
        void remove_tids_lt(TID_t tid);

        virtual size_t value_count() const
        {
            return this->p_values.size();
        }
        virtual size_t weight() const;
        virtual size_t overhead() const
        {
            return sizeof(MultipleValueEntry);
        }

        const SingleValueEntry_p front() const
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

        virtual AbstractEntry_p with_later(AbstractEntry_p& current_pointer,
                                           char* buf, size_t len,
                                           const TID_t new_tid);
        virtual AbstractEntry_p freeze_to_tid(AbstractEntry_p& current_pointer, const TID_t tid);
        virtual AbstractEntry_p discarding_tids_before(AbstractEntry_p& current_pointer, const TID_t tid);
    };

    typedef std::shared_ptr<MultipleValueEntry> MultipleValueEntry_p;

    class Cache;

    class Generation {
    private:
        // The sum of their weights.
        size_t _sum_weights;
    protected:
        EntryList _entries;
    public:
        // The maximum allowed weight
        const size_t max_weight;
        const generation_num generation;


        Generation(size_t limit, generation_num generation)
            :
              _sum_weights(0),
              max_weight(limit),
              generation(generation)
        {

        }

        RSR_INLINE size_t sum_weights() const
        {
            return this->_sum_weights;
        }

        RSR_INLINE bool oversize() const
        {
            return this->_sum_weights > this->max_weight;
        }

        RSR_INLINE bool empty() const
        {
            return this->_entries.empty();
        }

        size_t len() const
        {
            return this->_entries.size();
        }

        RSR_INLINE int will_fit(const AbstractEntry& entry)
        {
            return this->max_weight >= (entry.weight() + this->_sum_weights);
        }

        RSR_INLINE const AbstractEntry* lru() const
        {
            if (this->empty())
                return nullptr;
            return this->_entries.back();
        }

        RSR_INLINE AbstractEntry* lru()
        {
            if (this->empty())
                return nullptr;
            return this->_entries.back();
        }

        EntryList& iter()
        {
            return this->_entries;
        }

        RSR_INLINE void notice_weight_change(AbstractEntry& entry,
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
        RSR_INLINE void move_to_head(AbstractEntry& elt)
        {
            this->_entries.erase(elt.position());
            this->_entries.push_front(&elt);
            elt.position() = this->_entries.begin();
        }

        /**
         * Accept elt, which must be in another node, to be the new
         * head of this ring. This may oversize this node.
         */
        RSR_INLINE void adopt(AbstractEntry& elt);

        /**
         * Remove elt from the list. elt must already be in the list.
         *
         * Constant time.
         */
        RSR_INLINE void remove(AbstractEntry& elt);


        RSR_INLINE void replace_entry(AbstractEntry& incoming,
                                      size_t old_weight,
                                      AbstractEntry* old)
        {
            assert(!incoming.in_cache());
            assert(old->generation() == this);

            this->_sum_weights -= old_weight;
            this->_sum_weights += incoming.weight();
            // When we erase, the iterator goes invalid, so we must put it in
            // first
            if (old != &incoming) {
                incoming.generation() = this;
                incoming.position() = this->_entries.insert(old->position(),
                                                            &incoming);
                this->_entries.erase(old->position());
                old->generation() = nullptr;
            }
        }

        /**
         * Add elt as the most recently used object.  elt must not already be
         * in any list.
         *
         * Constant time.
         */
        virtual void add(AbstractEntry& elt);

        /**
         * Record that the entry has been used.
         * This updates its popularity counter  and makes it the
         * most recently used item in its ring. This is constant time.
         */
        virtual void on_hit(Cache& cache, AbstractEntry& entry);

        // Two rings are equal iff they are the same object.
        virtual bool operator==(const Generation& other) const;

    };

    class Eden : public Generation {
    private:
        OidList rejects;
    public:
        Cache* cache;
        Eden(size_t limit)
            : Generation(limit, GEN_EDEN), rejects(0)
        {
        }
        /**
         * Add elt as the most recently used object.  elt must not already be
         * in any list.
         *
         * Evict items from the cache, if needed, to get all the rings
         * down to size. Return a list of the keys evicted.
         */
        OidList& add_and_evict(AbstractEntry& elt);

    };

    class Protected : public Generation {
    public:
        Protected(size_t limit)
            : Generation(limit, GEN_PROTECTED)
        {
        }
    };

    class Probation : public Generation {
    private:
        OidList no_rejects;
    public:
        Probation(size_t limit)
            : Generation(limit, GEN_PROBATION), no_rejects(0)
        {
        }

        virtual void on_hit(Cache& cache, AbstractEntry& entry);
    };

    typedef std::unordered_map<OID_t, AbstractEntry_p> OidEntryMap;

    /**
     * The cache itself is three generations. See individual methods
     * or the Python code for information about how items move between rings.
     */
    class Cache {
    private:
        OidEntryMap data;
        OidList rejects;
        void update_mru(AbstractEntry& entry);
        void _handle_evicted(OidList& evicted);

    public:
        Eden ring_eden;
        Protected ring_protected;
        Probation ring_probation;

        Cache(size_t eden_limit, size_t protected_limit, size_t probation_limit)
            : ring_eden(eden_limit),
              ring_protected(protected_limit),
              ring_probation(probation_limit)
        {
            this->ring_eden.cache = this;
        }

        virtual ~Cache()
        {
        }

        // We'd much prefer to return const map&, but Cython fails to iterate
        // that for some reason.
        OidEntryMap& getData()
        {
            return this->data;
        }

        RSR_INLINE bool oversize()
        {
            return this->ring_eden.oversize()
                || this->ring_protected.oversize()
                || this->ring_probation.oversize(); // this used to be &&
        }


        RSR_INLINE int will_fit(const AbstractEntry& entry)
        {
            return this->ring_eden.will_fit(entry)
                || this->ring_probation.will_fit(entry)
                || this->ring_protected.will_fit(entry);
        }

        virtual size_t weight() const;

        /**
         * Add a new entry for the given state if one does not already exist.
         * It becomes the first entry in eden. If this causes the cache to be oversized,
         * entries are freed.
         */
        void add_to_eden(OID_t key, char* buf, size_t len, TID_t tid);

        /**
         * Does not rebalance rings. Use only when no evictions are necessary.
         */
        void replace_entry(AbstractEntry_p& new_entry, AbstractEntry_p& prev_entry,
                           size_t prev_weight );

        /**
         * Update an existing entry, replacing its value contents
         * and making it most-recently-used. The key must already
         * be present. Possibly evicts items if the entry grew.
         */
        void store_and_make_MRU(OID_t oid,
                                char* buf, size_t len,
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

        const AbstractEntry_p& get(const OID_t key) const;

        void age_frequencies();
        size_t len();
        void on_hit(OID_t key);
        int add_many(SingleValueEntry_p& shared_ptr_to_array,
                     int entry_count);

    };

} // namespace cache

} // namespace relstorage

#endif
