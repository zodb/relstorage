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
typedef unsigned int uint32_t;
#define nullptr NULL
#else
#include <cstdint>
#endif
#define NDEBUG 1
#define UNUSED(expr) do { (void)(expr); } while (0)

#include <string>
#include <vector>
#include <stdexcept>
#include <cassert>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

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

#if defined(PYPY_VERSION) && !defined(RS_REF_STRING)
  // Under PyPy, we don't want to keep PyObject* around.
  // They have overhead and prevent the GC from doing certain
  // things. Better to copy the object data to std::string.
  #define RS_COPY_STRING 1
  typedef std::string rs_string;
#else
  // On CPython, it's OK to keep PyObject* around for a long time,
  // as long as they are actually bytes objects.
  // They don't interact with the GC, and we avoid copies.
  typedef PyObject* rs_string;
#endif

// likely/unlikely to help the optimizer. Taken from Cython.
#if defined(__GNUC__)     && (__GNUC__ > 2 || (__GNUC__ == 2 && (__GNUC_MINOR__ > 95)))
  // This catches GCC and clang.
  #define likely(x)   __builtin_expect(!!(x), 1)
  #define unlikely(x) __builtin_expect(!!(x), 0)
#else /* !__GNUC__ or GCC < 2.95 */
  #define likely(x)   (x)
  #define unlikely(x) (x)
#endif /* __GNUC__ */


namespace relstorage {
namespace cache {
    namespace BIT = boost::intrusive;

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

    template <class T>
    struct PythonAllocator : public std::allocator<T> {
        // As a reminder: the `delete` expression first executes
        // the destructors, and then it calls the static ``operator delete``
        // on the type to release the storage. That's what our dispose()
        // mimics.
        PythonAllocator(const PythonAllocator& other)
            : std::allocator<T>()
        {
            UNUSED(other);
        }

        PythonAllocator(const std::allocator<T> other)
            : std::allocator<T>(other)
        {}

        PythonAllocator() : std::allocator<T>() {}

        T* allocate(size_t number_objects, const void* hint=0)
        {
            UNUSED(hint);

            void* p;
            if (likely(number_objects == 1))
                p = PyObject_Malloc(sizeof(T));
            else
                p = PyMem_Malloc(sizeof(T) * number_objects);
            return static_cast<T*>(p);
        }

        void deallocate(T* t, size_t n)
        {
            void* p = t;
            if (likely(n == 1)) {
                PyObject_Free(p);
            }
            else
                PyMem_Free(p);
        }

        // Destroy and deallocate in one step.
        void dispose(T* other)
        {
            this->destroy(other);
            this->deallocate(other, 1);
        }
    };

    template<typename StoredType, typename ImplementationType>
    class _StateOperations {
    public:
        // We don't provide any declarations here for earlier
        // recognition of issues.
        /** Return a StoredType that you now own (e.g., increment ref count) */
        //static const StoredType as_state(PyObject*const & p);
        /** Return a PyObject* that you now own. */
        //static PyObject* as_object(const StoredType& pickle);
        //static inline void incref(StoredType&);
        //static inline void decref(StoredType&);
        //static inline size_t size(const StoredType&);
        //static inline bool eq(const StoredType&, const StoredType&);
    };

    template<>
    class _StateOperations<PyObject*, PyObject*> {
    public:
        // NOTE: Almost all the the ``const`` here are lies.
        static PyObject* owning_state(const PyObject*const & p)
        {
            Py_INCREF(p);
            return const_cast<PyObject*>(p);
        }
        static inline void incref(const PyObject*const & pickle) {
            Py_INCREF(pickle);
        }
        static inline void decref(const PyObject*const& pickle) {
            Py_XDECREF(pickle);
        }
        static inline PyObject* owning_object(const PyObject* const& pickle)
        {
            Py_INCREF(pickle);
            return const_cast<PyObject*>(pickle);
        }
        static inline size_t size(const PyObject*const & pickle)
        {
            assert(pickle);
            ssize_t s = PyBytes_Size(const_cast<PyObject*&>(pickle));
            if (unlikely(s < 0))
                throw std::runtime_error("Size not valid");

            return static_cast<size_t>(s);
        }
        static inline bool eq(const PyObject*const& lhs,
                              const PyObject*const& rhs)
        {
            // Temporarily borrowing the references
            int eq = PyObject_RichCompareBool(
                         const_cast<PyObject*&>(lhs),
                         const_cast<PyObject*&>(rhs),
                         Py_EQ);
            if (unlikely(eq == -1))
                throw std::runtime_error("Comparison failed");
            return eq == 1;
        }
    };

    template<>
    class _StateOperations<std::string, std::string> {
    public:
        // Cython leaves refcounting to us when given parameters and
        // return types of PyObject*. It assumes we return an
        // incremented reference and that we hold a reference when
        // we're constructed until we're destructed.
        static std::string owning_state(PyObject*const & p) {
            // std::cerr << "Making copy of pyobject at " << p << std::endl;
            Py_ssize_t len = -1;
            char* bytes = nullptr;
            // This returns internal storage, which must not be modified.
            // Fortunately, std::strng() makes a copy.
            int r = PyBytes_AsStringAndSize(p, &bytes, &len);
            if (unlikely(r < 0))
                throw std::runtime_error("Failed to get string data");
            return std::string(bytes, len);
        }
        static inline void incref(const std::string& p) {UNUSED(p);}
        static inline void decref(const std::string& p) {UNUSED(p);}
        static inline PyObject* owning_object(const std::string& pickle)
        {
            // string::data returns an internal buffer, but PyBytes_FromStringAndSize
            // makes a copy.
            PyObject* o = PyBytes_FromStringAndSize(pickle.data(),
                                                    pickle.size());

            if (unlikely(!o))
                throw new std::runtime_error("Failed to copy string");
            return o;
        }
        static inline size_t size(const std::string& p) { return p.size(); }
        static inline bool eq(const std::string& lhs, const std::string& rhs) {
            return lhs == rhs;
        }

    };

    struct EntryListTag;
    struct EntryMapTag;

    class Cache;
    class Generation;
    class ICacheEntry;
    class SVCacheEntry;
    struct ProposedCacheEntry;

    // Compact storage of OIDs. Expected to remain small.
    typedef std::vector<OID_t> OidList;
    // A general list of cache entries. Used for generations and
    // multiple entries for the same key. A given ICacheEntry can be
    // in exactly one such list at a time.
    typedef BIT::list_base_hook<BIT::tag<EntryListTag> > EntryListHook;
    typedef BIT::list<
        ICacheEntry,
        // no constant time size saves a tiny amount of space, and lets items auto-unlink
        // if they get deleted. For the list, we don't do that: an entry needs to know
        // its generation, so we don't need auto_unlink. For the map index, though,
        // we do, because they don't know their cache.
        //constant_time_size<false>,
        // any given entry can be in the main generation list,
        // or in the list for a MultipleValue
        BIT::base_hook<EntryListHook>
        > EntryList;
    typedef BIT::list<
        SVCacheEntry,
        //constant_time_size<false>,
        BIT::base_hook<EntryListHook>
        > SVEntryList;

    // A map[oid] = ICacheEntry, as maintained by the cache for lookup.
    // Entries can be in exactly one such map at a time.
    struct OID_is_key;
    typedef BIT::set_base_hook<BIT::tag<EntryMapTag>,
                               BIT::link_mode<BIT::auto_unlink>,
                               BIT::optimize_size<true> > EntryMapHook;
    typedef BIT::set<ICacheEntry,
                BIT::key_of_value<OID_is_key>,
                BIT::base_hook<EntryMapHook>,
                BIT::constant_time_size<false>
                > OidEntryMap;

    /**
     * Base interface for cache entries.
     * A cache entry associates an OID/TID pair with a string value.
     * There are different ways to do that, this just defines the OID.
     *
     * Cache values may not be copied. They are semi reference-counted
     * from Python so when one is removed it can be destroyed. We rely on the GIL
     * to make all of this safe.
     */
    class ICacheEntry
        : public EntryListHook,
          public EntryMapHook {
    private:
        // One compiler produces a layout like this:
        // vtable*              : addr 0 - 7
        // EntryListHook* [2]   : addr 8 - 15, 16 - 23
        // EntryMapHook* [3]    : addr 24 - 31, 32 - 39, 40 - 47 : Setting optimize_size<false> adds another 8 byte quantity
        // Then private, then public (according to the order of entries
        // in each section), and then private and public for each derived class.
        // So if a section ends with a 8 byte object,
        // the next section should begin with any 8 byte objects to keep alignment.
        // In general, the first member should have the same alignment as the last member
        // of the parent.
        BOOST_MOVABLE_BUT_NOT_COPYABLE(ICacheEntry)
    protected:
        Generation* _generation;
        void _remove_from_generation();
        void _remove_from_generation_and_index();
        void _replace_with(ICacheEntry* new_entry);
    private:
        // This is only incremented and decremented
        // when we construct and destruct Python references.
        // When a node becomes unlinked from both indices, *and* has a 0 value here,
        // then it can be freed.
        mutable Py_ssize_t py_ob_refcount;
        RSR_INLINE bool in_index() const {
            const bool is_indexed = EntryMapHook::is_linked();
            assert((is_indexed && in_generation()) || (!is_indexed && !in_generation()));
            return is_indexed;
        }

        RSR_INLINE bool in_generation() const {
            const bool in_list = EntryListHook::is_linked();
            assert(
                   (EntryMapHook::is_linked() && _generation && in_list)
                   || (!EntryMapHook::is_linked() && !_generation && !in_list));
            return in_list;
        }
        friend class Cache;

    public:
        // The key for this entry.
        const OID_t key;
        // How popular this item is.
        uint32_t frequency;


        ICacheEntry()
            : EntryListHook(),
              EntryMapHook(),
              _generation(nullptr),
              py_ob_refcount(0),
              key(-1),
              frequency(1)
        {
        }

        ICacheEntry(OID_t key)
            : EntryListHook(),
              EntryMapHook(),
              _generation(nullptr),
              py_ob_refcount(0),
              key(key),
              frequency(1)
        {
        }

        virtual ~ICacheEntry()
        {
        }

        RSR_INLINE bool in_python () const
        {
            return py_ob_refcount > 0;
        }

        virtual bool can_delete () const
        {
            return !in_index() && !in_generation() && !in_python();
        }

        template <typename T>
        RSR_INLINE const T* Py_use() const
        {
            ++this->py_ob_refcount;
            return dynamic_cast<const T*>(this);
        }

        RSR_INLINE bool Py_release() const
        {
            --this->py_ob_refcount;
            return can_delete();
        }

        RSR_INLINE void remove_from_index()
        {
            EntryMapHook::unlink();
        }

        RSR_INLINE Generation*& generation()
        {
            return this->_generation;
        }

        RSR_INLINE Generation* generation() const
        {
            return this->_generation;
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
        /**
         * Return a matching object, or null.
         * The object could be an object linked into the cache, or it could be a new
         * object.
         */
        virtual SVCacheEntry* matching_tid(TID_t tid) = 0;

        virtual std::vector<TID_t> all_tids() const = 0;

        /**
         * All three of these functions return either this or a new value.
         *
         * Invariants:
         *  - This is in a generation.
         *  - This is in the oid index.
         * On return:
         *  - The generation's weights have been changed as appropriate.
         *  - The returned value is in the generation; if it is not this, this is
         *    no longer in a generation.
         *  - The returned value is in the oid index; if it is not this, this is
         *    no longer in the oid index.
         *
         * A return value of nullptr means that the requirements were not met and this
         * is no longer in any generation or the oid index.
         */
        virtual ICacheEntry* adding_value(const ProposedCacheEntry& proposed) = 0;
        virtual ICacheEntry* freeze_to_tid(const TID_t tid) = 0;
        virtual ICacheEntry* discarding_tids_before(const TID_t tid) = 0;

    };

    // BIT::key_of_value function object, must:
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


    struct ProposedCacheEntry {
    private:
        PyObject* _pickle;
        OID_t _oid;
        TID_t _tid;
        bool _frozen;
        int _frequency;
    public:
        // For bulk allocation in a vector
        ProposedCacheEntry() : _pickle(), _oid(-1), _tid(-1), _frozen(false), _frequency(1) {}

        ProposedCacheEntry(const ProposedCacheEntry& other)
            : _pickle(other._pickle),
              _oid(other._oid),
              _tid(other._tid),
              _frozen(other._frozen),
              _frequency(other._frequency)
        {
            Py_XINCREF(this->_pickle);
        }

        ProposedCacheEntry(OID_t oid, TID_t tid, PyObject* pickle,
                           bool frozen=false, int frequency=1)
            : _pickle(pickle),
              _oid(oid),
              _tid(tid),
              _frozen(frozen),
              _frequency(frequency)
        {
            Py_INCREF(this->_pickle);
        }

        ProposedCacheEntry& operator=(const ProposedCacheEntry& other)
        {
            this->_pickle = other._pickle;
            Py_XINCREF(this->_pickle);
            this->_oid = other._oid;
            this->_tid = other._tid;
            this->_frozen = other._frozen;
            this->_frequency = other._frequency;
            return *this;
        }

        void set_once(OID_t oid, TID_t tid, PyObject* pickle,
                      bool frozen=false, int frequency=1)
        {
            assert(this->_oid == this->_tid && this->_tid == -1);
            assert(oid >= 0 && tid >= 0);
            this->_oid = oid;
            this->_tid = tid;
            this->_pickle = pickle;
            this->_frozen = frozen;
            this->_frequency = frequency;
            Py_INCREF(this->_pickle);
        }

        ~ProposedCacheEntry()
        {
            Py_XDECREF(this->_pickle);
        }

        inline OID_t oid() const { return _oid; }
        inline TID_t tid() const { return _tid; }
        inline bool frozen() const { return _frozen; }
        inline PyObject* borrow_pickle() const { return _pickle; }
        inline int frequency() const { return _frequency; }
    };

    struct TempCacheFiller {
        typedef std::vector<ProposedCacheEntry,
                            PythonAllocator<ProposedCacheEntry> > EntryList;
        typedef EntryList::const_iterator iterator;

        EntryList entries;

        TempCacheFiller() {}
        TempCacheFiller(size_t n) : entries(n) {}
        void set_once_at(size_t i, OID_t oid, TID_t tid, PyObject* pickle,
                         bool frozen, int frequency)
        {
            entries[i].set_once(oid, tid, pickle, frozen, frequency);
        }

        iterator begin()
        {
            return entries.begin();
        }

        iterator end()
        {
            return entries.end();
        }
    };


    class SVCacheEntry : public ICacheEntry,
                         private _StateOperations<Pickle_t, Pickle_t> {
    private:
        // Remember alignment constraints; if the parent changes at all,
        // we might need to adjust here to avoid padding.
        bool _frozen;
        const Pickle_t _pickle;
        const TID_t _tid;
        BOOST_MOVABLE_BUT_NOT_COPYABLE(SVCacheEntry)
    protected:
        void free_state()
        {
            decref(_pickle);
            *const_cast<Pickle_t*>(&_pickle) = Pickle_t();
        }

    public:
        static void* operator new(size_t count);
        static void operator delete(void* ptr);

        SVCacheEntry(BOOST_RV_REF(SVCacheEntry) from)
            : ICacheEntry(from.key),
              _frozen(from._frozen),
              _pickle(boost::move(from._pickle)), // steal the reference
              _tid(from._tid)
        {
        }

        SVCacheEntry(const ProposedCacheEntry& proposed)
            : ICacheEntry(proposed.oid()),
              _frozen(proposed.frozen()),
              _pickle(owning_state(proposed.borrow_pickle())),
              _tid(proposed.tid())
        {
            this->frequency = proposed.frequency();
        }

        SVCacheEntry(const OID_t oid, TID_t tid,
                     const Pickle_t& pickle, bool frozen=false)
            : ICacheEntry(oid),
              _frozen(frozen),
              _pickle(pickle),
              _tid(tid)
        {
            _StateOperations::incref(pickle);
        }

        SVCacheEntry()
            : ICacheEntry(),
              _frozen(false),
              _pickle(),
              _tid(-1)
        {
        }

        virtual ~SVCacheEntry()
        {
            free_state();
        }

        bool operator==(const SVCacheEntry& other)
        {
            return key == other.key
                && _tid == other._tid
                && state_eq(other._pickle);
        }

        virtual std::vector<TID_t> all_tids() const
        {
            // initialize with 1 element, each having the value _tid
            return std::vector<TID_t>(1, _tid);
        }

        PyObject* as_object() const
        {
            return _StateOperations::owning_object(_pickle);
        }

        size_t size() const
        {
            return _StateOperations::size(this->_pickle);
        }

        bool state_eq(const Pickle_t& other) const
        {
            return _StateOperations::eq(this->_pickle, other);
        }

        const Pickle_t& state() const
        {
            _StateOperations::incref(this->_pickle);
            return this->_pickle;
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
            return tid_matches(tid, this->_tid, this->_frozen);
        }
        virtual SVCacheEntry* matching_tid(TID_t tid)
        {
            return tid_matches(tid) ? this : nullptr;
        }
        static inline bool tid_matches(TID_t the_tid, TID_t entry_tid, bool entry_frozen)
        {
            return the_tid == entry_tid || (the_tid < 0 && entry_frozen);
        }

        virtual ICacheEntry* adding_value(const ProposedCacheEntry& proposed);
        virtual ICacheEntry* freeze_to_tid(const TID_t tid);
        virtual ICacheEntry* discarding_tids_before(const TID_t tid);
    };

    class MVCacheEntry : public ICacheEntry {
    private:
        static PythonAllocator<MVCacheEntry> allocator;
        struct TID_is_key;
        typedef set_base_hook<BIT::link_mode<BIT::auto_unlink>, BIT::optimize_size<true> > TidMapHook;
        struct Entry : public TidMapHook, private _StateOperations<Pickle_t, Pickle_t> {
            static PythonAllocator<Entry> allocator;
            static void* operator new(size_t n);
            static void operator delete(void* ptr);

            const Pickle_t state;
            const TID_t tid;
            bool frozen;

            Entry(const ProposedCacheEntry& incoming)
                : state(owning_state(incoming.borrow_pickle())),
                  tid(incoming.tid()),
                  frozen(incoming.frozen())
            {}
            Entry(const SVCacheEntry& incoming)
                : state(boost::move(incoming.state())),
                  tid(incoming.tid()),
                  frozen(incoming.frozen())
            {}

            SVCacheEntry* new_sv(OID_t key, int freq) const
            {
                SVCacheEntry* new_entry = new SVCacheEntry(key,
                                                           tid,
                                                           state,
                                                           frozen);
                new_entry->frequency = freq;
                return new_entry;
            }

            size_t weight() const
            {
                return _StateOperations::size(this->state) + sizeof(Entry);
            }

            ~Entry()
            {
                decref(state);
            }
        private:
            BOOST_MOVABLE_BUT_NOT_COPYABLE(Entry);
        };

        struct TID_is_key {
            typedef TID_t type;
            const type& operator()(const Entry& v) const
            {
                return v.tid;
            }
        };

        struct Disposer {
            void operator()(Entry* e) { delete e; }
        };

        typedef boost::intrusive::set<
            Entry,
            BIT::key_of_value<TID_is_key>,
            BIT::constant_time_size<false>,
            BIT::base_hook<TidMapHook>
        > EntryList;
        EntryList p_values;
        BOOST_MOVABLE_BUT_NOT_COPYABLE(MVCacheEntry)

        SVCacheEntry* to_single()
        {
            const_iterator it = this->p_values.begin();
            const Entry& entry = *it;
            this->p_values.erase(it);
            assert(this->p_values.empty());
            SVCacheEntry* new_entry = entry.new_sv(this->key, this->frequency);
            delete &entry;
            return new_entry;
        }
        void remove_tids_lte(TID_t tid);
        void remove_tids_lt(TID_t tid);
        void insert(const ProposedCacheEntry& entry)
        {
            assert(entry.oid() == this->key);
            Entry* e = new Entry(entry);
            this->p_values.insert(*e);
        }
    public:
        typedef EntryList::const_iterator const_iterator;
        typedef EntryList::iterator iterator;

        static void* operator new(size_t n);
        static void operator delete(void* ptr);

        MVCacheEntry(const SVCacheEntry& prev,
                     const ProposedCacheEntry& incoming)
            : ICacheEntry(prev.key)
        {
            assert(prev.key == incoming.oid());
            assert(prev.tid() != incoming.tid());
            Entry* e = new Entry(prev);
            this->p_values.insert(*e);
            this->frequency = prev.frequency;
            insert(incoming);
        }

        ~MVCacheEntry()
        {
            p_values.clear_and_dispose(Disposer());
        }

        const_iterator begin() const
        {
            return p_values.begin();
        }

        const_iterator end() const
        {
            return p_values.end();
        }

        virtual size_t value_count() const
        {
            return this->p_values.size();
        }

        SVCacheEntry* copy_newest_entry() const
        {
            const Entry& entry = *this->p_values.rbegin();
            assert(this->p_values.rbegin() != this->p_values.rend());
            SVCacheEntry* new_entry = entry.new_sv(this->key, this->frequency);
            assert(new_entry);
            return new_entry;
        }

        TID_t newest_tid() const
        {
            return this->p_values.rbegin()->tid;
        }

        SVCacheEntry* copy_entry_matching_tid(TID_t tid) const
        {
            for(EntryList::const_reverse_iterator it = p_values.crbegin(), end = p_values.crend();
                it != end;
                ++it) {
                if (SVCacheEntry::tid_matches(tid, it->tid, it->frozen)) {
                    return it->new_sv(this->key, this->frequency);
                }
            }
            return nullptr;
        }

        virtual SVCacheEntry* matching_tid(TID_t tid)
        {
            return copy_entry_matching_tid(tid);
        }

        virtual size_t weight() const;
        virtual size_t overhead() const
        {
            return sizeof(MVCacheEntry);
        }

        bool empty() const
        {
            return this->p_values.empty();
        }

        bool degenerate() const
        {
            return this->p_values.size() == 1;
        }

        virtual std::vector<TID_t> all_tids() const
        {
            std::vector<TID_t> result;
            for(const_iterator it = p_values.begin(), _end = p_values.end(); it != _end; ++it) {
                result.push_back(it->tid);
            }
            return result;
        }

        virtual ICacheEntry* adding_value(const ProposedCacheEntry& proposed);
        virtual ICacheEntry* freeze_to_tid(const TID_t tid);
        virtual ICacheEntry* discarding_tids_before(const TID_t tid);
    };

    class Cache;

    class Generation {
        // When we are destructed, we unlink all items
        // from our list. This can prematurely be done with
        // clear().
    private:
        // The sum of their weights.
        size_t _sum_weights;
        size_t _max_weight;
        friend class Cache;
        friend class ICacheEntry;
        friend class SVCacheEntry;

        struct Disposer {
            void operator()(ICacheEntry* e) {e->generation() = nullptr;}
        };

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

        void clear() {
            this->_entries.erase_and_dispose(this->_entries.begin(), this->_entries.end(), Disposer());
            this->_sum_weights = 0;
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

        size_t size() const
        {
            return this->_entries.size();
        }

        RSR_INLINE int will_fit(const ICacheEntry& entry)
        {
            return this->_max_weight >= (entry.weight() + this->_sum_weights);
        }

        RSR_INLINE ICacheEntry* lru()
        {
            if (unlikely(this->empty()))
                return nullptr;
            return &this->_entries.back();
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
            this->_entries.erase(this->_entries.iterator_to(elt));
            this->_entries.push_front(elt);
        }

        /**
         * Accept elt, which must be in another node, to be the new
         * head of this ring. This may oversize this node.
         */
        RSR_INLINE void adopt(ICacheEntry& elt)
        {
            elt.generation()->remove(elt);
            assert(elt.generation() == nullptr);
            this->add(elt);
        }

        /**
         * Remove elt from the list. elt must already be in the list.
         *
         * Constant time.
         */
        RSR_INLINE void remove(ICacheEntry& elt)
        {
            assert(elt.generation() == this);
            _entries.erase(_entries.iterator_to(elt));
            elt.generation() = nullptr;
            this->_sum_weights -= elt.weight();
        }

        /**
         * Add elt as the most recently used object.  elt must not already be
         * in any list.
         *
         * Constant time.
         * Returns whether any items were removed from the index to make
         * room.
         */
        virtual bool add(ICacheEntry& elt, bool allow_rejects=true)
        {
            UNUSED(allow_rejects);
            assert(elt.generation() == nullptr);
            this->_entries.push_front(elt);
            elt.generation() = this;
            this->_sum_weights += elt.weight();
            return false;
        }

        /**
         * Record that the entry has been used.
         * This updates its popularity counter and makes it the
         * most recently used item in its ring. This is constant time.
         *
         * This is also called when the size of the entry is changing
         * due to it being written. It should still make it the MRU
         * but it may need to move things between lists to balance the sizes.
         */
        virtual void on_hit(ICacheEntry& entry);


        // Two rings are equal iff they are the same object.
        virtual bool operator==(const Generation& other) const;

    };

    class Eden : public Generation {
    private:
        friend class Cache;
        Cache& cache;
        Eden(size_t limit, Cache& cache) : Generation(limit, GEN_EDEN), cache(cache) {}
        bool _balance_rings(ICacheEntry*, bool allow_rejects=true);
    public:

        /**
         * Add elt as the most recently used object.  elt must not already be
         * in any list.
         *
         * Evict items from the cache, if needed, to get all the rings
         * down to size. Return a list of the keys evicted.
         */
        virtual bool add(ICacheEntry& elt, bool allow_rejects=true);
        virtual void on_hit(ICacheEntry& entry);
    };

    class Probation;

    class Protected : public Generation {
    private:
        Probation& _probation;
        Protected(size_t limit, Probation& probation)
            : Generation(limit, GEN_PROTECTED),
              _probation(probation)
        {}
        friend class Cache;
    public:
        virtual void on_hit(ICacheEntry& entry);
    };

    class Probation : public Generation {
    private:
        Protected& _protected;
        Probation(size_t limit, Protected& next)
            : Generation(limit, GEN_PROBATION),
              _protected(next)
        {}
        friend class Cache;
    public:
        /** A hit on probation moves to protected. This may evict. */
        virtual void on_hit(ICacheEntry& entry);
    };


    /**
     * The cache itself is three generations. See individual methods
     * or the Python code for information about how items move between rings.
     */
    class Cache {
    private:
        // Recall the order of destructors: "The body of an object's
        // destructor is executed, followed by the destructors of the
        // object's data members (in reverse order of their appearance
        // in the class definition), followed by the destructors of
        // the object's base classes (in reverse order of their
        // appearance in the class definition)."
        OidEntryMap data;
        OidList rejects;

        struct Disposer {
            void operator()(ICacheEntry* ptr)
            {
                // The docs claim the Disposer shouldn't throw, but
                // I see no reason why in the implementation. However,
                // we're using C-style asserts here, which outright terminate
                // the process.
                assert(!ptr->in_generation());
                if (likely(ptr->can_delete())) {
                    delete ptr;
                    return;
                }

                assert(ptr->in_python());
            }
        };
        inline SVCacheEntry* _get_or_peek(const OID_t key, const TID_t tid, const bool peek=false);
        BOOST_MOVABLE_BUT_NOT_COPYABLE(Cache)
    public:
        // types
        typedef OidEntryMap::const_iterator iterator;
        static PythonAllocator<SVCacheEntry> allocator;
        static PythonAllocator<ICacheEntry> deallocator;

        Eden ring_eden;
        Protected ring_protected;
        Probation ring_probation;

        Cache(size_t eden_limit=0, size_t protected_limit=0, size_t probation_limit=0)
            : ring_eden(eden_limit, *this),
              ring_protected(protected_limit, ring_probation),
              ring_probation(probation_limit, ring_protected)
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
            // When we're destroyed, since we're the effective owner of all the
            // data blocks,there generally should be be no outstanding references, but
            // there could be some owned by objects in Python in other threads.
            ring_eden.clear();
            ring_protected.clear();
            ring_probation.clear();

            // Then our data, deleting anything unreferenced in Python.
            this->data.clear_and_dispose(Disposer());
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
        void add_to_eden(const ProposedCacheEntry& proposed);

        /**
         * Update an existing entry, replacing its value contents
         * and making it most-recently-used. The key must already
         * be present. Possibly evicts items if the entry grew.
         */
        void store_and_make_MRU(const ProposedCacheEntry& proposed);

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

        /**
          * Return the entry for the OID, if it exists. May return a
          * multiple. Does not count as a hit.
          */
        ICacheEntry* get(const OID_t key);
        /**
         * Return the single entry for a the key and tid, which is possibly
         * new and should be deleted. Does not count as a hit.
         */
        SVCacheEntry* peek(const OID_t key, const TID_t tid);
        /**
         * Like peek(), but counts as a hit.
         */
        SVCacheEntry* get(const OID_t key, const TID_t tid);

        void age_frequencies();
        size_t size()
        {
            // The lists have constant time size(), the map
            // does not. The assert doesn't get used when we compile with
            // -DNDEBUG
            size_t ring_sizes = ring_eden.size() + ring_protected.size() + ring_probation.size();
            assert(this->data.size() == ring_sizes);
            return ring_sizes;
        }

        void on_hit(OID_t key);
        OidList add_many(TempCacheFiller& filler);

    };

} // namespace cache

} // namespace relstorage

#endif

// Local Variables:
// flycheck-clang-include-path: ("../../../include" "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/include/python2.7")
// End:
