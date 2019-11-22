/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <boost/optional.hpp>
#include <forward_list>
#include <functional>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <stack>

namespace voltdb {
    using namespace std;

    /**
     * Holder for a chunk, whether it is self-compacting or not.
     */
    struct ChunkHolder {
    protected:
        size_t const m_tupleSize;                  // size of a table tuple per allocation
        size_t const m_chunkSize;                  // #bytes per chunk
        unique_ptr<char> m_resource;
        void*const m_begin;                        // the chunk of memory allocated at instance construction
        void*const m_end;                          // m_begin + tupleSize * TableTupleAllocator::ALLOCS_PER_CHUNK
        void* m_next;                              // tail of allocation
        static size_t chunkSize(size_t tupleSize) noexcept;
    public:
        ChunkHolder(size_t tupleSize);
        ChunkHolder(ChunkHolder const&) = delete;       // non-copyable, non-assignable, moveable
        ChunkHolder operator=(ChunkHolder const&) = delete;
        ChunkHolder(ChunkHolder&&);
        ~ChunkHolder() = default;
        void* allocate() noexcept;                 // returns NULL if this chunk is full.
        bool contains(void const*) const;          // query if a table tuple is stored in current chunk
        bool full() const noexcept;
        bool empty() const noexcept;
    };

    /**
     * Non-compacting fixed-size chunk, that claims itself full only if it
     * is really full. We need to keep track of all the freed
     * locations for future allocations.
     * Since we are keeping a free list of hole spaces, it is
     * only economic to use when tupleSize is large.
     */
    class EagerNonCompactingChunk final: public ChunkHolder {
        stack<void*> m_freed{};
    public:
        EagerNonCompactingChunk(size_t);
        EagerNonCompactingChunk(EagerNonCompactingChunk const&) = delete;
        EagerNonCompactingChunk operator=(EagerNonCompactingChunk const&) = delete;
        EagerNonCompactingChunk(EagerNonCompactingChunk&&);
        ~EagerNonCompactingChunk() = default;
        void* allocate() noexcept;
        void free(void*);
        bool full() const noexcept;
        bool empty() const noexcept;
    };

    /**
     * Similar to EagerNonCompactingChunk, but decides itself full
     * ignoring any/many free() operations, unless all of allocations
     * in the chunk had been freed.
     *
     * This way, we are freed from book-keeping all freed
     * locations and achieve faster free() at the cost of lower
     * utilization.
     */
    class LazyNonCompactingChunk final: public ChunkHolder {
        size_t m_freed = 0;
    public:
        LazyNonCompactingChunk(size_t);
        LazyNonCompactingChunk(LazyNonCompactingChunk const&) = delete;
        LazyNonCompactingChunk operator=(LazyNonCompactingChunk const&) = delete;
        LazyNonCompactingChunk(LazyNonCompactingChunk&&) = default;
        ~LazyNonCompactingChunk() = default;
        // void* allocate() noexcept; same as ChunkHolder
        // when contains(void const*) returns true, the addr may
        // have already been freed: we just don't know.
        // bool full() const noexcept is the same as ChunkHolder;
        // but gives conservative, ignoring any holes.
        void free(void*);
        // bool empty() const noexcept is identical to ChunkHolder.
    };

    template<typename Chunk,
        typename = typename enable_if<is_base_of<ChunkHolder, Chunk>::value>::type>
    class NonCompactingChunks final {
        size_t const m_tupleSize;
        forward_list<Chunk> m_storage{};
    public:
        NonCompactingChunks(size_t);
        ~NonCompactingChunks() = default;
        NonCompactingChunks(EagerNonCompactingChunk const&) = delete;
        NonCompactingChunks(NonCompactingChunks&&) = default;
        NonCompactingChunks operator=(NonCompactingChunks const&) = delete;
        size_t tupleSize() const noexcept;
        void* allocate() noexcept;
        void free(void*);
        bool empty() const noexcept;               // mostly for testing purpose
    };

    /**
     * self-compacting chunk (with help from SelfCompactingChunks to compact across a list)
     */
    struct SelfCompactingChunk final: public ChunkHolder {
        SelfCompactingChunk(size_t tupleSize);
        SelfCompactingChunk(SelfCompactingChunk&&) = default;
        SelfCompactingChunk(SelfCompactingChunk const&) = delete;
        SelfCompactingChunk operator=(SelfCompactingChunk const&) = delete;
        ~SelfCompactingChunk() = default;
        void free(void* dst, void const* src);  // releases tuple at dst, and move tuple at src to dst. Used for cross-chunk movement
        void* free(void*);                      // release tuple from the last chunk of the list, return the moved tuple
        void const* begin() const noexcept;     // chunk base ptr
        void const* end() const noexcept;       // next alloc, <= m_end
        // void* allocate() noexcept, bool contains(void const*) const, bool full() const noexcept
        // all have same implementation as base
    };

    /**
     * When compacting on a list of chunks, move the head or the
     * tail of the list to fill in the hole
     */
    enum class ShrinkDirection: char {
        head, tail
    };

    /**
     * Shrink-directional-dependent book-keeping
     */
    template<ShrinkDirection shrink> class CompactingStorageTrait;

    template<> class CompactingStorageTrait<ShrinkDirection::tail> {
        using list_type = std::list<SelfCompactingChunk>;
        using iterator_type = typename list_type::iterator;
    public:
        void snapshot(bool) noexcept;
        /**
         * post-action when free() is called, only useful when shrinking
         * in head direction.
         * Called after in-chunk operation completes, since this
         * operates on the level of list iterator, not void*.
         */
        void released(list_type&, iterator_type);
        /**
         * List iterator when last free() was called. No-op if
         * shrinking in tail, since whether the table is in
         * snapshot process or not does not affect transactional
         * view of the storage.
         */
        boost::optional<iterator_type> lastReleased() const;
    };

    template<> class CompactingStorageTrait<ShrinkDirection::head> {
        using list_type = std::list<SelfCompactingChunk>;
        using iterator_type = typename list_type::iterator;
        bool m_inSnapshot = false;
        list_type* m_list = nullptr;
        boost::optional<iterator_type> m_last{};
    public:
        void snapshot(bool);
        void released(list_type&, iterator_type);
        boost::optional<iterator_type> lastReleased() const;
    };

    /**
     * A linked list of self-compacting chunks:
     * All allocation operations are appended to the last chunk
     * (creates new chunk if necessary); all free operations move
     * the non-empty allocation from the head to freed space.
     */
    template<ShrinkDirection dir>
    class SelfCompactingChunks {
        CompactingStorageTrait<dir> m_trait{};
    public:
        using list_type = std::list<SelfCompactingChunk>;
        size_t const m_tupleSize;
        list_type m_list{};
        /**
         * Transaction view of the first chunk. When deleting
         * from head, the begin() will skip chunks that are empty
         * as a result of free(void*) operations.
         */
        typename list_type::iterator chunkBegin();
        typename list_type::const_iterator chunkBegin() const;

        SelfCompactingChunks(size_t tupleSize);
        SelfCompactingChunks(SelfCompactingChunks const&) = delete;
        SelfCompactingChunks operator=(SelfCompactingChunks const&) = delete;
        SelfCompactingChunks(SelfCompactingChunks&&) = default;
        void* allocate() noexcept;
        void* free(void*);
        void snapshot(bool);
    private:
        /**
         * The chunk from whom table tuple need to be moved. This
         * means list tail if compacting from tail, or tail of
         * freed chunks from front of the list.
         */
        typename list_type::iterator compactFrom();
        typename list_type::const_iterator compactFrom() const;
    };

    /**
     * never: TxnPreHook::reverted() NEVER deletes map entries.
     *       The clean up is delayed until snapshot completes
     * always: it always delets map entry, in most eager fashion.
     * batched: it deletes in a batch style when fixed
     *       number of entries had been reverted. Kind-of lazy.
     */
    enum class RetainPolicy: char {
        never, always, batched
    };

    struct BaseHistoryRetainTrait {
        /**
         * Auxillary for the history retain policy on local storage
         * clean up. Called by HistoryRetainTrait templates on memory
         * location(s) that is deemed reclaimable.
         */
        using cb_type = function<void(void const*)>;
        cb_type const m_cb;
        BaseHistoryRetainTrait(cb_type const& cb);
    };

    /**
     * Helper to clean up memory upon application of history
     * changes.
     */
    template<RetainPolicy> struct HistoryRetainTrait;
    template<> struct HistoryRetainTrait<RetainPolicy::never>: public BaseHistoryRetainTrait {
        HistoryRetainTrait(cb_type const&);
        static void remove(void const*) noexcept;
    };

    template<> struct HistoryRetainTrait<RetainPolicy::always>: public BaseHistoryRetainTrait {
        HistoryRetainTrait(cb_type const&);
        void remove(void const*);
    };

    template<> struct HistoryRetainTrait<RetainPolicy::batched>: public BaseHistoryRetainTrait {
        size_t const m_batchSize;
        size_t m_size = 0;
        forward_list<void const*> m_batched{};
        HistoryRetainTrait(cb_type const&, size_t batchSize);
        void remove(void const*);
    };

    template<typename T>                           // concept check
    using is_chunks = typename enable_if<
            is_same<T, NonCompactingChunks<EagerNonCompactingChunk>>::value ||
            is_same<T, NonCompactingChunks<LazyNonCompactingChunk>>::value ||
            is_same<T, SelfCompactingChunks<ShrinkDirection::head>>::value ||
            is_same<T, SelfCompactingChunks<ShrinkDirection::tail>>::value>::type;

    template<typename Alloc,
        RetainPolicy policy,
        typename Collections,       // set/map typedefs
        typename = is_chunks<Alloc>>
    class TxnPreHook final {
        using set = typename Collections::set;
        using map = typename Collections::map;
        map m_changes{};   // addr in persistent storage under change => addr storing before-change content
        set m_copied{};                 // addr in persistent storage that we keep a local copy
        bool m_recording = false;                    // in snapshot process?
        Alloc m_storage;
        HistoryRetainTrait<policy> m_retainer;
        /**
         * Creates a deep copy of the tuple stored in local
         * storage, and keep track of it.
         */
        void* copy(void const* src);
        /**
         * - Update always changes the value of an existing table
         *   tuple; it tracks the address and the old tuple.
         * - Insertion always inserts to the tail of allocation
         *   chunks; it tracks the address and the new tuple.
         * - Deletion creates a hole at the tuple to be deleted, and
         *   moves the last table tuple to the hole. Two entries are
         *   added: the first tracks the address of the hole,
         *   and tuple to be deleted; the second tracks address of
         *   the tuple that gets moved to the hole by deletion, and
         *   its content.
         */
        void update(void const* src, void const* dst);         // src tuple from temp table written to dst in persistent storage
        void insert(void const* src, void const* dst);         // same
        void remove(void const* src, void const* dst);         // src tuple is deleted, and tuple at dst gets moved to src
    public:
        enum class ChangeType : char {Update, Insertion, Deletion};
        TxnPreHook(size_t);
        TxnPreHook(TxnPreHook const&) = delete;
        TxnPreHook(TxnPreHook&&) = default;
        TxnPreHook operator=(TxnPreHook const&) = default;
        ~TxnPreHook() = default;
        void start() noexcept;                     // start history recording
        void stop();                               // stop history recording
        void add(ChangeType, void const* src, void const* dst);
        void const* reverted(void const*) const;   // revert history at this place!
        void postReverted(void const*);            // local memory clean-up
    };

    template<typename Key, typename Value> struct stdCollections {
        using set = std::set<Key>;
        using map = std::map<Key, Value>;
    };

    /**
     * A iterable, self-compacting list of chunks, which deletes
     * from head of the list of chunks.
     *
     * We don't need to iterate a non-compacting list of chunks,
     * using them as pure allocator. (i.e. wo. role of iterator).
     *
     * \param Tag - callable on iterated value that tells if current iterating value is
     * considered worthy of handling to the client. If not, then
     * it is skipped. It is used to identify whether a tuple is
     * dirty, and depending on the view of iterator, different
     * decisions incur.
     */
    template<typename Tag, typename = typename enable_if<is_class<Tag>::value>::type>
    struct IterableTableTupleChunks final {
        using iterator_value_type = void*;         // constness-independent type being iterated over
        using cont_type = SelfCompactingChunks<ShrinkDirection::head>;
        static Tag s_tagger;
        IterableTableTupleChunks() = delete;       // only iterator types can be created/used
        template<bool Const,                       // RO or RW
            bool TxnView>                          // From which chunk do we start to iterate
        class iterator_type :
            public std::iterator<forward_iterator_tag, typename
                    conditional<Const, add_const<iterator_value_type>::type, iterator_value_type>::type> {

            using super = iterator<forward_iterator_tag,
                  typename conditional<Const, add_const<iterator_value_type>::type, iterator_value_type>::type>;
            using value_type = typename super::value_type;
            using reference = typename super::reference;
            // ctor arg type
            using container_type = typename
                add_lvalue_reference<typename conditional<Const, add_const<cont_type>::type, cont_type>::type>::type;

            ptrdiff_t const m_offset;
            typename conditional<Const, typename add_const<typename cont_type::list_type>::type, typename cont_type::list_type>::type&
                m_list;
            typename conditional<Const, typename cont_type::list_type::const_iterator, typename cont_type::list_type::iterator>::type
                m_iter;
            value_type m_cursor;
            void advance();
        protected:
            iterator_type(container_type);
            iterator_type(iterator_type const&) = default;
            iterator_type(iterator_type&&) = default;
        public:
            static iterator_type begin(container_type);
            static iterator_type end(container_type);
            bool operator==(iterator_type const&) const noexcept;
            inline bool operator!=(iterator_type const& o) const noexcept {
                return ! operator==(o);
            }
            iterator_type& operator++();           // prefix
            iterator_type operator++(int);         // postfix
            reference operator*() noexcept;
        };
        /**
         * Iterators used by transaction
         */
        using iterator = iterator_type<false, true>;
        using const_iterator = iterator_type<true, true>;

        static iterator begin(cont_type&);
        static iterator end(cont_type&);
        static const_iterator cbegin(cont_type const&);
        static const_iterator cend(cont_type const&);
        static const_iterator begin(cont_type const&);
        static const_iterator end(cont_type const&);

        /**
         * Iterators with callback, applied when
         * accessing iterated value.
         * When iterating with call back, the beginning chunk
         * wouldn't always be the first chunk in the list.
         */
        template<bool Const>
        class iterator_cb_type : public iterator_type<Const, false> {
            using super = iterator_type<Const, false>;
        public:
            using value_type = typename super::value_type;
            // call-back type: must be std::function since it's
            // determined at run-time via history_type object
            using cb_type = function<value_type(value_type)> const;
            using container_type = typename super::super2;
            iterator_cb_type(container_type, cb_type);
            value_type operator*();                // looses noexcept guarantee
        private:
            cb_type m_cb;
        };

        template<typename Alloc, RetainPolicy policy, bool Const,
            typename Collections = stdCollections<void const*, void const*>>
        class time_traveling_iterator_type : public iterator_cb_type<Const> {
            using super = iterator_cb_type<Const>;
            using hook_type = TxnPreHook<Alloc, policy, Collections>;
        public:
            using container_type = typename super::container_type;
            using history_type = typename
                add_lvalue_reference<typename conditional<Const, hook_type const, hook_type>::type>::type;
            time_traveling_iterator_type(container_type, history_type);
        };
        template<typename Alloc, RetainPolicy policy>
        using iterator_cb = time_traveling_iterator_type<Alloc, policy, false>;
        template<typename Alloc, RetainPolicy policy>
        using const_iterator_cb = time_traveling_iterator_type<Alloc, policy, true>;

        template<typename Alloc, RetainPolicy policy> iterator_cb<Alloc, policy>
        static begin(cont_type&, typename iterator_cb<Alloc, policy>::history_type);
        template<typename Alloc, RetainPolicy policy> iterator_cb<Alloc, policy>
        static end(cont_type&, typename iterator_cb<Alloc, policy>::history_type);
        template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
        static cbegin(cont_type const&, typename const_iterator_cb<Alloc, policy>::history_type);
        template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
        static cend(cont_type const&, typename const_iterator_cb<Alloc, policy>::history_type);
        template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
        static begin(cont_type const&, typename const_iterator_cb<Alloc, policy>::history_type);
        template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
        static end(cont_type const&, typename const_iterator_cb<Alloc, policy>::history_type);
    };

    template<bool constness>
    using iterator_fun_type = function<void(typename conditional<constness, void const*, void*>::type)>;
    // concept check
    template<typename iterator_type, typename Chunk, bool constness>
    using is_iterator_of = typename enable_if<
        is_base_of<typename Chunk::template iterator_type<constness, false>, iterator_type>::value ||
        is_base_of<typename Chunk::template iterator_type<constness, true>, iterator_type>::value>::type;

    /**
     * Auxillary iterator over all resouces allocated.
     * Also serves as an example for using various types of iterator
     */
    template<typename iterator_type, typename Chunks,       // lambda version
        bool constness = is_const<Chunks>::value,
        typename = is_chunks<Chunks>,
        typename = is_iterator_of<iterator_type, Chunks, constness>>
    void for_each(Chunks&, iterator_fun_type<constness>&);

    template<typename iterator_type, typename Chunks, typename F,      // functor version
        bool constness = is_const<Chunks>::value,
        typename = is_chunks<Chunks>,
        typename = is_iterator_of<iterator_type, Chunks, constness>>
    void for_each(Chunks&, F&);
}

