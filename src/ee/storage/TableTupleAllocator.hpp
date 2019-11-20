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

    struct std_allocator {                    // wrapper of std::allocator
        static void* alloc(size_t);
        static void dealloc(void*);
    };

    /**
     * Holder for a chunk, whether it is self-compacting or not.
     */
    struct ChunkHolder {
        size_t const m_tupleSize;                  // size of a table tuple per allocation
    protected:
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
        void* allocate() noexcept;                 // returns NULL if current chunk is full.
        bool contains(void const*) const;          // query if a table tuple is stored in current chunk
        bool full() const noexcept;
    };

    /**
     * Non-compacting fixed-size chunk, that claims itself full only if it
     * is really full. We need to keep track of all the freed
     * locations for future allocations.
     * Since we are keeping a free list of hole spaces, it is
     * only economic to use when tupleSize is large.
     */
    class NonCompactingChunk final: public ChunkHolder {
        stack<void*> m_freed{};
    public:
        NonCompactingChunk(size_t tupleSize);
        NonCompactingChunk(NonCompactingChunk const&) = delete;
        NonCompactingChunk operator=(NonCompactingChunk const&) = delete;
        NonCompactingChunk(NonCompactingChunk&&);
        ~NonCompactingChunk() = default;
        void* allocate() noexcept;
        void free(void*);
        bool full() const noexcept;
    };

    class NonCompactingChunks final {
        size_t const m_tupleSize;
        forward_list<NonCompactingChunk> m_storage{};
    public:
        NonCompactingChunks(size_t);
        ~NonCompactingChunks() = default;
        NonCompactingChunks(NonCompactingChunk const&) = delete;
        NonCompactingChunks(NonCompactingChunks&&) = default;
        NonCompactingChunks operator=(NonCompactingChunks const&) = delete;
        size_t tupleSize() const noexcept;
        void* allocate() noexcept;
        void free(void*);
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
        // void* allocate() noexcept,
        // bool contains(void const*) const and
        // bool full() const noexcept, have same implementation as base
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
        using iterator_type = list<SelfCompactingChunk>::iterator;
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
        using iterator_type = list<SelfCompactingChunk>::iterator;
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
    protected:
        using list_type = std::list<SelfCompactingChunk>;
        size_t const m_tupleSize;
        list_type m_list{};
        CompactingStorageTrait<dir> m_trait{};
    public:
        SelfCompactingChunks(size_t tupleSize);
        SelfCompactingChunks(SelfCompactingChunks const&) = delete;    // non-copyable, non-assignable, moveable
        SelfCompactingChunks operator=(SelfCompactingChunks const&) = delete;
        SelfCompactingChunks(SelfCompactingChunks&&) = default;
        void* allocate() noexcept;
        void* free(void*);
        void snapshot(bool);
        bool less(void const*, void const*) const; // natural order of two tuples.
    private:
        /**
         * The chunk from whom table tuple need to be moved. This
         * means list tail if compacting from tail, or tail of
         * freed chunks from front of the list.
         */
        typename list_type::iterator compactFrom();
        /**
         * Transaction view of the first chunk. When deleting
         * from head, the begin() will skip chunks that are empty
         * as a result of free(void*) operations.
         */
        typename list_type::iterator chunkBegin();
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

    template<typename Alloc, typename Retainer,
        // Alloc must be either NonCompactingChunks or SelfCompactingChunks
        typename = enable_if<is_same<Alloc, NonCompactingChunks>::value ||
            is_same<Alloc, SelfCompactingChunks<ShrinkDirection::head>>::value ||
            is_same<Alloc, SelfCompactingChunks<ShrinkDirection::tail>>::type>,
        typename = typename enable_if<is_base_of<BaseHistoryRetainTrait, Retainer>::value>::type>
    class TxnPreHook final {
    public:
        enum class ChangeType : char {
            Update, Insertion, Deletion
        };
    private:
        /**
         * Meta-data specifying when and how often to release
         * from map when a ptr has been reverted.
         */
        struct RetainTrait {
            enum class Policy : char {
                never, always, batched
            } const m_policy;
            size_t const m_batchSize;
//            RetainTrait()
        };
        map<void const*, void const*> m_changes{};   // addr in persistent storage under change => addr storing before-change content
        set<void const*> m_copied{};                 // addr in persistent storage that we keep a local copy
        bool m_recording = false;                    // in snapshot process?
        Alloc m_storage;
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

    /**
     * A iterable, self-compacting list of chunks, which deletes
     * from head of the list of chunks.
     */
    struct IterableTableTupleChunks final : public SelfCompactingChunks<ShrinkDirection::head> {
        template<bool Const>                   // RO or RW
        class iterator_type : public std::iterator<forward_iterator_tag,
                typename conditional<Const, void const*, void*>::type> {
            friend class IterableTableTupleChunks;
            using super = iterator<forward_iterator_tag,
                  typename conditional<Const, void const*, void*>::type>;
            using value_type = typename super::value_type;
            using reference = typename super::reference;
            using container_type = typename        // ctor arg type
                conditional<Const, IterableTableTupleChunks const, IterableTableTupleChunks>::type&;

            size_t const m_offset;
            typename conditional<Const, typename add_const<list_type>::type, list_type>::type&
                m_list;
            typename conditional<Const, typename list_type::const_iterator, typename list_type::iterator>::type
                m_iter;
            value_type m_cursor;
            void advance();
        public:
            iterator_type(container_type);
            iterator_type(iterator_type const&) = default;
            iterator_type(iterator_type&&) = default;
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
        using iterator = iterator_type<false>;
        using const_iterator = iterator_type<true>;

        iterator begin();
        iterator end();
        const_iterator cbegin() const;
        const_iterator cend() const;
        const_iterator begin() const;
        const_iterator end() const;

        /**
         * Iterators with callback, applied when
         * accessing iterated value.
         */
        template<bool Const>
        class iterator_cb_type : public iterator_type<Const> {
            using super = iterator_type<Const>;
        public:
            using value_type = typename super::value_type;
            // call-back type
            using cb_type = function<value_type(value_type)> const;
            using container_type = typename super::container_type;
            iterator_cb_type(container_type, cb_type);
            value_type operator*();                // looses noexcept guarantee
        private:
            cb_type m_cb;
        };

        template<typename Alloc, typename Retainer, bool Const>
        class time_traveling_iterator_type : public iterator_cb_type<Const> {
            using super = iterator_cb_type<Const>;
        public:
            using container_type = typename super::container_type;
            using history_type = typename conditional<Const,
                  TxnPreHook<Alloc, Retainer> const,
                  TxnPreHook<Alloc, Retainer>>::type&;
            time_traveling_iterator_type(container_type, history_type);
        };
        template<typename Alloc, typename Retainer>
        using iterator_cb = time_traveling_iterator_type<Alloc, Retainer, false>;
        template<typename Alloc, typename Retainer>
        using const_iterator_cb = time_traveling_iterator_type<Alloc, Retainer, true>;

        template<typename Alloc, typename Retainer> iterator_cb<Alloc, Retainer>
            begin(typename iterator_cb<Alloc, Retainer>::history_type);
        template<typename Alloc, typename Retainer> iterator_cb<Alloc, Retainer>
            end(typename iterator_cb<Alloc, Retainer>::history_type);
        template<typename Alloc, typename Retainer> const_iterator_cb<Alloc, Retainer>
            cbegin(typename const_iterator_cb<Alloc, Retainer>::history_type) const;
        template<typename Alloc, typename Retainer> const_iterator_cb<Alloc, Retainer>
            cend(typename const_iterator_cb<Alloc, Retainer>::history_type) const;
        template<typename Alloc, typename Retainer> const_iterator_cb<Alloc, Retainer>
            begin(typename const_iterator_cb<Alloc, Retainer>::history_type) const;
        template<typename Alloc, typename Retainer> const_iterator_cb<Alloc, Retainer>
            end(typename const_iterator_cb<Alloc, Retainer>::history_type) const;
    };

    typename IterableTableTupleChunks::iterator begin(IterableTableTupleChunks&);
    typename IterableTableTupleChunks::iterator end(IterableTableTupleChunks&);

    typename IterableTableTupleChunks::const_iterator cbegin(IterableTableTupleChunks const&);
    typename IterableTableTupleChunks::const_iterator cend(IterableTableTupleChunks const&);

    typename IterableTableTupleChunks::const_iterator begin(IterableTableTupleChunks const&);
    typename IterableTableTupleChunks::const_iterator end(IterableTableTupleChunks const&);

    template<typename Alloc, typename Retainer, bool Const> typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>
        begin(typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::container_type,
                typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::history_type);
    template<typename Alloc, typename Retainer, bool Const> typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>
        end(typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::container_type,
                typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::history_type);

    template<typename Alloc, typename Retainer> typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>
        cbegin(typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::container_type,
                typename IterableTableTupleChunks::iterator_cb<Alloc, Retainer>::history_type);
    template<typename Alloc, typename Retainer> typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>
        cend(typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::container_type,
                typename IterableTableTupleChunks::iterator_cb<Alloc, Retainer>::history_type);

}
