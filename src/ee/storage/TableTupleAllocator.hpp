/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
#include <atomic>
#include <cassert>
#include <forward_list>
#include <functional>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <vector>

// older GCC compilers incurs some efficiency loss
#if defined(__GNUC__) && (__GNUC__ <= 4)
#define CENTOS7
#endif

namespace voltdb {
    namespace storage {
        using namespace std;
        /**
         * Allocator iterator view type
         */
        enum class iterator_view_type : char {txn, snapshot};
        /**
         * Allocator iterator permission type:
         * ro - iterator cannot modify iterated values
         * rw - iterator can modify iterated values
         */
        enum class iterator_permission_type : char {ro, rw};

        /**
         * never: TxnPreHook::reverted() NEVER deletes map entries.
         *       The clean up is delayed until snapshot completes
         * always: it always delets map entry, in most eager fashion.
         * batched: it deletes in a batch style when fixed
         *       number of entries had been reverted. Kind-of lazy.
         */
        enum class gc_policy: unsigned char {
            never, always, batched =
#ifdef NDEBUG
                numeric_limits<unsigned char>::max()
#else
                16                                     // for eecheck only
#endif
        };

        /**
         * More efficient stack than STL (which was backed by
         * deque), backed by forward_list.
         */
        template<typename T>
        class Stack: private forward_list<T> {
            using super = forward_list<T>;
            using reference = typename super::reference;
            size_t m_size = 0;
        public:
            using iterator = typename super::iterator;
            using const_iterator = typename super::const_iterator;

            explicit Stack() = default;
            ~Stack() = default;
            Stack(Stack const&) = default;
            Stack(Stack&&) = default;
            Stack& operator=(Stack const&) = default;
            template<typename... Args> reference emplace(Args&&...);
            size_t size() const noexcept;
            T const& top() const;
            T& top();
            void pop();
            void clear() noexcept;
            using super::begin; using super::cbegin;
            using super::end; using super::cend;
            using super::empty;
        };

        /**
         * Adaptor allowing for customized memory allocation
         * behavior.
         */
        class StdAllocator : private unique_ptr<char[]> {
            using super = unique_ptr<char[]>;
        public:
            StdAllocator(size_t n) : super(new char[n]) {}
            using super::get;
        };

        /**
         * Holder for a chunk, whether it is self-compacting or not.
         */
        template<typename Alloc = StdAllocator>
        class ChunkHolder {
            size_t const m_id;                         // chunk id
            size_t const m_tupleSize;                  // size of a table tuple per allocation
            Alloc m_resource;
            void*const m_end;                          // indication of chunk capacity
        protected:
            void* m_next;                              // tail of allocation
            ChunkHolder(ChunkHolder const&) = delete;  // non-copyable, non-assignable, non-moveable
            ChunkHolder& operator=(ChunkHolder const&) = delete;
            ChunkHolder(ChunkHolder&&) = delete;
            friend class CompactingChunks;              // for batch free
        public:
            ChunkHolder(size_t id, size_t tupleSize, size_t chunkSize);
            ~ChunkHolder() = default;
            void* allocate() noexcept;                 // returns NULL if this chunk is full.
            bool contains(void const*) const;          // query if a table tuple is stored in current chunk
            bool full() const noexcept;
            bool empty() const noexcept;
            void*const begin() const noexcept;
            void*const end() const noexcept;
            void*const next() const noexcept;
            size_t tupleSize() const noexcept;
            size_t id() const noexcept;
        };

        /**
         * Non-compacting fixed-size chunk, that claims itself full only if it
         * is really full. We need to keep track of all the freed
         * locations for future allocations.
         * Since we are keeping a free list of hole spaces, it is
         * only economic to use when tupleSize is large.
         */
        class EagerNonCompactingChunk final: public ChunkHolder<> {
            using super = ChunkHolder<>;
            Stack<void*> m_freed{};
            EagerNonCompactingChunk(EagerNonCompactingChunk const&) = delete;
            EagerNonCompactingChunk& operator=(EagerNonCompactingChunk const&) = delete;
            EagerNonCompactingChunk(EagerNonCompactingChunk&&) = delete;
        public:
            EagerNonCompactingChunk(size_t, size_t, size_t);
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
        class LazyNonCompactingChunk final: public ChunkHolder<> {
            using super = ChunkHolder<>;
            size_t m_freed = 0;
            LazyNonCompactingChunk(LazyNonCompactingChunk const&) = delete;
            LazyNonCompactingChunk& operator=(LazyNonCompactingChunk const&) = delete;
            LazyNonCompactingChunk(LazyNonCompactingChunk&&) = delete;
        public:
            LazyNonCompactingChunk(size_t, size_t, size_t);
            ~LazyNonCompactingChunk() = default;
            // void* allocate() noexcept; same as ChunkHolder
            // when contains(void const*) returns true, the addr may
            // have already been freed: we just don't know.
            // bool full() const noexcept is the same as ChunkHolder;
            // but gives conservative, ignoring any holes.
            void free(void*);
            // bool empty() const noexcept is identical to ChunkHolder.
        };

        /**
         * Homogeneous-sized singly-linked list of memory holders
         * (i.e. ChunkHolder).
         *
         * Gives O(log(n)) search speed for
         * std::forward_list<ChunkHolder>, search by void* and by chunk id;
         * and limit insertion to tail and erase to front.
         */
        template<typename Chunk,
            typename = typename enable_if<is_base_of<ChunkHolder<>, Chunk>::value>::type>
        class ChunkList : private forward_list<Chunk> {
            using super = forward_list<Chunk>;
            size_t const m_tupleSize;
            size_t const m_chunkSize;                  // how many bytes per chunk
            size_t m_size = 0;
            size_t m_lastChunkId = 0;
            typename super::iterator m_back = super::end();
            map<void const*, typename super::iterator> m_byAddr{};
            map<size_t, typename super::iterator> m_byId{};
            void add(typename super::iterator);
            void remove(typename super::iterator);
        protected:
            size_t& lastChunkId();
            template<typename Pred> void remove_if(Pred p);
            typename super::iterator& last() noexcept;
        public:
            using iterator = typename super::iterator;
            using const_iterator = typename super::const_iterator;
            using reference = typename super::reference;
            using const_reference = typename super::const_reference;
            explicit ChunkList(size_t tupleSize) noexcept;
            // "override" writing behavior
            template<typename... Args> iterator emplace_back(Args&&...);
            void pop_front();
            void pop_back();
            void clear() noexcept;
            using super::begin; using super::end; using super::cbegin; using super::cend;
            using super::empty; using super::front;
            /**
             * Queries
             */
            size_t size() const noexcept;
            size_t tupleSize() const noexcept;
            size_t chunkSize() const noexcept;
            iterator const& last() const noexcept;
            // the O(log(n)) killer
            iterator const* find(void const*) const;
            iterator const* find(size_t) const;
        };

        /**
         * Note that a NonCompactingChunks organizes chunks in a
         * singly-linked-list, meaning that the iteration order
         * is (almost) always different from allocation order.
         *
         * Warning: do not combine a deleter (i.e. a functor that
         * calls free()) with an iterator such as
         * for_each<iterator_type>(...). The reason is that
         * non-compacting chunks uses some kind of book-keeping
         * for the holes that had already been freed, which the
         * iterator is ignorant. To do so, you must use const
         * iterator to collect all available memory address, and
         * call free() on those addresses.
         *
         * For the same reason, iterating (even using const
         * iterator such as fold) over a chunk that had been
         * free()-ed WILL lead to crash, since neither the iterator,
         * nor chunk itself, knows of sequence of safe addresses
         * that can be used. Clients of NonCompactingChunks
         * assume more responsiblity, and iterator on it is much
         * weaker than compacting chunks.
         */
        template<typename Chunk,
            typename = typename enable_if<is_base_of<ChunkHolder<>, Chunk>::value>::type>
        class NonCompactingChunks final : private ChunkList<Chunk> {
            template<typename Chunks, typename Tag, typename E> friend class IterableTableTupleChunks;
            static auto constexpr CHUNK_REMOVAL_THRESHOLD = 64lu;    // iterate list and remove empty chunks when there are so many
            size_t m_emptyChunks = 0;                  // amortize chunk removal
            size_t m_allocs = 0;
            NonCompactingChunks(EagerNonCompactingChunk const&) = delete;
            NonCompactingChunks(NonCompactingChunks&&) = delete;
            NonCompactingChunks& operator=(NonCompactingChunks const&) = delete;
        protected:
            size_t& emptyChunks();
        public:
            using Compact = integral_constant<bool, false>;
            NonCompactingChunks(size_t) noexcept;
            ~NonCompactingChunks() = default;
            size_t size() const noexcept;              // number of allocation requests, <= sum(allocated memory from system), considering modulo and non-full spaces
            size_t chunks() const noexcept;            // number of chunks in the list
            void* allocate();
            void free(void*);
            using list_type = ChunkList<Chunk>;
            using typename list_type::iterator; using typename list_type::const_iterator;
            using list_type::empty; using list_type::clear; using list_type::begin;
            using list_type::tupleSize; using list_type::chunkSize;
        };

        /**
         * self-compacting chunk (with help from CompactingChunks to compact across a list)
         */
        struct CompactingChunk final : public ChunkHolder<> {
            using super = ChunkHolder<>;
            CompactingChunk(size_t id, size_t tupleSize, size_t chunkSize);
            CompactingChunk(CompactingChunk&&) = delete;
            CompactingChunk(CompactingChunk const&) = delete;
            CompactingChunk& operator=(CompactingChunk const&) = delete;
            ~CompactingChunk() = default;
            void free(void* dst, void const* src);  // releases tuple at dst, and move tuple at src to dst. Used for cross-chunk movement
            void* free(void*);                      // release tuple from the last chunk of the list, return the moved tuple
            void* free();                           // release tuple from the last chunk of the list, from the last allocation
            // void* allocate() noexcept, bool contains(void const*) const, bool full() const noexcept,
            // begin() and end(), all have same implementation as ChunkHolder
        };

        template<typename Key, typename Value> struct stdCollections {
            using set = std::set<Key>;
            using map = std::map<Key, Value>;
        };

        /**
         * Shrink-directional-dependent book-keeping
         */
        class CompactingStorageTrait {
            using list_type = ChunkList<CompactingChunk>;
            /**
             * Linearized access order depending on shrink
             * direction, to ensure that chunks are accessed in
             * snapshot view the same order as they appear in txn
             * view when snapshot started.
             */
            list_type* m_storage;
            bool m_frozen = false;
        public:
            explicit CompactingStorageTrait(list_type*) noexcept;
            bool frozen() const noexcept;
            void freeze(); void thaw();
            /**
             * post-action when free() is called, only useful when shrinking
             * in head direction.
             * Called after in-chunk operation completes, since this
             * operates on the level of list iterator, not void*.
             */
            list_type::iterator releasable(typename list_type::iterator);
            /**
             * Action associated with *snapshot RW iterator* when
             * it has done visiting a tuple. If that tuple had
             * been removed in txn view, then erase the
             * containing chunk if possible
             */
            void release(typename list_type::iterator, void const*);
        };

        /**
         * Comparison (less<Number>) of rolling unsigned numbers.
         */
        template<typename T,
            typename = typename enable_if<is_integral<T>::value && ! is_signed<T>::value>::type>
        inline constexpr bool less_rolling(T const& l, T const& r) noexcept {
            return static_cast<typename make_signed<T>::type>(l - r) < 0;
        }
    }
}

/**
 * Needed for maps keyed on iterator, or chunk.
 * Of course, compared items must belong to the same list.
 */
namespace std {
    using namespace voltdb::storage;
    template<> struct less<typename ChunkList<CompactingChunk>::iterator> {
        using value_type = typename ChunkList<CompactingChunk>::iterator;
        inline bool operator()(value_type const& lhs, value_type const& rhs) const noexcept {
            // Rolling number comparison, assuming that neither is end().
            return less_rolling(lhs->id(), rhs->id());
        }
    };
}

namespace voltdb {
    namespace storage {

        /**
         * Communication channel between TxnPreHook and
         * HookedCompactingChunks
         */
        class CompactingChunks;
        class position_type {
            size_t const m_chunkId = 0;
            void const* m_addr = nullptr;
        public:
            position_type() noexcept = default;        // empty initiator
            position_type(CompactingChunks const&, void const*);
            template<typename iterator> position_type(void const*, iterator const&) noexcept;
            position_type(ChunkHolder<> const&) noexcept;
            position_type(position_type const&) noexcept = default;
            position_type(position_type&&) noexcept = default;
            position_type& operator=(position_type const&) noexcept;
            size_t chunkId() const noexcept;
            void const* address() const noexcept;
            bool empty() const noexcept;               // makes it behave like std::optional<position_type>
        };

        /**
         * A linked list of self-compacting chunks:
         * All allocation operations are appended to the last chunk
         * (creates new chunk if necessary); all free operations move
         * the non-empty allocation from the head to freed space.
         */
        class CompactingChunks : private ChunkList<CompactingChunk>, private CompactingStorageTrait {
            template<typename Chunks, typename Tag, typename E> friend struct IterableTableTupleChunks;
            using list_type = ChunkList<CompactingChunk>;
            static size_t s_id;
            static size_t gen_id();

            size_t const m_id;                    // equivalent to "table id", to ensure injection relation to rw iterator
            char const* m_lastFreeFromHead = nullptr;  // arg of previous call to free(from_head, ?)
            pair<list_type::iterator, void const*> m_txnFirstChunk{list_type::end(), nullptr};     // (moving) left boundary for txn
            position_type m_frozenTxnBoundary{};  // (frozen) right boundary for txn
            // the end of allocations when snapshot started: (block id, end ptr)
            CompactingChunks(CompactingChunks const&) = delete;
            CompactingChunks& operator=(CompactingChunks const&) = delete;
            CompactingChunks(CompactingChunks&&) = delete;
            class BatchRemoveAccumulator : private map<list_type::iterator, vector<void*>> {
                CompactingChunks* m_self;
                using map_type = map<list_type::iterator, vector<void*>>;
            protected:
                CompactingChunks& chunks() noexcept;
                // force "removing" the chunk to be compacted from, either by deleting (when not frozen), or
                // adjusting m_txnFirstChunk.
                list_type::iterator pop();
                vector<void*> collect() const;
                using map_type::clear;
            public:
                explicit BatchRemoveAccumulator(CompactingChunks*);
                void insert(list_type::iterator, void*);
                vector<void*> sorted();                         // in compacting order
            };
        protected:
            size_t m_allocs = 0;
            class DelayedRemover : protected BatchRemoveAccumulator {
                using super = BatchRemoveAccumulator;
                size_t m_size = 0;
                bool m_prepared = false;
                set<void*> m_remove{};
                map<void*, void*> m_move{};
            public:
                explicit DelayedRemover(CompactingChunks&);
                // Register a single allocation to be removed later
                size_t add(void*);
                // Memory movements (src to be removed => dst to be copied over) due to batch remove
                DelayedRemover& prepare(bool);
                map<void*, void*> const& movements() const;
                set<void*> const& removed() const;
                // Actuate batch remove
                size_t force(bool);
            } m_batched;
            using list_type::last;
            using list_type::pop_back;
        public:
            using Compact = integral_constant<bool, true>;
            CompactingChunks(size_t tupleSize) noexcept;
            /**
             * Queries
             */
            size_t chunks() const noexcept;            // number of chunks
            size_t size() const noexcept;              // number of allocation requested
            size_t id() const noexcept;
            using list_type::tupleSize; using list_type::chunkSize;
            using list_type::empty; using list_type::begin; using list_type::end;
            using CompactingStorageTrait::frozen;

            // search in txn memory region (i.e. excludes snapshot-related, front portion of list)
            list_type::iterator const* find(void const*) const noexcept;
            list_type::iterator const* find(size_t) const noexcept;
            pair<list_type::iterator, void const*> const& beginTxn() const noexcept;   // (moving) txn left boundary
            position_type const& endTxn_frozen() const noexcept;                       // txn left boundary when freezing
            pair<list_type::iterator, void const*>& beginTxn() noexcept;               // NOTE: this should really be private. Use it with care!!!
            /**
             * Memory operations
             */
            void* allocate();
            // frees a single tuple, and returns the tuple that gets copied
            // over the given address, which is at the tail of
            // first/last chunk; or nullptr when the address is
            // *reasonably* invalid. See documents of
            // CompactingChunksIgnorableFree struct in .cpp for
            // details.
            void* free(void*);
            /**
             * Light weight free() operations from either end,
             * involving no compaction.
             */
            enum class remove_direction : char {from_head, from_tail};
            void free(remove_direction, void const*);
            /**
             * State changes
             */
            void freeze();
            void thaw();
            /**
             * Auxillary others
             */
            using list_type::iterator; using list_type::const_iterator; using list_type::pop_front;
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
        template<gc_policy> struct HistoryRetainTrait;
        template<> struct HistoryRetainTrait<gc_policy::never> : BaseHistoryRetainTrait {
            HistoryRetainTrait(cb_type const&);
            static void remove(void const*) noexcept;
        };

        template<> struct HistoryRetainTrait<gc_policy::always> : BaseHistoryRetainTrait {
            HistoryRetainTrait(cb_type const&);
            void remove(void const*);
        };

        template<> struct HistoryRetainTrait<gc_policy::batched> : BaseHistoryRetainTrait {
            size_t m_size = 0;
            forward_list<void const*> m_batched{};
            HistoryRetainTrait(cb_type const&);
            void remove(void const*);
        };

        template<typename T>                           // concept check
        using is_chunks = integral_constant<bool,
            is_same<typename remove_const<T>::type, NonCompactingChunks<EagerNonCompactingChunk>>::value ||
            is_same<typename remove_const<T>::type, NonCompactingChunks<LazyNonCompactingChunk>>::value ||
            is_base_of<CompactingChunks, typename remove_const<T>::type>::value>;

        template<typename Alloc, typename Trait,
            typename Collections = stdCollections<void const*, void const*>,
            typename = typename enable_if<is_chunks<Alloc>::value && is_base_of<BaseHistoryRetainTrait, Trait>::value>::type>
        class TxnPreHook : private Trait {
            using set = typename Collections::set;
            using map = typename Collections::map;
            map m_changes{};                // addr in persistent storage under change => addr storing before-change content
            set m_copied{};                 // addr in persistent storage that we keep a local copy
            bool m_recording = false;       // in snapshot process?
            bool m_hasDeletes = false;      // observer for iterator::advance()
            Alloc m_storage;
            void* m_last = nullptr;         // last allocation by copy(void const*);
            /**
             * Creates a deep copy of the tuple stored in local
             * storage, and keep track of it.
             */
            void* _copy(void const* src, bool);
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
            void update(void const*);
            void insert(void const*);
            void remove(void const*);
        public:
            enum class ChangeType : char {Update, Insertion, Deletion};
            using is_hook = integral_constant<bool, true>;
            TxnPreHook(size_t);
            TxnPreHook(TxnPreHook const&) = delete;
            TxnPreHook(TxnPreHook&&) = delete;
            TxnPreHook& operator=(TxnPreHook const&) = delete;
            ~TxnPreHook() = default;
            void freeze(); void thaw();
            // NOTE: the deletion event need to happen before
            // calling add(...), unlike insertion/update.
            void add(CompactingChunks const&, ChangeType, void const*);
            void const* operator()(void const*) const;             // revert history at this place!
            void release(void const*);                             // local memory clean-up. Client need to call this upon having done what is needed to record current address in snapshot.
            // auxillary buffer that client must need for tuple deletion/update operation,
            // to hold value before change.
            // Client is responsible to fill the buffer before
            // calling add() API.
            void copy(void const* prev);
            bool const& hasDeletes() const noexcept;
        };

        template<typename Chunks, typename Tag, typename> struct IterableTableTupleChunks;     // fwd decl

        /**
         * Client API that manipulates in high level.
         */
        template<typename Hook, typename E = typename enable_if<Hook::is_hook::value>::type>
        class HookedCompactingChunks : public CompactingChunks, public Hook {
            using CompactingChunks::allocate; using CompactingChunks::free;// hide details
            using CompactingChunks::freeze; using Hook::freeze;
            using Hook::add; using Hook::copy;
        public:
            using hook_type = Hook;                    // for hooked_iterator_type
            using Hook::release;                       // reminds to client: this must be called for GC to happen (instead of delaying it to thaw())
            HookedCompactingChunks(size_t) noexcept;
            template<typename Tag>
            shared_ptr<typename IterableTableTupleChunks<HookedCompactingChunks<Hook, E>, Tag, void>::hooked_iterator>
            freeze();
            void thaw();                               // switch of snapshot process
            void* allocate();                          // NOTE: now that client in control of when to fill in, be cautious not to overflow!!
            void update(void*);                        // NOTE: this must be called prior to any memcpy operations happen
            void const* remove(void*);
            /**
             * Light weight free() operations from either end,
             * involving no compaction. Removing from head when
             * frozen is forbidden.
             *
             * When removing from head direction, the last invocation must be
             * followed by another new invocation as
             * (remove_direction::from_head, nullptr). Forgetting to do so will
             * result in uncleaned removal (i.e. part of remove calls are lost).
             */
            void remove(remove_direction, void const*);
            /**
             * Batch removal using a single call
             * \arg #1: a set of allocation addresses to be
             * removed
             * \arg #2: a call back that client specifies what
             * should happen when a move (from compaction)
             * occurs. Map for removed addr => addr that fills in
             * the removed address
             */
            size_t remove(set<void*> const&, function<void(map<void*, void*>const&)> const&);
            /**
             * Batch removal using separate calls
             */
            size_t remove_add(void*);
            map<void*, void*>const& remove_moves();
            size_t remove_force();
        };

        /**
         * A pseudo-iterable, self-compacting list of chunks, which deletes
         * from head of the list of chunks.
         * It is pseudo-iterable, since the end() are made up with
         * special state, and also std algorithms cannot be applied
         * since we are iterating over void*.
         *
         * We don't need to iterate a non-compacting list of chunks,
         * using them as pure allocator. (i.e. wo. role of iterator).
         *
         * Important note on using writeable iterator on
         * compacting chunks: when deleting an allocation, unless
         * the position that does not trigger compaction, the
         * cursor content gets updated. If the caller increments
         * the iterator, the updated content gets skipped over.
         *
         * \param Tag - callable on iterated value that tells if current iterating value is
         * considered worthy of handling to the client. If not, then
         * it is skipped. It is used to identify whether a tuple is
         * dirty, and depending on the view of iterator, different
         * decisions incur.
         */
        template<typename Chunks, typename Tag,
            typename = typename enable_if<is_class<Tag>::value && is_chunks<Chunks>::value>::type>
        struct IterableTableTupleChunks final {
            using iterator_value_type = void*;         // constness-independent type being iterated over
            static Tag s_tagger;
            IterableTableTupleChunks() = delete;       // only iterator types can be created/used
            template<iterator_permission_type perm, iterator_view_type vtype>
            class iterator_type : public std::iterator<forward_iterator_tag,
                typename conditional<perm == iterator_permission_type::ro, void const*, iterator_value_type>::type> {
                using super = iterator<forward_iterator_tag,
                    typename conditional<perm == iterator_permission_type::ro, void const*, iterator_value_type>::type>;
                ptrdiff_t const m_offset;
                using list_type = typename conditional<perm == iterator_permission_type::ro,
                      typename add_const<typename Chunks::list_type>::type,
                      typename Chunks::list_type>::type;
                list_type& m_storage;
                using list_iterator_type = typename conditional<perm == iterator_permission_type::ro,
                      typename Chunks::list_type::const_iterator, typename Chunks::list_type::iterator>::type;
                list_iterator_type m_iter;
            protected:
                using value_type = typename super::value_type;
                // ctor arg type
                using container_type = typename
                    add_lvalue_reference<typename conditional<perm == iterator_permission_type::ro,
                    Chunks const, Chunks>::type>::type;
                class Constructible {
                    set<size_t> m_inUse{};
                public:
                    void validate(container_type);
                    void remove(container_type);
                } static s_constructible;
                value_type m_cursor;
                void advance();
                container_type storage() const noexcept;
                list_iterator_type const& list_iterator() const noexcept;
                operator position_type() const noexcept;
            public:
                using constness = integral_constant<bool, perm == iterator_permission_type::ro>;
                iterator_type(container_type);
                iterator_type(iterator_type const&) = default;
                iterator_type(iterator_type&&) = default;
                ~iterator_type();
                static iterator_type begin(container_type);
                bool operator==(iterator_type const&) const noexcept;
                inline bool operator!=(iterator_type const& o) const noexcept {
                    return ! operator==(o);
                }
                bool drained() const noexcept;
                iterator_type& operator++();           // prefix
                iterator_type operator++(int);         // postfix
                value_type operator*() noexcept;// ideally this should be const method for const iterator; but that would require class specialization...
            };
            /**
             * Iterators used by transaction
             */
            using iterator = iterator_type<iterator_permission_type::rw, iterator_view_type::txn>;
            using const_iterator = iterator_type<iterator_permission_type::ro, iterator_view_type::txn>;

            static iterator begin(Chunks&);
            static const_iterator cbegin(Chunks const&);
            static const_iterator begin(Chunks const&);

            /**
             * Iterators with callback, applied when
             * accessing iterated value.
             * When iterating with call back, the beginning chunk
             * wouldn't always be the first chunk in the list.
             *
             * NOTE: since we impose no limitations as to what
             * cb_type can do, it is client's responsibility to
             * correct any behavior in the iterator. For example,
             * if call back may return NULL, the client must know
             * about it and skip/throw accordingly.
             */
            template<typename Trans, iterator_permission_type perm>
            class iterator_cb_type : public iterator_type<perm, iterator_view_type::snapshot> {
                using super = iterator_type<perm, iterator_view_type::snapshot>;
                using cb_type = typename conditional<perm == iterator_permission_type::ro,
                      Trans const&, Trans&>::type;
                cb_type m_cb;
            protected:
                using value_type = typename super::value_type;
                using container_type = typename super::container_type;
            public:
                value_type operator*() noexcept;
                iterator_cb_type(container_type, cb_type);
                static iterator_cb_type begin(container_type, cb_type);
            };
            // The following 2 typedefs are for testing purpose only.
            template<typename Trans> using iterator_cb = iterator_cb_type<Trans, iterator_permission_type::rw>;
            template<typename Trans> using const_iterator_cb = iterator_cb_type<Trans, iterator_permission_type::ro>;

            /**
             * This is the snapshot iterator for the client. The
             * iterator_cb_type is an intermediate abstraction that
             * prepare for this type. Of course, you may create
             * slightly different combinations in the same gist of
             * HookedCompactingChunks, and instantiate it to
             * hooked_iterator_type.
             *
             * An optimization on the snapshot boundary obtained
             * from HookedCompactingChunks ensures that
             * dereferencing this iterator type **will never**
             * get NULL, unlike above iterators on snapshots. See
             * TableTupleAllocationTest::testHookedCompactingChunks()
             * for where this is checked.
             */
            template<iterator_permission_type perm>
            class hooked_iterator_type : public iterator_cb_type<typename Chunks::hook_type, perm> {
                using super = iterator_cb_type<typename Chunks::hook_type, perm>;
            public:
                using container_type = typename super::container_type;
                using value_type = typename super::value_type;
                hooked_iterator_type(typename super::container_type);
                bool drained() const noexcept;
                static hooked_iterator_type begin(container_type);
            };
            using hooked_iterator = hooked_iterator_type<iterator_permission_type::rw>;
            using const_hooked_iterator = hooked_iterator_type<iterator_permission_type::ro>;
        };

        struct truth {                                             // simplest Tag that always returns true
            constexpr bool operator()(void*) const noexcept { return true; }
            constexpr bool operator()(void const*) const noexcept { return true; }
        };
        template<unsigned char NthBit, typename = typename enable_if<NthBit < 8>::type>
        struct NthBitChecker {                         // Tag that checks whether the n-th bit of first byte is on
            static constexpr char const MASK = 1 << NthBit;
            bool operator()(void*) const noexcept;
            bool operator()(void const*) const noexcept;
            static void set(void*) noexcept;
            static void reset(void*) noexcept;
        };
        template<ptrdiff_t> struct ByteOf {            // Byte offsetter
            void* operator()(void*) const noexcept;
            void const* operator()(void const*) const noexcept;
        };

        /**
         * Iterators through the container, using const iterator
         * and RO functor/lambda.
         */
        template<typename iterator_type, typename Fun, typename Chunks,
            typename = typename enable_if<is_chunks<Chunks>::value && iterator_type::constness::value && is_const<Chunks>::value>::type>
        inline void fold(Chunks& c, Fun f) {           // fold is only allowed for RO operation on the chunk
            for (auto iter = iterator_type::begin(c); ! iter.drained(); ++iter) {
                f(*iter);
            }
        }

        /**
         * Iterates through the container, using the non-const
         * iterator and RW functor/lambda. The callable type Fun
         * is expected to have return type void, or is ignroed.
         */
        template<typename iterator_type, typename Fun, typename Chunks,
            typename = typename enable_if<is_chunks<Chunks>::value && ! iterator_type::constness::value && ! is_const<Chunks>::value>::type>
        inline void for_each(Chunks& c, Fun&& f) {
            for (auto iter = iterator_type::begin(c);  // we can also equality check with iterator_type::end(c) as stop criterion. Just make sure end() is evaluated once only.
                    ! iter.drained();) {               // We need to make a copy of resource pointer, advance iterator, and apply
                auto* addr = *iter;                    // the functor/lambda, since application could invalidate iterator.
                ++iter;
                f(addr);
            }
        }

        /**
         * Iterates through the container, allowing for early
         * loop exit. The return type of callable Fun is
         * converted to boolean, and loop exists when it evaluates to true.
         */
        template<typename iterator_type, typename Fun, typename Chunks,
            typename = typename enable_if<is_chunks<Chunks>::value && iterator_type::constness::value == is_const<Chunks>::value>::type>
        inline void until(Chunks& c, Fun&& f) {
            for (auto iter = iterator_type::begin(c); ! iter.drained();) {
                auto* addr = *iter;
                ++iter;
                if (f(addr)) {
                    break;
                }
            }
        }

        // versions for multi-ary iterator constructors
        template<typename iterator_type, typename Fun, typename Chunks, typename... Args>
        inline void fold(Chunks& c, Fun&& f, Args&&... args) {
            static_assert(iterator_type::constness::value && is_const<Chunks>::value,
                    "Fold only applies to const iterators and const chunks");
            for (auto iter = iterator_type::begin(c, forward<Args&&>(args)...); ! iter.drained(); ++iter) {
                f(*iter);
            }
        }

        template<typename iterator_type, typename Fun, typename Chunks, typename... Args>
        inline void for_each(Chunks& c, Fun&& f, Args&&... args) {
            static_assert(! iterator_type::constness::value && ! is_const<Chunks>::value,
                    "for_each only applies to non-const iterators and non-const chunks");
            for (auto iter = iterator_type::begin(c, forward<Args&&>(args)...); ! iter.drained(); ++iter) {
                f(*iter);
            }
        }

        template<typename iterator_type, typename Fun, typename Chunks, typename... Args>
        inline void until(Chunks& c, Fun&& f, Args&&... args) {
            static_assert(iterator_type::constness::value == is_const<Chunks>::value,
                    "until(): constness of Chunks and iterator should be the same");
            for (auto iter = iterator_type::begin(c, forward<Args&&>(args)...); ! iter.drained();) {
                auto* addr = *iter;
                ++iter;
                if (f(addr)) {
                    break;
                }
            }
        }
    }

    template<typename F1, typename F2>
    struct Compose {                                   // functor composer
        static F1 const m_f1;
        static F2 const m_f2;
        template<typename T, typename R>               // can spare R in C++14
        inline R operator()(T&& input) const {
            return m_f1(m_f2(input));
        }
    };
}

