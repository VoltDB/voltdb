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
    namespace storage {
        using namespace std;

        /**
         * When compacting on a list of chunks, move the head or the
         * tail of the list to fill in the hole
         */
        enum class ShrinkDirection: char {head, tail};
        /**
         * Allocator iterator view type
         */
        enum class iterator_view_type {txn, snapshot};
        /**
         * Allocator iterator permission type:
         * ro - iterator cannot modify iterated values
         * rw - iterator can modify iterated values
         */
        enum class iterator_permission_type {ro, rw};

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
            void*const begin() const noexcept;
            void*const end() const noexcept;
            void*const next() const noexcept;
            size_t tupleSize() const noexcept;
        };

        /**
         * An action associated with deleting a chunk from a
         * linked list. We cannot simply delete the chunk when it
         * becomes empty, since that would invalidate the
         * iterator on chunks, by making it dangling. When that
         * happens, accessing/writing on the iterator would crash
         * and incrementing it would set to end of the list.
         */
        class ThunkHolder {
            using fun_type = function<void(void)>;
            bool m_contains = false;
            fun_type m_thunk{};
        public:
            void of(fun_type const&&) noexcept;
            void run() noexcept;                       // thunk shold not throw
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

        /**
         * Reflection on chunks type
         */
        enum class ChunksType: char {
            EagerNonCompactingChunks,
            LazyNonCompactingChunks,
            HeadCompactingChunks,
            TailCompactingChunks
        };

        template<typename Chunk, typename = typename enable_if<is_base_of<ChunkHolder, Chunk>::value>::type>
        class NonCompactingChunks final : private ThunkHolder {
            template<typename Chunks, typename Tag, typename E1, typename E2> friend class IterableTableTupleChunks;
            using list_type = forward_list<Chunk>;
            size_t const m_tupleSize;
            list_type m_storage{};

            list_type& storage() noexcept;
            list_type const& storage() const noexcept;
            // these 2 are not actually needed for non-compacting chunks
            typename list_type::iterator chunkBegin() noexcept;
            typename list_type::const_iterator chunkBegin() const noexcept;
        public:
            NonCompactingChunks(size_t);
            ~NonCompactingChunks() = default;
            NonCompactingChunks(EagerNonCompactingChunk const&) = delete;
            NonCompactingChunks(NonCompactingChunks&&) = default;
            NonCompactingChunks operator=(NonCompactingChunks const&) = delete;
            size_t tupleSize() const noexcept;
            void* allocate() noexcept;
            void free(void*);
            bool empty() noexcept;               // Non-const because of ThunkHolder. Need to be invoked before for_each<iterator, ...>() call.
        };

        /**
         * self-compacting chunk (with help from CompactingChunks to compact across a list)
         */
        struct CompactingChunk final : public ChunkHolder {
            CompactingChunk(size_t tupleSize);
            CompactingChunk(CompactingChunk&&) = default;
            CompactingChunk(CompactingChunk const&) = delete;
            CompactingChunk operator=(CompactingChunk const&) = delete;
            ~CompactingChunk() = default;
            void free(void* dst, void const* src);  // releases tuple at dst, and move tuple at src to dst. Used for cross-chunk movement
            void* free(void*);                      // release tuple from the last chunk of the list, return the moved tuple
            void* free();                           // release tuple from the last chunk of the list, from the last allocation
            // void* allocate() noexcept, bool contains(void const*) const, bool full() const noexcept,
            // begin() and end(), all have same implementation as ChunkHolder
        };

        /**
         * Shrink-directional-dependent book-keeping
         */
        template<ShrinkDirection shrink> class CompactingStorageTrait;

        template<> class CompactingStorageTrait<ShrinkDirection::tail> : private ThunkHolder {
            using list_type = list<CompactingChunk>;
            using iterator_type = typename list_type::iterator;
        public:
            using ThunkHolder::run;                    // grant access to CompactingChunks
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

        template<> class CompactingStorageTrait<ShrinkDirection::head> : private ThunkHolder {
            using list_type = std::list<CompactingChunk>;
            using iterator_type = typename list_type::iterator;
            bool m_inSnapshot = false;
            list_type* m_list = nullptr;
            boost::optional<iterator_type> m_last{};
        public:
            using ThunkHolder::run;
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
        class CompactingChunks {
            template<typename Chunks, typename Tag, typename E1, typename E2> friend class IterableTableTupleChunks;

            using list_type = std::list<CompactingChunk>;
            CompactingStorageTrait<dir> m_trait{};
            size_t const m_tupleSize;
            list_type m_list{};

            // Below private methods are for iterators only
            /**
             * The chunk from whom table tuple need to be moved. This
             * means list tail if compacting from tail, or tail of
             * freed chunks from front of the list.
             */
            typename list_type::iterator compactFrom();
            typename list_type::const_iterator compactFrom() const;
            /**
             * Transaction view of the first chunk. When deleting
             * from head, the begin() will skip chunks that are empty
             * as a result of free(void*) operations.
             */
            typename list_type::iterator chunkBegin();
            typename list_type::const_iterator chunkBegin() const;
            list_type& storage() noexcept;
            list_type const& storage() const noexcept;
            void snapshot(bool);
            size_t tupleSize() const noexcept;
        public:
            static constexpr ShrinkDirection DIRECTION = dir;
            CompactingChunks(size_t tupleSize);
            CompactingChunks(CompactingChunks const&) = delete;
            CompactingChunks operator=(CompactingChunks const&) = delete;
            CompactingChunks(CompactingChunks&&) = default;
            void* allocate() noexcept;
            // frees tuple and returns the tuple that gets copied
            // over the given address, which is at the tail of
            // first/last chunk.
            // NOTE: due to thunk (for keeping validator valid),
            // when calling free() with iterator (e.g. for_each<iterator>),
            // it is possible that the next call arg won't exist,
            // when current call that decrements m_next is only
            // in effect in the next free() call, and before the
            // iterator advancement. We ignore this kind of error.
            // Return value of NULL indicates nothing gets freed
            // when used with iterator. We still do validate that
            // argument address is in the chunks otherwise.
            void* free(void*);
            bool empty();                              // non-const bc. ThunkHolder of m_trait
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
        template<> struct HistoryRetainTrait<RetainPolicy::never> : public BaseHistoryRetainTrait {
            HistoryRetainTrait(cb_type const&);
            static void remove(void const*) noexcept;
        };

        template<> struct HistoryRetainTrait<RetainPolicy::always> : public BaseHistoryRetainTrait {
            HistoryRetainTrait(cb_type const&);
            void remove(void const*);
        };

        template<> struct HistoryRetainTrait<RetainPolicy::batched> : public BaseHistoryRetainTrait {
            size_t const m_batchSize;
            size_t m_size = 0;
            forward_list<void const*> m_batched{};
            HistoryRetainTrait(cb_type const&, size_t batchSize);
            void remove(void const*);
        };

        template<typename T>                           // concept check
        using is_chunks = typename enable_if<
            is_same<typename remove_const<T>::type, NonCompactingChunks<EagerNonCompactingChunk>>::value ||
            is_same<typename remove_const<T>::type, NonCompactingChunks<LazyNonCompactingChunk>>::value ||
            is_same<typename remove_const<T>::type, CompactingChunks<ShrinkDirection::head>>::value ||
            is_same<typename remove_const<T>::type, CompactingChunks<ShrinkDirection::tail>>::value>::type;

        template<typename Alloc, RetainPolicy policy, typename Collections /* set/map typedefs */, typename = is_chunks<Alloc>>
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
            typename = is_chunks<Chunks>,
            typename = typename enable_if<is_class<Tag>::value>::type>
        struct IterableTableTupleChunks final {
            using iterator_value_type = void*;         // constness-independent type being iterated over
            static Tag s_tagger;
            IterableTableTupleChunks() = delete;       // only iterator types can be created/used
            template<iterator_permission_type perm, iterator_view_type vtype>
            class iterator_type : public std::iterator<forward_iterator_tag,
                typename conditional<perm == iterator_permission_type::ro, void const*, iterator_value_type>::type> {
                using super = iterator<forward_iterator_tag,
                    typename conditional<perm == iterator_permission_type::ro, void const*, iterator_value_type>::type>;
                // we cannot reliably rely on std::iterator for
                // constness of its typedefs
                using value_type = typename super::value_type;
                using reference = typename super::reference;

                ptrdiff_t const m_offset;
                using list_type = typename conditional<perm == iterator_permission_type::ro,
                         typename add_const<typename Chunks::list_type>::type, typename Chunks::list_type>::type;
                list_type& m_list;
                typename conditional<perm == iterator_permission_type::ro,
                         typename Chunks::list_type::const_iterator, typename Chunks::list_type::iterator>::type m_iter;
                value_type m_cursor;
                void advance();
                /**
                 * Different treatment on SelfCompactingChunks<head> on the last chunk:
                 * since within a chunk it is always compacting
                 * in different direction as it grows,
                 */
                static void advance(list_type&, value_type&) noexcept;
            protected:
                // ctor arg type
                using container_type = typename
                    add_lvalue_reference<typename conditional<perm == iterator_permission_type::ro,
                    Chunks const, Chunks>::type>::type;
                iterator_type(container_type);
                iterator_type(iterator_type const&) = default;
                iterator_type(iterator_type&&) = default;
            public:
                using constness = integral_constant<bool, perm == iterator_permission_type::ro>;
                static iterator_type begin(container_type);
                static iterator_type end(container_type);
                bool operator==(iterator_type const&) const noexcept;
                inline bool operator!=(iterator_type const& o) const noexcept {
                    return ! operator==(o);
                }
                iterator_type& operator++();           // prefix
                iterator_type operator++(int);         // postfix
                reference operator*() noexcept;        // ideally this should be const method for const iterator; but that would require class specialization...
            };
            /**
             * Iterators used by transaction
             */
            using iterator = iterator_type<iterator_permission_type::rw, iterator_view_type::txn>;
            using const_iterator = iterator_type<iterator_permission_type::ro, iterator_view_type::txn>;

            static iterator begin(Chunks&);
            static iterator end(Chunks&);
            static const_iterator cbegin(Chunks const&);
            static const_iterator cend(Chunks const&);
            static const_iterator begin(Chunks const&);
            static const_iterator end(Chunks const&);

            /**
             * Iterators with callback, applied when
             * accessing iterated value.
             * When iterating with call back, the beginning chunk
             * wouldn't always be the first chunk in the list.
             */
            template<iterator_permission_type perm>
            class iterator_cb_type : public iterator_type<perm, iterator_view_type::snapshot> {
                using super = iterator_type<perm, iterator_view_type::snapshot>;
            protected:
                using value_type = typename super::value_type;
                // call-back type: must be std::function since it's
                // determined at run-time via history_type object
                using cb_type = function<value_type(value_type)> const;
                using container_type = typename super::container_type;
                iterator_cb_type(container_type, cb_type);
            private:
                cb_type m_cb;
            public:
                value_type operator*();                // looses noexcept guarantee
                static iterator_cb_type begin(container_type, cb_type);
                static iterator_cb_type end(container_type, cb_type);
            };

            template<typename Alloc, RetainPolicy policy, iterator_permission_type perm,
                typename Collections = stdCollections<void const*, void const*>>
            class time_traveling_iterator_type : public iterator_cb_type<perm> {
                using super = iterator_cb_type<perm>;
                using hook_type = TxnPreHook<Alloc, policy, Collections>;
                using container_type = typename super::container_type;
                using history_type = typename add_lvalue_reference<
                    typename conditional<perm == iterator_permission_type::ro, hook_type const, hook_type>::type>::type;
                time_traveling_iterator_type(container_type, history_type);
            public:
                static time_traveling_iterator_type begin(container_type, history_type);
                static time_traveling_iterator_type end(container_type, history_type);
            };
            template<typename Alloc, RetainPolicy policy>
            using iterator_cb = time_traveling_iterator_type<Alloc, policy, iterator_permission_type::rw>;
            template<typename Alloc, RetainPolicy policy>
            using const_iterator_cb = time_traveling_iterator_type<Alloc, policy, iterator_permission_type::ro>;

            template<typename Alloc, RetainPolicy policy> iterator_cb<Alloc, policy>
            static begin(Chunks&, typename iterator_cb<Alloc, policy>::history_type);
            template<typename Alloc, RetainPolicy policy> iterator_cb<Alloc, policy>
            static end(Chunks&, typename iterator_cb<Alloc, policy>::history_type);
            template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
            static cbegin(Chunks const&, typename const_iterator_cb<Alloc, policy>::history_type);
            template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
            static cend(Chunks const&, typename const_iterator_cb<Alloc, policy>::history_type);
            template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
            static begin(Chunks const&, typename const_iterator_cb<Alloc, policy>::history_type);
            template<typename Alloc, RetainPolicy policy> const_iterator_cb<Alloc, policy>
            static end(Chunks const&, typename const_iterator_cb<Alloc, policy>::history_type);
        };

        struct truth;                                              // simplest Tag that always returns true
        template<unsigned char NthBit> struct NthBitChecker;       // Tag that checks whether the n-th bit of first byte is on

        /**
         * Auxillary iterator over all resouces allocated.
         * Also serves as an example for using various types of iterator
         *
         * We need both types for Chunks and iterator_type, since we
         * use const_iterator on const Chunk and iterator on
         * non-const Chunk.
         *
         * Note also that we need to call "pure"-query function
         * empty() to release any thunk when last free()
         * call needs to remove a chunk
         */
        template<bool constness>
        using iterator_fun_type = function<void(typename conditional<constness, void const*, void*>::type)>;

        template<typename iterator_type, typename Chunks,       // lambda version
            bool constness = is_const<Chunks>::value,
            typename = is_chunks<Chunks>,
            typename = typename enable_if<iterator_type::constness::value == constness>::type>
        void for_each(Chunks& c, iterator_fun_type<constness>&& f) {
            // Important: using side-effect that releases any thunks
            const_cast<typename remove_const<Chunks>::type&>(c).empty();
            // Caution: since c is changeable, end(c) would get
            // evaluated on each iteration if we put it inside the
            // for-loop.
            auto const end = iterator_type::end(c);
            auto iter = iterator_type::begin(c);
            while(iter != end) {
                f(*iter);
                ++iter;
            }
        }

        template<typename iterator_type, typename Fun, typename Chunks,      // functor version
            bool constness = is_const<Chunks>::value,
            typename = is_chunks<Chunks>,
            typename = typename enable_if<iterator_type::constness::value == constness>::type>
        void for_each(Chunks& c, Fun&& f) {
            // Important: using side-effect that releases any thunks
            const_cast<typename remove_const<Chunks>::type&>(c).empty();
            // NOTE: if Fun is overloaded with both "const void*" and "void*" argument
            // types, then it wouldn't always resolve correctly on
            // neither GCC-7.4 nor CLANG-6.0
            auto const end = iterator_type::end(c);
            auto iter = iterator_type::begin(c);
            while (iter != end) {
                f(*iter);
                ++iter;
            }
        }
    }
}

