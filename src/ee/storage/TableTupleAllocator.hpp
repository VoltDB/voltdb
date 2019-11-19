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
#include <functional>
#include <iterator>
#include <list>
#include <map>
#include <memory>

namespace std {
    template<> struct less<void*> {                // for std::map<void*, T>
        bool operator()(void* l, void* r) const noexcept {
            return reinterpret_cast<char*>(l) < reinterpret_cast<char*>(r);
        }
    };
}

namespace voltdb {
    using namespace std;

    enum class ChangeType : char {
        Update, Insertion, Deletion
    };

    struct std_allocator {                    // wrapper of std::allocator
        static void* alloc(size_t);
        static void dealloc(void*);
    };

    template<typename Alloc>
    class ChangeHistory final {
    public:
        /**
         * Meta-data specifying when and how often to release
         * from map when a ptr has been reverted.
         */
        struct RetainTrait {
            /**
             * never: ChangeHistory::reverted() never deletes map * entry
             * always: it always delets map entry
             * batched: it deletes in a batch style when fixed
             * number of entries had been reverted
             */
            enum class Policy : char {
                never, always, batched
            } const m_policy;
            size_t const m_batchSize;
//            RetainTrait()
        };
        class Change {                             // a single change, deep-copy non-inlined tuple
            unique_ptr<char[], decltype(Alloc::dealloc)> m_tuple;
        public:
            Change(void const* c, size_t len);
            Change(Change const&) = delete;
            Change operator=(Change const&) = delete;
            Change(Change&&) = default;
            ~Change() = default;
            void const* get() const;
        };
    private:
        size_t const m_tupleSize;
        bool m_recording = false;
        map<void const*, Change> m_changes;
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
        ChangeHistory(size_t);
        void start();                              // start history recording
        void stop();                               // stop history recording
        void add(ChangeType, void const* src, void const* dst);
        void const* reverted(void const*) const;     // revert history at this place!
    };

    class TableTupleChunk final {                  // self-compacting chunk (with help from TableTupleChunks to compact across a list)
        size_t const m_tupleSize;                  // size of a table tuple per allocation
        size_t const m_chunkSize;                  // #bytes per chunk
        unique_ptr<char> m_resource;
        void*const m_begin;                        // the chunk of memory allocated at instance construction
        void*const m_end;                          // m_begin + tupleSize * TableTupleAllocator::ALLOCS_PER_CHUNK
        void* m_next;                              // tail of allocation
        constexpr static size_t MAX_BYTES_PER_CHUNK =          // each chunk cannot exceed 5MB
            5 * 1024 * 1024;
        static size_t chunkSize(size_t tupleSize) noexcept;
    public:
        TableTupleChunk(size_t tupleSize);
        ~TableTupleChunk() = default;
        TableTupleChunk(TableTupleChunk const&) = delete;       // non-copyable, non-assignable, moveable
        TableTupleChunk operator=(TableTupleChunk const&) = delete;
        TableTupleChunk(TableTupleChunk&&);
        void* allocate() noexcept;              // returns NULL if current chunk is full
        void free(void* dst, void const* src);  // releases tuple at dst, and move tuple at src to dst. Used for cross-chunk movement
        void* free(void*);                      // release tuple from the last chunk of the list, return the moved tuple
        bool contains(void const*) const;       // query if a table tuple is stored in current chunk
        void const* begin() const noexcept;     // chunk base ptr
        void const* end() const noexcept;       // next alloc, <= m_end
        bool full() const noexcept;
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
        using list_type = std::list<TableTupleChunk>;
        using iterator_type = list<TableTupleChunk>::iterator;
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
        using list_type = std::list<TableTupleChunk>;
        using iterator_type = list<TableTupleChunk>::iterator;
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
    class TableTupleChunks {
    protected:
        using list_type = std::list<TableTupleChunk>;
        size_t const m_tupleSize;
        list_type m_list{};
        CompactingStorageTrait<dir> m_trait{};
    public:
        TableTupleChunks(size_t tupleSize);
        TableTupleChunks(TableTupleChunks const&) = delete;    // non-copyable, non-assignable, moveable
        TableTupleChunks operator=(TableTupleChunks const&) = delete;
        TableTupleChunks(TableTupleChunks&&) = default;
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
     * A iterable, self-compacting list of chunks, which deletes
     * from head of the list of chunks.
     */
    struct IterableTableTupleChunks final : public TableTupleChunks<ShrinkDirection::head> {
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

        template<typename Alloc, bool Const>
        class time_traveling_iterator_type : public iterator_cb_type<Const> {
            using super = iterator_cb_type<Const>;
        public:
            using container_type = typename super::container_type;
            using history_type =
                typename conditional<Const, ChangeHistory<Alloc> const, ChangeHistory<Alloc>>::type&;
            time_traveling_iterator_type(container_type, history_type);
        };
        template<typename Alloc>
        using iterator_cb = time_traveling_iterator_type<Alloc, false>;
        template<typename Alloc>
        using const_iterator_cb = time_traveling_iterator_type<Alloc, true>;

        template<typename Alloc> iterator_cb<Alloc> begin(typename iterator_cb<Alloc>::history_type);
        template<typename Alloc> iterator_cb<Alloc> end(typename iterator_cb<Alloc>::history_type);
        template<typename Alloc> const_iterator_cb<Alloc> cbegin(typename const_iterator_cb<Alloc>::history_type) const;
        template<typename Alloc> const_iterator_cb<Alloc> cend(typename const_iterator_cb<Alloc>::history_type) const;
        template<typename Alloc> const_iterator_cb<Alloc> begin(typename const_iterator_cb<Alloc>::history_type) const;
        template<typename Alloc> const_iterator_cb<Alloc> end(typename const_iterator_cb<Alloc>::history_type) const;
    };

    typename IterableTableTupleChunks::iterator begin(IterableTableTupleChunks&);
    typename IterableTableTupleChunks::iterator end(IterableTableTupleChunks&);

    typename IterableTableTupleChunks::const_iterator cbegin(IterableTableTupleChunks const&);
    typename IterableTableTupleChunks::const_iterator cend(IterableTableTupleChunks const&);

    typename IterableTableTupleChunks::const_iterator begin(IterableTableTupleChunks const&);
    typename IterableTableTupleChunks::const_iterator end(IterableTableTupleChunks const&);

    template<typename Alloc, bool Const> typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Const>
        begin(typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Const>::container_type,
                typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Const>::history_type);
    template<typename Alloc, bool Const> typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Const>
        end(typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Const>::container_type,
                typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Const>::history_type);

    template<typename Alloc> typename IterableTableTupleChunks::const_iterator_cb<Alloc>
        cbegin(typename IterableTableTupleChunks::const_iterator_cb<Alloc>::container_type,
                typename IterableTableTupleChunks::iterator_cb<Alloc>::history_type);
    template<typename Alloc> typename IterableTableTupleChunks::const_iterator_cb<Alloc>
        cend(typename IterableTableTupleChunks::const_iterator_cb<Alloc>::container_type,
                typename IterableTableTupleChunks::iterator_cb<Alloc>::history_type);

}
