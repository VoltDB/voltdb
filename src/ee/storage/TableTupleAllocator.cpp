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

#include "TableTupleAllocator.hpp"
#include "common/debuglog.h"
#include <array>

using namespace voltdb;
using namespace voltdb::storage;

static char buf[64];

template<typename T>
template<typename... Args>
inline typename Stack<T>::reference Stack<T>::emplace(Args&&... args) {
    super::emplace_front(forward<Args>(args)...);
    ++m_size;
    return super::front();
}

template<typename T> inline size_t Stack<T>::size() const noexcept {
    return m_size;
}

template<typename T> inline T const& Stack<T>::top() const {
    return super::front();
}

template<typename T> inline T& Stack<T>::top() {
    return super::front();
}

template<typename T> inline void Stack<T>::pop() {
    super::pop_front();
    --m_size;
}

template<typename T> inline void Stack<T>::clear() noexcept {
    super::clear();
    m_size = 0;
}

inline size_t ChunkHolder::chunkSize(size_t tupleSize) noexcept {
    // preferred list of chunk sizes ranging from 4KB to 16MB.
    // For debug build, it starts with 512-byte block, which the
    // test logic assumes.
#ifdef NDEBUG
    static array<size_t, 13> const preferred{{
#else
    static array<size_t, 14> const preferred{{0x200,
#endif
        4 * 0x400/*4KB*/,8 * 0x400,    0x10 * 0x400,  0x20 * 0x400,
        0x40 * 0x400,    0x80 * 0x400, 0x100 * 0x400, 0x200 * 0x400,
        0x100000/*1MB*/, 2 * 0x100000, 4 * 0x100000,  8 * 0x100000,
        0x10 * 0x100000
    }};
    static LRU<512, size_t, size_t> lru{};
    auto const* maybe_value = lru.get(tupleSize);
    if (maybe_value != nullptr) {
        return *maybe_value;
    } else {
        // we always pick smallest preferred chunk size to calculate
        // how many tuples a chunk fits. The picked chunk should fit
        // for > 4 allocations
        auto const value = *find_if(preferred.cbegin(), preferred.cend(),
                [tupleSize](size_t s) { return tupleSize * 4 <= s; })
            / tupleSize * tupleSize;
        lru.add(tupleSize, value);
        return value;
    }
}

// We remove member initialization from init list to save from
// storing chunk size into object
inline ChunkHolder::ChunkHolder(size_t tupleSize): m_tupleSize(tupleSize) {
    vassert(tupleSize <= 4 * 0x100000);    // individual tuple cannot exceeding 4MB
    auto const size = chunkSize(m_tupleSize);
    m_resource.reset(new char[size]);
    m_next = m_resource.get();
    vassert(m_next != nullptr);
    const_cast<void*&>(m_end) = reinterpret_cast<char*>(m_next) + size;
}

inline void* ChunkHolder::allocate() noexcept {
    if (next() >= end()) {                 // chunk is full
        return nullptr;
    } else {
        void* res = next();
        reinterpret_cast<char*&>(m_next) += m_tupleSize;
        return res;
    }
}

inline bool ChunkHolder::contains(void const* addr) const {
    // check alignment
    vassert(addr < begin() || addr >= end() || 0 ==
            (reinterpret_cast<char const*>(addr) - reinterpret_cast<char*const>(begin())) % m_tupleSize);
    return addr >= begin() && addr < next();
}

inline bool ChunkHolder::full() const noexcept {
    return next() == end();
}

inline bool ChunkHolder::empty() const noexcept {
    return next() == begin();
}

inline void* const ChunkHolder::begin() const noexcept {
    return reinterpret_cast<void*>(m_resource.get());
}

inline void* const ChunkHolder::end() const noexcept {
    return m_end;
}

inline void* const ChunkHolder::next() const noexcept {
    return m_next;
}

inline size_t ChunkHolder::tupleSize() const noexcept {
    return m_tupleSize;
}

inline EagerNonCompactingChunk::EagerNonCompactingChunk(size_t s): ChunkHolder(s) {}

inline void* EagerNonCompactingChunk::allocate() noexcept {
    if (m_freed.empty()) {
        return ChunkHolder::allocate();
    } else {                               // allocate from free list first, in LIFO order
        auto* r = m_freed.top();
        vassert(r < next() && r >= begin());
        m_freed.pop();
        return r;
    }
}

inline void EagerNonCompactingChunk::free(void* src) {
    if (reinterpret_cast<char*>(src) + tupleSize() == next()) {     // last element: decrement boundary ptr
        m_next = src;
    } else {                               // hole in the middle: keep track of it
        m_freed.emplace(src);
    }
    if (empty()) {
        m_next = begin();
        m_freed = decltype(m_freed){};
    }
}

inline bool EagerNonCompactingChunk::empty() const noexcept {
    return ChunkHolder::empty() || tupleSize() * m_freed.size() ==
        reinterpret_cast<char const*>(next()) - reinterpret_cast<char const*>(begin());
}

inline bool EagerNonCompactingChunk::full() const noexcept {
    return ChunkHolder::full() && m_freed.empty();
}

inline LazyNonCompactingChunk::LazyNonCompactingChunk(size_t tupleSize) : ChunkHolder(tupleSize) {}

inline void LazyNonCompactingChunk::free(void* src) {
    vassert(src >= begin() && src < next());
    if (reinterpret_cast<char*>(src) + tupleSize() == next()) {     // last element: decrement boundary ptr
        m_next = src;
    } else {
        ++m_freed;
    }
    if (m_freed * tupleSize() ==
            reinterpret_cast<char const*>(next()) - reinterpret_cast<char const*>(begin())) {
        // everything had been freed, the chunk becomes empty
        m_next = begin();
        m_freed = 0;
    }
}

template<typename Chunk, typename E>
inline void ChunkList<Chunk, E>::add(iterator const&& h) {
    m_map.emplace(h->begin(), h);
}

template<typename Chunk, typename E>
inline typename ChunkList<Chunk, E>::iterator const* ChunkList<Chunk, E>::find(void const* k) const {
    if (! m_map.empty()) {
        auto const& iter = prev(m_map.upper_bound(k));                    // find last entry whose begin() <= k
        return iter != m_map.cend() && iter->second->contains(k) ? &iter->second : nullptr;
    } else {
        return nullptr;
    }
}

template<typename Chunk, typename E>
template<typename... Args>
inline void ChunkList<Chunk, E>::emplace_front(Args&&... args) {
    super::emplace_front(forward<Args>(args)...);
    add(super::begin());
}

template<typename Chunk, typename E>
template<typename... Args>
inline void ChunkList<Chunk, E>::emplace_back(Args&&... args) {
    super::emplace_back(forward<Args>(args)...);
    add(prev(super::end()));
}

template<typename Chunk, typename E>
inline void ChunkList<Chunk, E>::splice(const_iterator pos, ChunkList& other, iterator it) noexcept {
    m_map.emplace(it->begin(), it);
    other.m_map.erase(it->begin());
    super::splice(
#ifdef CENTOS7
            next(begin(), distance(cbegin(), pos)),
#else
            pos,
#endif
            other, it);
}

template<typename Chunk, typename E>
inline typename ChunkList<Chunk, E>::iterator
ChunkList<Chunk, E>::erase(typename ChunkList<Chunk, E>::iterator it) {
    m_map.erase(it->begin());
    return super::erase(it);
}

template<typename Chunk, typename E>
inline typename ChunkList<Chunk, E>::iterator ChunkList<Chunk, E>::erase(
        typename ChunkList<Chunk, E>::iterator first,
        typename ChunkList<Chunk, E>::iterator last) {
    for_each(first, last, [this](Chunk const& c) { m_map.erase(c.begin()); });
    return super::erase(first, last);
}

template<typename Chunk, typename E>
inline void ChunkList<Chunk, E>::clear() noexcept {
    m_map.clear();
    super::clear();
}

// queue: insert to tail; get from head; stack: insert to head,
// get from head. We could use if-constexpr to replace this
// implementation hassel.
template<shrink_direction> struct InsertionPos;
template<> struct InsertionPos<shrink_direction::head> {
    using Chunks = ChunkList<CompactingChunk>;
    inline typename Chunks::iterator operator()(Chunks& c) const noexcept {
        return c.end();
    }
};
template<> struct InsertionPos<shrink_direction::tail> {
    using Chunks = ChunkList<CompactingChunk>;
    inline typename Chunks::iterator operator()(Chunks& c) const noexcept {
        return c.begin();
    }
};

inline ExtendedIterator::ExtendedIterator(bool fromHead,
        typename ExtendedIterator::iterator_type const&& iter) noexcept :
m_shrinkFromHead(fromHead), m_iter(iter) {}

inline bool ExtendedIterator::shrinkFromHead() const noexcept {
    return m_shrinkFromHead;
}

inline void const* ExtendedIterator::operator()() const noexcept {
    return m_iter();
}

template<shrink_direction dir> inline void
CompactingStorageTrait<dir>::LinearizedChunks::emplace(
        typename CompactingStorageTrait<dir>::list_type& o,
        typename CompactingStorageTrait<dir>::list_type::iterator const& it) noexcept {
    constexpr static InsertionPos<dir> const insertion{};
    list_type::splice(insertion(*this), o, it);
}

template<shrink_direction dir> inline typename CompactingStorageTrait<dir>::LinearizedChunks::reference
CompactingStorageTrait<dir>::LinearizedChunks::top() {
    return list_type::front();
}

template<shrink_direction dir> inline typename CompactingStorageTrait<dir>::LinearizedChunks::const_reference
CompactingStorageTrait<dir>::LinearizedChunks::top() const {
    return list_type::front();
}

template<shrink_direction dir> inline void CompactingStorageTrait<dir>::LinearizedChunks::pop() {
    list_type::erase(list_type::begin());
}

template<shrink_direction dir> inline
CompactingStorageTrait<dir>::LinearizedChunks::ConstExtendedIteratorHelper::ConstExtendedIteratorHelper(
       typename CompactingStorageTrait<dir>::LinearizedChunks const& c) noexcept : m_cont(c) {}

template<shrink_direction dir> inline void
CompactingStorageTrait<dir>::LinearizedChunks::ConstExtendedIteratorHelper::reset() const noexcept {
    m_val = nullptr;
}

template<shrink_direction dir> inline void const*
CompactingStorageTrait<dir>::LinearizedChunks::ConstExtendedIteratorHelper::operator()() const {
    if (m_val == nullptr) {
        m_iter = m_cont.cbegin();
        return m_val = m_iter->begin();
    } else if (m_iter == m_cont.cend()) {
        return nullptr;
    } else if (m_val == nullptr) {
        return m_val = m_iter->begin();
    } else {
        reinterpret_cast<char const*&>(m_val) += m_iter->tupleSize();
        return m_val >= m_iter->end() ?    // ugly cross-border adjustment
            (++m_iter == m_cont.cend() ? nullptr : (m_val = m_iter->begin())) :
            m_val;
    }
}

template<shrink_direction dir> inline void const*
CompactingStorageTrait<dir>::LinearizedChunks::extendedIteratorHelper() {
    if (! empty()) {
        auto* r = top().allocate();       // NOTE: we are not really allocating here; what we are
        if (r == nullptr) {                    // doing is iterating through the chunk holding tuple values
            pop();                        // that are invisible to txn, but visible to snapshot.
            return extendedIteratorHelper();
        } else {
            return r;
        }
    } else {
        return nullptr;
    }
}

// Helper of destructuring iterator
template<shrink_direction dir> inline typename CompactingStorageTrait<dir>::LinearizedChunks::iterator_type
CompactingStorageTrait<dir>::LinearizedChunks::iterator() noexcept {
    return [this]() { return extendedIteratorHelper(); };
}

template<shrink_direction dir> inline typename CompactingStorageTrait<dir>::LinearizedChunks::iterator_type
CompactingStorageTrait<dir>::LinearizedChunks::iterator() const noexcept {
    m_iterHelper.reset();
    return [this]() { return this->m_iterHelper(); };
}

template<typename C, typename E>
inline NonCompactingChunks<C, E>::NonCompactingChunks(size_t tupleSize) noexcept : m_tupleSize(tupleSize) {}

template<typename C, typename E>
inline size_t NonCompactingChunks<C, E>::tupleSize() const noexcept {
    return m_tupleSize;
}

template<typename C, typename E>
inline void* NonCompactingChunks<C, E>::allocate() {
    auto iter = find_if(list_type::begin(), list_type::end(),
            [](C const& c) { return ! c.full(); });
    void* r;
    if (iter == list_type::cend()) {        // all chunks are full
        list_type::emplace_front(m_tupleSize);
        r = list_type::front().allocate();
    } else {
        r = iter->allocate();
    }
    vassert(r != nullptr);
    ++m_allocs;
    return r;
}

template<typename C, typename E> inline size_t NonCompactingChunks<C, E>::size() const noexcept {
    return m_allocs;
}

template<typename C, typename E> inline void NonCompactingChunks<C, E>::free(void* src) {
    if (! tryFree(src)) {
        snprintf(buf, sizeof buf, "NonCompactingChunks cannot free address %p", src);
        buf[sizeof buf - 1] = 0;
        throw runtime_error(buf);
    }
}

template<typename C, typename E> inline bool NonCompactingChunks<C, E>::tryFree(void* src) {
    auto* p = list_type::find(src);
    if (p != nullptr) {
        vassert(*p != list_type::end());
        (*p)->free(src);
        if ((*p)->empty()) {
            list_type::erase(*p);              // immediate chunk removal
        }
        --m_allocs;
    }
    return p != nullptr;
}

inline CompactingChunk::CompactingChunk(size_t s) : ChunkHolder(s) {}

inline void CompactingChunk::free(void* dst, void const* src) {     // cross-chunk free(): update only on dst chunk
    vassert(contains(dst));
    vassert(! contains(src));
    memcpy(dst, src, tupleSize());
}

inline void* CompactingChunk::free(void* dst) {                     // within-chunk free()
    vassert(contains(dst));
    if (reinterpret_cast<char*>(dst) + tupleSize() == m_next) {     // last allocation on the chunk
        return free();
    } else {                               // free in the middle
        memcpy(dst, free(), tupleSize());
        return m_next;
    }
}

inline void* CompactingChunk::free() {                               // within-chunk free() of last allocated
    vassert(m_next > begin());
    return reinterpret_cast<char*&>(m_next) -= tupleSize();
}

template<shrink_direction dir>
inline void CompactingStorageTrait<dir>::associate(list_type* s) noexcept {
    vassert(s != nullptr);
    m_storage = s;
}

template<shrink_direction dir> inline void CompactingStorageTrait<dir>::freeze() noexcept {
    vassert(m_storage != nullptr);
    m_frozen = true;
}

template<shrink_direction dir> inline void CompactingStorageTrait<dir>::thaw() noexcept {
    vassert(m_storage != nullptr);
    if (m_frozen) {
        m_unreleased.clear();
        m_frozen = false;
    }
}

/**
 * Immediately free to OS when the tail is unused
 */
template<shrink_direction dir> inline void CompactingStorageTrait<dir>::released(
        typename CompactingStorageTrait<dir>::iterator iter) {
    vassert(m_storage != nullptr);
    if (iter->empty()) {
        if (m_frozen) {                // splice or free, depending on any active snapshot
            m_unreleased.emplace(*m_storage, iter);
        } else {
            m_storage->erase(iter);
        }
    }
}

template<shrink_direction dir> inline ExtendedIterator
CompactingStorageTrait<dir>::operator()() noexcept {
    return {dir == shrink_direction::head, m_unreleased.iterator()};
}

template<shrink_direction dir> inline ExtendedIterator
CompactingStorageTrait<dir>::operator()() const noexcept {
    return {dir == shrink_direction::head, m_unreleased.iterator()};
}

template<shrink_direction dir>
inline CompactingChunks<dir>::CompactingChunks(size_t tupleSize) noexcept : m_tupleSize(tupleSize) {
    trait::associate(this);
}

template<shrink_direction dir> inline size_t CompactingChunks<dir>::size() const noexcept {
    return m_allocs;
}

template<shrink_direction dir> inline void* CompactingChunks<dir>::allocate() {
    if (empty() || back().full()) {                  // always allocates from tail
        emplace_back(m_tupleSize);
    }
    ++m_allocs;
    return back().allocate();
}

/**
 * Ugly hack to work around using iterator of compacting chunks that deletes,
 * by signaling that the free() call on the invalid memory address is a no-op.
 */
template<shrink_direction> struct CompactingChunksIgnorableFree;
/**
 * When shrinking from head, since e.g. for_each<iterator_type>(...)
 * retrieves the address, advances iterator, then calls callable,
 * it is possible that the advanced address is invalidated if it is
 * (effectively) removed by the memory movement.
 */
template<> struct CompactingChunksIgnorableFree<shrink_direction::head> {
    inline bool operator()(ChunkList<CompactingChunk>const& l, void* dst) const noexcept {
        return l.cbegin()->next() == dst;
    }
};

/**
 * Things are tricker when shrinking from tail. When the
 * advanced address is the only available allocation in the whole
 * chunk, that chunk still exists when advance() is called but
 * ceases to be when free() depletes that only allocation. Since
 * Chunks are iterators are on different planets, there is no
 * remedy available, and client is responsible for catching
 * std::range_error from free() call.
 */
template<> struct CompactingChunksIgnorableFree<shrink_direction::tail> {
    inline constexpr bool operator()(ChunkList<CompactingChunk>const&, void*) const noexcept {
        return false;
    }
};

template<shrink_direction dir> void* CompactingChunks<dir>::free(void* dst) {
    static CompactingChunksIgnorableFree<dir> const ignored{};
    auto* pos = find(dst);                 // binary search
    if (pos == nullptr) {
        if (ignored(*this, dst)) {
            // See document in method declaration why this is not an error.
            --m_allocs;
            return nullptr;
        } else {
            snprintf(buf, sizeof buf, "CompactingChunks<%s>::free(%p): invalid address.",
                    dir == shrink_direction::head ? "head" : "tail", dst);
            buf[sizeof buf - 1] = 0;
            throw range_error(buf);
        }
    } else {
        auto from_iter = compactFrom();      // the tuple from which to memmove
        void* src = from_iter->free();
        auto& dst_iter = *pos;
        if (dst_iter != from_iter) {         // cross-chunk movement needed
            dst_iter->free(dst, src);        // memcpy()
        }
        trait::released(from_iter);
        --m_allocs;
        return src;
    }
}

namespace voltdb { namespace storage {          // older GCC would warn in absence of this
    template<shrink_direction dir> inline size_t CompactingChunks<dir>::tupleSize() const noexcept {
        return m_tupleSize;
    }

    template<> inline typename CompactingChunks<shrink_direction::head>::list_type::iterator
        CompactingChunks<shrink_direction::head>::compactFrom() noexcept {
            return begin();
        }

    template<> inline typename CompactingChunks<shrink_direction::tail>::list_type::iterator
        CompactingChunks<shrink_direction::tail>::compactFrom() noexcept {
            return prev(end());
        }

    template<> inline typename CompactingChunks<shrink_direction::head>::list_type::const_iterator
        CompactingChunks<shrink_direction::head>::compactFrom() const noexcept {
            return cbegin();
        }

    template<> inline typename CompactingChunks<shrink_direction::tail>::list_type::const_iterator
        CompactingChunks<shrink_direction::tail>::compactFrom() const noexcept {
            return prev(cend());
        }
}}

template<typename Chunks, typename Tag, typename E> Tag IterableTableTupleChunks<Chunks, Tag, E>::s_tagger{};
template<typename Chunks, typename Tag, typename E> bool const IterableTableTupleChunks<Chunks, Tag, E>::FALSE_VALUE = false;

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type src,
        bool const& f):
    m_offset(src.tupleSize()), m_storage(src),
    m_iter(m_storage.begin()),
    m_cursor(const_cast<value_type>(m_iter == m_storage.end() ? nullptr : m_iter->begin())),
    m_deletedSnapshot(f) {
    // paranoid check
    static_assert(is_reference<container_type>::value,
            "IterableTableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "IterableTableTupleChunks::value_type is not a pointer");
    while (m_cursor != nullptr && ! s_tagger(m_cursor)) {
        advance();         // calibrate to first non-skipped position
    }
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type c) {
    return {c};
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type c) {
    typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view> cur(c);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline bool IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::operator==(
        iterator_type<perm, view> const& o) const noexcept {
    return m_cursor == o.m_cursor;
}

/**
 * Helper to check if the iterator needs to advance to next
 * chunk. The only special occasion is snapshot view of
 * head-compacting, when there are tuple deletions occurring
 * during snapshot.
 */
template<typename Chunks, typename Iter, iterator_view_type view, typename Comp>
struct ChunkBoundary {
    inline void const* operator()(Chunks&, Iter const& iter, bool) const noexcept {
        return iter->next();
    }
};

template<typename Chunks, typename Iter>
struct ChunkBoundary<Chunks, Iter, iterator_view_type::snapshot, integral_constant<Compactibility, Compactibility::head>> {
    inline void const* operator()(Chunks& l, Iter const& iter, bool flag) const noexcept {
        return flag && iter == l.begin() ? iter->end() : iter->next();
    }
};

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
void IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::advance() {
    static constexpr ChunkBoundary<list_type, decltype(m_iter), view, typename Chunks::Compact> const boundary{};
    if (! drained()) {
        // Need to maintain invariant that m_cursor is nullptr iff
        // iterator points to end().
        bool finished = true;
        // we need to check emptyness since iterator could be
        // invalidated when non-lazily releasing the only available chunk.
        if (! m_storage.empty() && m_iter != m_storage.end()) {
            const_cast<void*&>(m_cursor) =
                reinterpret_cast<char*>(const_cast<void*>(m_cursor)) + m_offset;
            if (m_cursor < boundary(m_storage, m_iter, m_deletedSnapshot)) {
                finished = false;              // within chunk
            } else {
                ++m_iter;                      // cross chunk
                if (m_iter != m_storage.end()) {
                    const_cast<void*&>(m_cursor) = const_cast<void*>(m_iter->begin());
                    finished = false;
                } else {
                    const_cast<void*&>(m_cursor) = nullptr;
                }
            }
        } else {
            const_cast<void*&>(m_cursor) = nullptr;
        }
        if (! finished && ! s_tagger(m_cursor)) {
            advance();                         // skip current cursor
        }
    }
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>&
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::operator++() {
    advance();
    return *this;
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::operator++(int) {
    decltype(*this) copy(*this);
    advance();
    return copy;
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::value_type
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::operator*() noexcept {
    return m_cursor;
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline bool IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::drained() const noexcept {
    return m_cursor == nullptr;
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::iterator
IterableTableTupleChunks<Chunks, Tag, E>::begin(Chunks& c) {
    return iterator::begin(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::iterator
IterableTableTupleChunks<Chunks, Tag, E>::end(Chunks& c) {
    return iterator::end(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E>::cbegin(Chunks const& c) {
    return const_iterator::begin(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E>::cend(Chunks const& c) {
    return const_iterator::end(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E>::begin(Chunks const& c) {
    return cbegin(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E>::end(Chunks const& c) {
    return cend(c);
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm>
inline IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<perm>::iterator_cb_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::cb_type cb,
        bool const& f):
    super(c, f), m_cb(cb) {}            // using snapshot view to set list iterator begin

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::value_type
IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<perm>::operator*() noexcept {
    return m_cb(super::operator*());
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<perm>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::cb_type cb) {
    return {c, cb};
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<perm>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm>::cb_type cb) {
    typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<perm> cur(c, cb);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm>
inline IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::history_type h) :
    super(c, [&h](value_type c) { return const_cast<value_type>(h.reverted(const_cast<value_type>(c))); }, h.hasDeletes()),
    m_extendingCb(c()),
    m_extendingPtr(m_extendingCb.shrinkFromHead() ? m_extendingCb() : nullptr) {
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm>
inline bool IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::drained() const noexcept {
    return super::drained() && m_extendingPtr == nullptr;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm>
inline void IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::advance() {
    if (! drained()) {
        if (m_extendingPtr != nullptr) {
            m_extendingPtr = m_extendingCb();
        } else {
            super::advance();
            if (super::m_cursor == nullptr && ! m_extendingCb.shrinkFromHead()) {
                // tail compacting: point to deceased chunks
                m_extendingPtr = m_extendingCb();
            }
        }
    }
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>&
IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::operator++() {
    advance();
    return *this;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>
IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::operator++(int) {
    decltype(*this) copy(*this);
    advance();
    return copy;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::value_type
IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::operator*() noexcept {
    return m_extendingPtr == nullptr ? super::operator*() : super::m_cb(const_cast<value_type>(m_extendingPtr));
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>
IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::history_type h) {
    return {c, h};
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook, iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>
IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::history_type h) {
    IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm> cur(c, h);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm> inline
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::hooked_iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>::container_type c) :
super(c, c) {}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm> inline typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>::container_type c) {
    return {c};
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm> inline typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>::container_type c) {
    IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm> cur{c};
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb<Hook>
IterableTableTupleChunks<Chunks, Tag, E>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb<Hook>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb<Hook>::history_type h) {
    return {c, h};
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb<Hook>
IterableTableTupleChunks<Chunks, Tag, E>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb<Hook>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb<Hook>::history_type h) {
    iterator_cb<Hook> iter(c, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>
IterableTableTupleChunks<Chunks, Tag, E>::cbegin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::history_type h) {
    return {c, h};
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>
IterableTableTupleChunks<Chunks, Tag, E>::cend(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::history_type h) {
    const_iterator_cb<Hook> iter(c, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>
IterableTableTupleChunks<Chunks, Tag, E>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::history_type h) {
    return cbegin(c, h);
}

template<typename Chunks, typename Tag, typename E>
template<typename Hook>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>
IterableTableTupleChunks<Chunks, Tag, E>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template const_iterator_cb<Hook>::history_type h) {
    return cend(c, h);
}

template<unsigned char NthBit, typename E>
inline bool NthBitChecker<NthBit, E>::operator()(void* p) const noexcept {
    return *reinterpret_cast<unsigned char*>(p) & MASK;
}
template<unsigned char NthBit, typename E>
inline bool NthBitChecker<NthBit, E>::operator()(void const* p) const noexcept {
    return *reinterpret_cast<unsigned char const*>(p) & MASK;
}
template<unsigned char NthBit, typename E>
inline void NthBitChecker<NthBit, E>::set(void* p) noexcept {
    *reinterpret_cast<unsigned char*&>(p) |= MASK;
}
template<unsigned char NthBit, typename E>
inline void NthBitChecker<NthBit, E>::reset(void* p) noexcept {
    *reinterpret_cast<unsigned char*&>(p) &= ~MASK;
}

template<ptrdiff_t offset>
inline void* ByteOf<offset>::operator()(void* p) const noexcept {
    return reinterpret_cast<unsigned char*>(p) + offset;
}

template<ptrdiff_t offset>
inline void const* ByteOf<offset>::operator()(void const* p) const noexcept {
    return reinterpret_cast<unsigned char const*>(p) + offset;
}

inline BaseHistoryRetainTrait::BaseHistoryRetainTrait(typename BaseHistoryRetainTrait::cb_type const& cb): m_cb(cb) {}

inline HistoryRetainTrait<gc_policy::never>::HistoryRetainTrait(
        typename BaseHistoryRetainTrait::cb_type const& cb): BaseHistoryRetainTrait(cb) {}

inline void HistoryRetainTrait<gc_policy::never>::remove(void const*) noexcept {}

inline HistoryRetainTrait<gc_policy::always>::HistoryRetainTrait(
        typename BaseHistoryRetainTrait::cb_type const& cb): BaseHistoryRetainTrait(cb) {}

inline void HistoryRetainTrait<gc_policy::always>::remove(void const* addr) {
    m_cb(addr);
}

inline HistoryRetainTrait<gc_policy::batched>::HistoryRetainTrait(
        HistoryRetainTrait::cb_type const& cb): BaseHistoryRetainTrait(cb) {}

inline void HistoryRetainTrait<gc_policy::batched>::remove(void const* addr) {
    if (++m_size == BatchSize) {
        for_each(m_batched.begin(), m_batched.end(), m_cb);
        m_batched.clear();
        m_size = 0;
    } else {
        m_batched.emplace_front(addr);
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline TxnPreHook<Alloc, Trait, C, E>::TxnPreHook(size_t tupleSize):
    Trait([this](void const* key) {
                this->m_changes.erase(key);                        // no-op for non-existent key
                this->m_copied.erase(key);
                this->m_storage.tryFree(const_cast<void*>(key));
            }),
    m_storage(tupleSize) { }

template<typename Alloc, typename Trait, typename C, typename E> inline bool const&
TxnPreHook<Alloc, Trait, C, E>::hasDeletes() const noexcept {
    return m_hasDeletes;
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::copy(void const* p) {     // API essential
    if (m_last == nullptr) {
        m_last = m_storage.allocate();
        vassert(m_last != nullptr);
    }
    memcpy(m_last, p, m_storage.tupleSize());
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::add(typename TxnPreHook<Alloc, Trait, C, E>::ChangeType type,
        void const* src, void const* dst) {
    if (m_recording) {
        switch (type) {
            case ChangeType::Update:
                update(dst);
                break;
            case ChangeType::Insertion:
                insert(src, dst);
                break;
            case ChangeType::Deletion:
            default:
                remove(src, dst);
        }
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::freeze() noexcept {
    m_recording = true;
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::thaw() {
    if (m_recording) {
        m_changes.clear();
        m_copied.clear();
        m_storage.clear();
        m_hasDeletes = m_recording = false;
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void* TxnPreHook<Alloc, Trait, C, E>::_copy(void const* src, bool) {
    void* dst = m_storage.allocate();
    vassert(dst != nullptr);
    memcpy(dst, src, m_storage.tupleSize());
    m_copied.emplace(src);
    return dst;
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::update(void const* dst) {
    // src tuple from temp table written to dst in persistent storage
    if (m_recording && ! m_changes.count(dst)) {
        m_changes.emplace(dst, _copy(dst, false));
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::insert(void const* src, void const* dst) {
    vassert(src == nullptr);
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of. Just mark the position as
        // previously unused.
        m_changes.emplace(dst, nullptr);
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::remove(void const* src, void const* dst) {
    // src tuple is deleted, and tuple at dst gets moved to src
    if (m_recording && m_changes.count(src) == 0) {
        // Need to copy the original value that gets deleted
        vassert(m_last != nullptr);
        m_changes.emplace(src, m_last);
        m_last = nullptr;
        m_hasDeletes = true;
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void const* TxnPreHook<Alloc, Trait, C, E>::reverted(void const* src) const {
    auto const pos = m_changes.find(src);
    return pos == m_changes.cend() ? src : pos->second;
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::release(void const* src) {
    Trait::remove(src);
}

template<typename Chunks, typename Hook, typename E> inline
HookedCompactingChunks<Chunks, Hook, E>::HookedCompactingChunks(size_t s) noexcept : Chunks(s), Hook(s) {}

template<typename Chunks, typename Hook, typename E> inline void
HookedCompactingChunks<Chunks, Hook, E>::freeze() noexcept {
    Hook::freeze();
    Chunks::freeze();
}

template<typename Chunks, typename Hook, typename E> inline void
HookedCompactingChunks<Chunks, Hook, E>::thaw() {
    Hook::thaw();
    Chunks::thaw();
}

template<typename Chunks, typename Hook, typename E> inline void const*
HookedCompactingChunks<Chunks, Hook, E>::insert(void const* src) {
    void const* r = memcpy(Chunks::allocate(), src, Chunks::tupleSize());
    Hook::add(Hook::ChangeType::Insertion, nullptr, r);
    return r;
}

template<typename Chunks, typename Hook, typename E> inline void
HookedCompactingChunks<Chunks, Hook, E>::update(void* dst, void const* src) {
    Hook::add(Hook::ChangeType::Update, src, dst);
    memcpy(dst, src, Chunks::tupleSize());
}

template<typename Chunks, typename Hook, typename E> inline void const*
HookedCompactingChunks<Chunks, Hook, E>::remove(void* dst) {
    Hook::copy(dst);
    void const* src = Chunks::free(dst);
    Hook::add(Hook::ChangeType::Deletion, dst, src);
    return src;
}

// # # # # # # # # # # # # # # # # # Codegen: begin # # # # # # # # # # # # # # # # # # # # # # #
namespace __codegen__ {    // clumsy hack around macro arg arity check
template<shrink_direction dir, typename Alloc, gc_policy gc>
using mt = HookedCompactingChunks<CompactingChunks<dir>,
      TxnPreHook<NonCompactingChunks<Alloc>, HistoryRetainTrait<gc>>>;

using t1 = mt<shrink_direction::head, EagerNonCompactingChunk, gc_policy::never>;
using t2 = mt<shrink_direction::head, EagerNonCompactingChunk, gc_policy::always>;
using t3 = mt<shrink_direction::head, EagerNonCompactingChunk, gc_policy::batched>;

using t4 = mt<shrink_direction::head, LazyNonCompactingChunk, gc_policy::never>;
using t5 = mt<shrink_direction::head, LazyNonCompactingChunk, gc_policy::always>;
using t6 = mt<shrink_direction::head, LazyNonCompactingChunk, gc_policy::batched>;

using t7 = mt<shrink_direction::tail, EagerNonCompactingChunk, gc_policy::never>;
using t8 = mt<shrink_direction::tail, EagerNonCompactingChunk, gc_policy::always>;
using t9 = mt<shrink_direction::tail, EagerNonCompactingChunk, gc_policy::batched>;

using t10 = mt<shrink_direction::tail, LazyNonCompactingChunk, gc_policy::never>;
using t11 = mt<shrink_direction::tail, LazyNonCompactingChunk, gc_policy::always>;
using t12 = mt<shrink_direction::tail, LazyNonCompactingChunk, gc_policy::batched>;
}
#define range8(ranger)                                                           \
    ranger(0); ranger(1); ranger(2); ranger(3); ranger(4); ranger(5); ranger(6); ranger(7)
// BitChecker: 8 instantiations
#define NthBitCheckerCodegen(n) template struct voltdb::storage::NthBitChecker<n>
range8(NthBitCheckerCodegen);
#undef NthBitCheckerCodegen
// ByteOf for first 8 bytes: 8 instantiations
#define ByteOfCodegen(n) template struct voltdb::storage::ByteOf<n>
range8(ByteOfCodegen);
// composition of BytOf |> NthBitCheckerCodegen, for first 4 bytes: 8 x 4 = 32 instantiations
#define ComposeByteAndBit0(m, n) template struct voltdb::Compose<ByteOf<n>, NthBitChecker<m>>
#define ComposeByteAndBitCodegen(m)                                              \
    ComposeByteAndBit0(m, 0); ComposeByteAndBit0(m, 1); ComposeByteAndBit0(m, 2); ComposeByteAndBit0(m, 3)
range8(ComposeByteAndBitCodegen);
#undef ComposeByteAndBitCodegen
#undef ComposeByteAndBit0
#undef range8

// Chunks
template class voltdb::storage::NonCompactingChunks<EagerNonCompactingChunk>;
template class voltdb::storage::NonCompactingChunks<LazyNonCompactingChunk>;
template class voltdb::storage::CompactingChunks<shrink_direction::head>;
template class voltdb::storage::CompactingChunks<shrink_direction::tail>;
// HookedCompactingChunks : 2 x 2 x 3 = 12 instantiations
#define HookedChunksCodegen2(dir, alloc, gc)                                     \
    template class voltdb::storage::HookedCompactingChunks<                      \
        CompactingChunks<dir>, TxnPreHook<alloc, HistoryRetainTrait<gc>>>
#define HookedChunksCodegen1(dir, alloc)                                         \
    HookedChunksCodegen2(dir, alloc, gc_policy::never);                          \
    HookedChunksCodegen2(dir, alloc, gc_policy::always);                         \
    HookedChunksCodegen2(dir, alloc, gc_policy::batched)
#define HookedChunksCodegen(dir)                                                 \
    HookedChunksCodegen1(dir, NonCompactingChunks<EagerNonCompactingChunk>);     \
    HookedChunksCodegen1(dir, NonCompactingChunks<LazyNonCompactingChunk>)
HookedChunksCodegen(shrink_direction::head);
HookedChunksCodegen(shrink_direction::tail);
#undef HookedChunksCodegen
#undef HookedChunksCodegen1
#undef HookedChunksCodegen2
// iterators: 2 x (4 + 2 x 2 x 3) x 9 = ? instantiations
#define IteratorTagCodegen2(perm, chunks, tag)                                   \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>            \
    ::template iterator_type<perm, iterator_view_type::txn>;                     \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>            \
    ::template iterator_type<perm, iterator_view_type::snapshot>;                \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>            \
    ::template iterator_cb_type<perm>;

#define IteratorTagCodegen1(chunks, tag)                                         \
    IteratorTagCodegen2(iterator_permission_type::rw, chunks, tag);              \
    IteratorTagCodegen2(iterator_permission_type::ro, chunks, tag)
#define IteratorTagCodegen(tag)                                                  \
    IteratorTagCodegen1(NonCompactingChunks<EagerNonCompactingChunk>, tag);      \
    IteratorTagCodegen1(NonCompactingChunks<LazyNonCompactingChunk>, tag);       \
    IteratorTagCodegen1(CompactingChunks<shrink_direction::head>, tag);          \
    IteratorTagCodegen1(CompactingChunks<shrink_direction::tail>, tag);          \
    IteratorTagCodegen1(__codegen__::t1, tag);                                   \
    IteratorTagCodegen1(__codegen__::t2, tag);                                   \
    IteratorTagCodegen1(__codegen__::t3, tag);                                   \
    IteratorTagCodegen1(__codegen__::t4, tag);                                   \
    IteratorTagCodegen1(__codegen__::t5, tag);                                   \
    IteratorTagCodegen1(__codegen__::t6, tag);                                   \
    IteratorTagCodegen1(__codegen__::t7, tag);                                   \
    IteratorTagCodegen1(__codegen__::t8, tag);                                   \
    IteratorTagCodegen1(__codegen__::t9, tag);                                   \
    IteratorTagCodegen1(__codegen__::t10, tag);                                  \
    IteratorTagCodegen1(__codegen__::t11, tag);                                  \
    IteratorTagCodegen1(__codegen__::t12, tag)

IteratorTagCodegen(truth);
IteratorTagCodegen(NthBitChecker<0>); IteratorTagCodegen(NthBitChecker<1>);
IteratorTagCodegen(NthBitChecker<2>); IteratorTagCodegen(NthBitChecker<3>);
IteratorTagCodegen(NthBitChecker<4>); IteratorTagCodegen(NthBitChecker<5>);
IteratorTagCodegen(NthBitChecker<6>); IteratorTagCodegen(NthBitChecker<7>);
#undef IteratorTagCodegen
#undef IteratorTagCodegen1
#undef IteratorTagCodegen2
// TxnPreHook: 2 x 3 = 6 instantiations
#define TxnPreHookCodegen(alloc)                                                 \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::always>>;       \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::batched>>;      \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::never>>
// we do not use compacting chunk for underlying storage
TxnPreHookCodegen(NonCompactingChunks<EagerNonCompactingChunk>);
TxnPreHookCodegen(NonCompactingChunks<LazyNonCompactingChunk>);
#undef TxnPreHookCodegen
// time traveling iterator: explodes into 2 x 9 x 2 x 3 x 2 = 216 instantiations
#define TTIteratorCodegen4(chunks, tag, alloc, trait, perm)                             \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>::template         \
time_traveling_iterator_type<TxnPreHook<alloc, trait>, perm>
#define TTIteratorCodegen3(chunks, tag, alloc, trait)                                   \
    TTIteratorCodegen4(chunks, tag, alloc, trait, iterator_permission_type::ro);        \
    TTIteratorCodegen4(chunks, tag, alloc, trait, iterator_permission_type::rw)
#define TTIteratorCodegen2(chunks, tag, alloc)                                          \
    TTIteratorCodegen3(chunks, tag, alloc, HistoryRetainTrait<gc_policy::always>);      \
    TTIteratorCodegen3(chunks, tag, alloc, HistoryRetainTrait<gc_policy::batched>);     \
    TTIteratorCodegen3(chunks, tag, alloc, HistoryRetainTrait<gc_policy::never>)
#define TTIteratorCodegen1(chunks, tag)                                                 \
    TTIteratorCodegen2(chunks, tag, NonCompactingChunks<EagerNonCompactingChunk>);      \
    TTIteratorCodegen2(chunks, tag, NonCompactingChunks<LazyNonCompactingChunk>)
#define TTIteratorCodegen(chunks)                                                       \
    TTIteratorCodegen1(chunks, NthBitChecker<0>); TTIteratorCodegen1(chunks, NthBitChecker<1>); \
    TTIteratorCodegen1(chunks, NthBitChecker<2>); TTIteratorCodegen1(chunks, NthBitChecker<3>); \
    TTIteratorCodegen1(chunks, NthBitChecker<4>); TTIteratorCodegen1(chunks, NthBitChecker<5>); \
    TTIteratorCodegen1(chunks, NthBitChecker<6>); TTIteratorCodegen1(chunks, NthBitChecker<7>); \
    TTIteratorCodegen1(chunks, truth)
TTIteratorCodegen(CompactingChunks<shrink_direction::head>);
TTIteratorCodegen(CompactingChunks<shrink_direction::tail>);
TTIteratorCodegen(__codegen__::t1);
TTIteratorCodegen(__codegen__::t2);
TTIteratorCodegen(__codegen__::t3);
TTIteratorCodegen(__codegen__::t4);
TTIteratorCodegen(__codegen__::t5);
TTIteratorCodegen(__codegen__::t6);
TTIteratorCodegen(__codegen__::t7);
TTIteratorCodegen(__codegen__::t8);
TTIteratorCodegen(__codegen__::t9);
TTIteratorCodegen(__codegen__::t10);
TTIteratorCodegen(__codegen__::t11);
TTIteratorCodegen(__codegen__::t12);

#undef TTIteratorCodegen
#undef TTIteratorCodegen1
#undef TTIteratorCodegen2
#undef TTIteratorCodegen3
#undef TTIteratorCodegen4
// hooked_iterator_type : 8 x 2 x 2 x 3 = 96 instantiations
#define HookedIteratorCodegen3(tag, dir, alloc, gc)                                           \
template class voltdb::storage::IterableTableTupleChunks<                                     \
    HookedCompactingChunks<CompactingChunks<dir>, TxnPreHook<alloc, HistoryRetainTrait<gc>>>, \
    tag>::template hooked_iterator_type<iterator_permission_type::rw>;                                 \
template class voltdb::storage::IterableTableTupleChunks<                                     \
    HookedCompactingChunks<CompactingChunks<dir>, TxnPreHook<alloc, HistoryRetainTrait<gc>>>, \
    tag>::template hooked_iterator_type<iterator_permission_type::ro>
#define HookedIteratorCodegen2(tag, dir, alloc)                                               \
    HookedIteratorCodegen3(tag, dir, alloc, gc_policy::never);                                \
    HookedIteratorCodegen3(tag, dir, alloc, gc_policy::always);                               \
    HookedIteratorCodegen3(tag, dir, alloc, gc_policy::batched)
#define HookedIteratorCodegen1(tag, dir)                                                      \
    HookedIteratorCodegen2(tag, dir, NonCompactingChunks<EagerNonCompactingChunk>);           \
    HookedIteratorCodegen2(tag, dir, NonCompactingChunks<LazyNonCompactingChunk>)
#define HookedIteratorCodegen(tag)                                                            \
HookedIteratorCodegen1(tag, shrink_direction::head);                                          \
HookedIteratorCodegen1(tag, shrink_direction::tail)
HookedIteratorCodegen(truth);
HookedIteratorCodegen(NthBitChecker<0>); HookedIteratorCodegen(NthBitChecker<1>);
HookedIteratorCodegen(NthBitChecker<2>); HookedIteratorCodegen(NthBitChecker<3>);
HookedIteratorCodegen(NthBitChecker<4>); HookedIteratorCodegen(NthBitChecker<5>);
HookedIteratorCodegen(NthBitChecker<6>); HookedIteratorCodegen(NthBitChecker<7>);
#undef HookedIteratorCodegen
#undef HookedIteratorCodegen1
#undef HookedIteratorCodegen2
#undef HookedIteratorCodegen3
// # # # # # # # # # # # # # # # # # Codegen: end # # # # # # # # # # # # # # # # # # # # # # #

