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

using namespace voltdb;
using namespace voltdb::storage;

inline size_t ChunkHolder::chunkSize(size_t tupleSize) noexcept {
    // preferred list of chunk sizes ranging from 4KB to 16MB
    static constexpr array<size_t, 14> const preferred{                 // 0x400 == 1024
        512,                                                            // for testing
        4 * 0x400, 8 * 0x400, 16 * 0x400, 32 * 0x400, 64 * 0x400,       // 0x100_000 == 1024 * 1024
          128 * 0x400, 256 * 0x400, 512 * 0x400, 0x100000, 2 * 0x100000,
          4 * 0x100000, 8 * 0x100000, 16 * 0x100000
    };
    // we always pick smallest preferred chunk size to calculate
    // how many tuples a chunk fits. The picked chunk should fit
    // for > 4 allocations
    return *find_if(preferred.cbegin(), preferred.cend(),
            [tupleSize](size_t s) { return tupleSize * 4 <= s; })
        / tupleSize * tupleSize;
}

inline ChunkHolder::ChunkHolder(size_t tupleSize):
    m_tupleSize(tupleSize), m_chunkSize(chunkSize(tupleSize)),
    m_resource(new char[m_chunkSize]),
    m_begin(m_resource.get()),
    m_end(reinterpret_cast<char*const>(m_begin) + m_chunkSize),
    m_next(m_begin) {
    vassert(tupleSize <= 4 * 0x100000);
}

inline ChunkHolder::ChunkHolder(ChunkHolder&& rhs):
    m_tupleSize(rhs.m_tupleSize), m_chunkSize(rhs.m_chunkSize), m_resource(move(rhs.m_resource)),
    m_begin(rhs.m_begin), m_end(rhs.m_end), m_next(rhs.m_next) {
    const_cast<void*&>(rhs.m_begin) = nullptr;
}

inline void* ChunkHolder::allocate() noexcept {
    if (m_next >= m_end) {                 // chunk is full
        return nullptr;
    } else {
        void* res = m_next;
        reinterpret_cast<char*&>(m_next) += m_tupleSize;
        return res;
    }
}

inline bool ChunkHolder::contains(void const* addr) const {
    // check alignment
    vassert(addr < m_begin || addr >= m_end || 0 ==
            (reinterpret_cast<char const*>(addr) - reinterpret_cast<char*const>(m_begin)) % m_tupleSize);
    return addr >= m_begin && addr < m_next;
}

inline bool ChunkHolder::full() const noexcept {
    return m_next == m_end;
}

inline bool ChunkHolder::empty() const noexcept {
    return m_next == m_begin;
}

inline void* const ChunkHolder::begin() const noexcept {
    return m_begin;
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

inline void ThunkHolder::of(function<void(void)> const&& f) noexcept {
    vassert(! m_contains);
    m_thunk = f;
    m_contains = true;
}

inline void ThunkHolder::run() noexcept {
    if (m_contains) {
        m_thunk();
        m_contains = false;
    }
}

inline EagerNonCompactingChunk::EagerNonCompactingChunk(size_t s): ChunkHolder(s) {}

inline EagerNonCompactingChunk::EagerNonCompactingChunk(EagerNonCompactingChunk&& o):
    ChunkHolder(move(o)), m_freed(move(o.m_freed)) {}

inline void* EagerNonCompactingChunk::allocate() noexcept {
    if (! m_freed.empty()) {               // allocate from free list first, in LIFO order
        void* r = m_freed.top();
        vassert(r < m_next && r >= begin());
        m_freed.pop();
        return r;
    } else {
        return ChunkHolder::allocate();
    }
}

inline void EagerNonCompactingChunk::free(void* src) {
    if (reinterpret_cast<char*>(src) + tupleSize() == m_next) {     // last element: decrement boundary ptr
        m_next = src;
    } else {                               // hole in the middle: keep track of it
        m_freed.push(src);
    }
    if (empty()) {
        m_next = begin();
        decltype(m_freed) t{};
        m_freed.swap(t);
    }
}

inline bool EagerNonCompactingChunk::empty() const noexcept {
    return ChunkHolder::empty() || tupleSize() * m_freed.size() ==
        reinterpret_cast<char const*>(m_next) - reinterpret_cast<char const*>(begin());
}

inline bool EagerNonCompactingChunk::full() const noexcept {
    return ChunkHolder::full() && m_freed.empty();
}

inline LazyNonCompactingChunk::LazyNonCompactingChunk(size_t tupleSize) : ChunkHolder(tupleSize) {}

inline void LazyNonCompactingChunk::free(void* src) {
    vassert(src >= begin() && src < m_next);
    if (reinterpret_cast<char*>(src) + tupleSize() == m_next) {     // last element: decrement boundary ptr
        m_next = src;
    } else {
        ++m_freed;
    }
    if (m_freed * tupleSize() ==
            reinterpret_cast<char const*>(m_next) - reinterpret_cast<char const*>(begin())) {
        // everything had been freed, the chunk becomes empty
        m_next = begin();
        m_freed = 0;
    }
}

template<typename C, typename E>
inline NonCompactingChunks<C, E>::NonCompactingChunks(size_t tupleSize): m_tupleSize(tupleSize) {}

template<typename C, typename E>
inline size_t NonCompactingChunks<C, E>::tupleSize() const noexcept {
    return m_tupleSize;
}

template<typename C, typename E>
inline void* NonCompactingChunks<C, E>::allocate() noexcept {
    // First, remove chunk that need removing in previous free()
    // call.
    ThunkHolder::run();
    // linear search for non-full chunk
    auto iter = find_if(m_storage.begin(), m_storage.end(),
            [](C const& c) { return ! c.full(); });
    void* r;
    if (iter == m_storage.cend()) {        // all chunks are full
        m_storage.emplace_front(m_tupleSize);
        r = m_storage.front().allocate();
    } else {
        r = iter->allocate();
    }
    vassert(r != nullptr);
    return r;
}

template<typename C, typename E>
inline void NonCompactingChunks<C, E>::free(void* src) {
    // First, remove chunk that need removing in previous free()
    // call.
    ThunkHolder::run();
    // linear search for containing chunk, keeping knowledge of
    // previous chunk in singly linked list
    auto iter = m_storage.begin(), prev = iter;
    while (iter != m_storage.end() && ! iter->contains(src)) {
        prev = iter++;
    }
    vassert(iter != m_storage.end());
    iter->free(src);
    if (iter->empty()) {                   // create thunk for releasing the chunk
        // We need to delay release operation, bc otherwise when it is
        // called via iterator, the operation would invalidate
        // IterableTableTupleChunks::iterator::m_iter. Since that
        // no longer exists before advancing, the iterator on
        // NonCompactingChunks would get reset to end()
        if (iter == m_storage.begin()) {         // free first chunk
            if (next(iter) == m_storage.end()) {
                // the only chunk: we can safely remove it now,
                // knowing it wouldn't factually effect iterator
                // on chunks
                m_storage.pop_front();
            } else {
                ThunkHolder::of([this]() { m_storage.pop_front(); });
            }
        } else {   // free non-first chunk: we know for sure that
            // there would be remaining chunks after removing
            // current chunk, thus we need the thunk
            ThunkHolder::of([prev, this]() { m_storage.erase_after(prev); });
        }
    }
}

template<typename C, typename E>
inline bool NonCompactingChunks<C, E>::empty() noexcept {
    ThunkHolder::run();
    return m_storage.empty();
}

template<typename C, typename E>
inline typename NonCompactingChunks<C, E>::list_type& NonCompactingChunks<C, E>::storage() noexcept {
    return m_storage;
}

template<typename C, typename E>
inline typename NonCompactingChunks<C, E>::list_type const& NonCompactingChunks<C, E>::storage() const noexcept {
    return m_storage;
}

template<typename C, typename E>
inline typename NonCompactingChunks<C, E>::list_type::iterator
NonCompactingChunks<C, E>::chunkBegin() noexcept {
    return m_storage.begin();
}

template<typename C, typename E>
inline typename NonCompactingChunks<C, E>::list_type::const_iterator
NonCompactingChunks<C, E>::chunkBegin() const noexcept {
    return m_storage.begin();
}

inline CompactingChunk::CompactingChunk(size_t s) : ChunkHolder(s) {}

inline void CompactingChunk::free(void* dst, void const* src) {     // cross-chunk free(): update only on dst chunk
    vassert(contains(dst));
    vassert(! contains(src));
    memcpy(dst, src, tupleSize());
}

inline void* CompactingChunk::free(void* dst) {                     // within-chunk free()
    vassert(contains(dst));
    if (reinterpret_cast<char*>(dst) + tupleSize() == m_next) {         // last allocation on the chunk
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

/**
 * No-op when compacting from tail, since whether or not we are
 * in compacting does not alter the behavior of free() operation,
 * in particular, no need to delay releasing memory back to OS.
 */
inline void CompactingStorageTrait<ShrinkDirection::tail>::snapshot(bool) noexcept {}

/**
 * Immediately free to OS when the tail is unused
 */
inline void CompactingStorageTrait<ShrinkDirection::tail>::released(
        typename CompactingStorageTrait<ShrinkDirection::tail>::list_type& l,
        typename CompactingStorageTrait<ShrinkDirection::tail>::iterator_type iter) {
    ThunkHolder::run();
    if (iter->empty()) {
        ThunkHolder::of([&l, iter]() { l.erase(iter); });
    }
}

inline boost::optional<typename CompactingStorageTrait<ShrinkDirection::tail>::iterator_type>
CompactingStorageTrait<ShrinkDirection::tail>::lastReleased() const {
    return {};
}

inline void CompactingStorageTrait<ShrinkDirection::head>::snapshot(bool snapshot) {
    ThunkHolder::run();
    if (! snapshot && m_inSnapshot) {      // snapshot finishes: releases all front chunks that were free.
        // This is the forcing of lazy-release
        if (m_last) {
            // invariant: if free() has ever been called, then we know from first free() invocation
            // on which chunk it was requested. TODO: thunk
            vassert(m_list);
            m_list->erase(m_list->begin(),
                    (*m_last)->empty() ?              // is the tail of lazily-freed chunk empty?
                    *m_last : prev(*m_last));         // if not, then we free up to (exclusively) m_last
            m_last = m_list->begin();
        }
    }
    m_inSnapshot = snapshot;
}

inline void CompactingStorageTrait<ShrinkDirection::head>::released(
        typename CompactingStorageTrait<ShrinkDirection::head>::list_type& l,
        typename CompactingStorageTrait<ShrinkDirection::head>::iterator_type iter) {
    ThunkHolder::run();
    // invariant: release() can only be called on the same list
    vassert(m_list == nullptr || m_list == &l);
    if (m_inSnapshot) {                    // Update iterator only when snapshot in progress
        m_last = iter;
        if (m_list == nullptr) {
            m_list = &l;
        }
    }
    // not in snapshot: eagerly release chunk memory if applicable
    if (! m_inSnapshot && iter->empty()) {
        ThunkHolder::of([&l, iter]() { l.erase(iter); });
    }
}

inline boost::optional<typename CompactingStorageTrait<ShrinkDirection::head>::iterator_type>
CompactingStorageTrait<ShrinkDirection::head>::lastReleased() const {
    return m_last;
}

template<ShrinkDirection dir> inline CompactingChunks<dir>::CompactingChunks(size_t tupleSize) :
    m_tupleSize(tupleSize) {}

template<ShrinkDirection dir> inline void CompactingChunks<dir>::snapshot(bool s) {
    m_trait.snapshot(s);
}

template<ShrinkDirection dir> inline bool CompactingChunks<dir>::empty() {
    m_trait.run();
    return m_list.empty();
}

template<ShrinkDirection dir> inline void* CompactingChunks<dir>::allocate() noexcept {
    m_trait.run();
    if (m_list.empty() || m_list.back().full()) {                  // always allocates from tail
        m_list.emplace_back(m_tupleSize);
    }
    return m_list.back().allocate();
}

template<ShrinkDirection dir> void* CompactingChunks<dir>::free(void* dst) {
    m_trait.run();
    auto dst_iter = find_if(m_list.begin(), m_list.end(),
            [dst](CompactingChunk const& c) { return c.contains(dst); });
    if (dst_iter == m_list.cend()) {
        throw std::runtime_error("Address not found");
    }
    auto from_iter = compactFrom();   // the tuple from which to memmove
    void* src = from_iter->free();
    if (dst_iter != from_iter) {      // cross-chunk movement needed
        dst_iter->free(dst, src);     // memcpy()
    }
    m_trait.released(m_list, from_iter);    // delayed potential chunk removal
    return src;
}

template<ShrinkDirection dir> inline size_t CompactingChunks<dir>::tupleSize() const noexcept {
    return m_tupleSize;
}

template<ShrinkDirection dir>
inline typename CompactingChunks<dir>::list_type& CompactingChunks<dir>::storage() noexcept {
    return m_list;
}

template<ShrinkDirection dir>
inline typename CompactingChunks<dir>::list_type const& CompactingChunks<dir>::storage() const noexcept {
    return m_list;
}

template<> inline typename CompactingChunks<ShrinkDirection::head>::list_type::iterator
CompactingChunks<ShrinkDirection::head>::compactFrom() {
    m_trait.run();
    auto lastReleased = m_trait.lastReleased();
    return lastReleased ? *lastReleased : m_list.begin();
}

template<> inline typename CompactingChunks<ShrinkDirection::tail>::list_type::iterator
CompactingChunks<ShrinkDirection::tail>::compactFrom() {
    m_trait.run();
    return prev(m_list.end());
}

template<> inline typename CompactingChunks<ShrinkDirection::head>::list_type::const_iterator
CompactingChunks<ShrinkDirection::head>::compactFrom() const {
    auto lastReleased = m_trait.lastReleased();
    return lastReleased ? *lastReleased : m_list.begin();
}

template<> inline typename CompactingChunks<ShrinkDirection::tail>::list_type::const_iterator
CompactingChunks<ShrinkDirection::tail>::compactFrom() const {
    return prev(m_list.cend());
}

template<> inline typename CompactingChunks<ShrinkDirection::tail>::list_type::iterator
CompactingChunks<ShrinkDirection::tail>::chunkBegin() {
    return m_list.begin();
}

template<> inline typename CompactingChunks<ShrinkDirection::head>::list_type::iterator
CompactingChunks<ShrinkDirection::head>::chunkBegin() {
    return compactFrom();
}

template<> inline typename CompactingChunks<ShrinkDirection::tail>::list_type::const_iterator
CompactingChunks<ShrinkDirection::tail>::chunkBegin() const {
    return m_list.cbegin();
}

template<> inline typename CompactingChunks<ShrinkDirection::head>::list_type::const_iterator
CompactingChunks<ShrinkDirection::head>::chunkBegin() const {
    return compactFrom();
}

template<typename Chunks, typename Tag, typename E1, typename E2>
Tag IterableTableTupleChunks<Chunks, Tag, E1, E2>::s_tagger;

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>::container_type src):
    m_offset(src.tupleSize()), m_list(src.storage()),
    m_iter(view == iterator_view_type::txn ? src.chunkBegin() : m_list.begin()),
    m_cursor(const_cast<value_type>(m_iter == m_list.end() ? nullptr : m_iter->begin())) {
    // paranoid check
    static_assert(is_reference<container_type>::value,
            "IterableTableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "IterableTableTupleChunks::value_type is not a pointer");
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>::container_type c) {
    return {c};
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>::container_type c) {
    typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view> cur(c);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline bool IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::operator==(
        iterator_type<perm, view> const& o) const noexcept {
    return m_cursor == o.m_cursor;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
void IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::advance() {
    // Need to maintain invariant that m_cursor is nullptr iff
    // iterator points to end().
    bool finished = true;
    // we need to check emptyness since iterator could be
    // invalidated when non-lazily releasing the only available chunk.
    if (! m_list.empty() && m_iter != m_list.end()) {
        const_cast<void*&>(m_cursor) =
            reinterpret_cast<char*>(const_cast<void*>(m_cursor)) + m_offset;
        if (m_cursor < m_iter->next()) {
            finished = false;              // within chunk
        } else {
            ++m_iter;                      // cross chunk
            if (m_iter != m_list.end()) {
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

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>&
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::operator++() {
    advance();
    return *this;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::operator++(int) {
    typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view> copy(*this);
    advance();
    return copy;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm, iterator_view_type view>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_type<perm, view>::reference
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_type<perm, view>::operator*() noexcept {
    return m_cursor;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator
IterableTableTupleChunks<Chunks, Tag, E1, E2>::begin(Chunks& c) {
    return iterator::begin(c);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator
IterableTableTupleChunks<Chunks, Tag, E1, E2>::end(Chunks& c) {
    return iterator::end(c);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E1, E2>::cbegin(Chunks const& c) {
    return const_iterator::begin(c);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E1, E2>::cend(Chunks const& c) {
    return const_iterator::end(c);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E1, E2>::begin(Chunks const& c) {
    return cbegin(c);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E1, E2>::end(Chunks const& c) {
    return cend(c);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm>
inline IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_cb_type<perm>::iterator_cb_type(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::cb_type cb):
    super(c, false), m_cb(cb) {}            // using snapshot view to set list iterator begin

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::value_type
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_cb_type<perm>::operator*() {
    return m_cb(super::operator*());
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_cb_type<perm>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::cb_type cb) {
    return {c, cb};
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::iterator_cb_type<perm>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm>::cb_type cb) {
    typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb_type<perm> cur(c, cb);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy, iterator_permission_type perm, typename C>
inline IterableTableTupleChunks<Chunks, Tag, E1, E2>::time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type::history_type h) :
    super(c, [&h](typename super::value_type c){
                using type = typename super::value_type;
                return const_cast<type>(h.reverted(const_cast<type>(c)));
            }) {}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy, iterator_permission_type perm, typename C>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::time_traveling_iterator_type<Alloc, policy, perm, C>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type::history_type h) {
    return {c, h};
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy, iterator_permission_type perm, typename C>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::time_traveling_iterator_type<Alloc, policy, perm, C>::end(
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template time_traveling_iterator_type<Alloc, policy, perm, C>::time_traveling_iterator_type::history_type h) {
    IterableTableTupleChunks<Chunks, Tag, E1, E2>::time_traveling_iterator_type<Alloc, policy, perm, C> cur(c, h);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb<Alloc, policy>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::begin(
        Chunks& c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb<Alloc, policy>::history_type h) {
    return {c, h};
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb<Alloc, policy>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::end(
        Chunks& c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template iterator_cb<Alloc, policy>::history_type h) {
    iterator_cb<Alloc, policy> iter(c, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::cbegin(
        Chunks const& c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>::history_type h) {
    return {c, h};
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::cend(
        Chunks const& c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>::history_type h) {
    const_iterator_cb<Alloc, policy> iter(c, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::begin(
        Chunks const& c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>::history_type h) {
    return cbegin(c, h);
}

template<typename Chunks, typename Tag, typename E1, typename E2>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>
IterableTableTupleChunks<Chunks, Tag, E1, E2>::end(
        Chunks const& c,
        typename IterableTableTupleChunks<Chunks, Tag, E1, E2>::template const_iterator_cb<Alloc, policy>::history_type h) {
    return cend(c, h);
}

struct voltdb::storage::truth {                                 // simplest Tag
    constexpr bool operator()(void*) const noexcept {
        return true;
    }
    constexpr bool operator()(void const*) const noexcept {
        return true;
    }
};

template<unsigned char NthBit> struct voltdb::storage::NthBitChecker {
    NthBitChecker() noexcept {
        static_assert(NthBit < 8, "The n-th bit must be between 0-7 inclusively.");
    }
    constexpr bool operator()(void* p) const noexcept {
        return *reinterpret_cast<char*>(p) & (1 << NthBit);
    }
    constexpr bool operator()(void const* p) const noexcept {
        return *reinterpret_cast<char const*>(p) & (1 << NthBit);
    }
    static void set(void* p) noexcept {
        *reinterpret_cast<char*&>(p) |= 1 << NthBit;
    }
    static void reset(void* p) noexcept {
        *reinterpret_cast<char*&>(p) &= ~(1 << NthBit);
    }
};

// # # # # # # # # # # # # # # # # # codegen: begin # # # # # # # # # # # # # # # # # # # # # # #
// Chunks
template class voltdb::storage::NonCompactingChunks<EagerNonCompactingChunk>;
template class voltdb::storage::NonCompactingChunks<LazyNonCompactingChunk>;
template class voltdb::storage::CompactingChunks<ShrinkDirection::head>;
template class voltdb::storage::CompactingChunks<ShrinkDirection::tail>;
// non-compacting chunks
template class voltdb::storage::IterableTableTupleChunks<NonCompactingChunks<EagerNonCompactingChunk>, truth>
    ::template iterator_type<iterator_permission_type::rw, iterator_view_type::txn>;          // iterator
template class voltdb::storage::IterableTableTupleChunks<NonCompactingChunks<LazyNonCompactingChunk>, truth>
    ::template iterator_type<iterator_permission_type::rw, iterator_view_type::txn>;
template class voltdb::storage::IterableTableTupleChunks<NonCompactingChunks<EagerNonCompactingChunk>, truth>
    ::template iterator_type<iterator_permission_type::ro, iterator_view_type::txn>;           // const_iterator
template class voltdb::storage::IterableTableTupleChunks<NonCompactingChunks<LazyNonCompactingChunk>, truth>
    ::template iterator_type<iterator_permission_type::ro, iterator_view_type::txn>;
// self-compacting chunks: the txn view
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::head>, truth>
    ::template iterator_type<iterator_permission_type::rw, iterator_view_type::txn>;          // iterator
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::tail>, truth>
    ::template iterator_type<iterator_permission_type::rw, iterator_view_type::txn>;
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::head>, truth>
    ::template iterator_type<iterator_permission_type::ro, iterator_view_type::txn>;           // const_iterator
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::tail>, truth>
    ::template iterator_type<iterator_permission_type::ro, iterator_view_type::txn>;
// self-compacting chunks: the snapshot view
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::head>, truth>
    ::template iterator_type<iterator_permission_type::rw, iterator_view_type::snapshot>;      // iterator
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::tail>, truth>
    ::template iterator_type<iterator_permission_type::rw, iterator_view_type::snapshot>;
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::head>, truth>
    ::template iterator_type<iterator_permission_type::ro, iterator_view_type::snapshot>;       // const_iterator
template class voltdb::storage::IterableTableTupleChunks<CompactingChunks<ShrinkDirection::tail>, truth>
    ::template iterator_type<iterator_permission_type::ro, iterator_view_type::snapshot>;
// # # # # # # # # # # # # # # # # # codegen: end # # # # # # # # # # # # # # # # # # # # # # #

inline BaseHistoryRetainTrait::BaseHistoryRetainTrait(typename BaseHistoryRetainTrait::cb_type const& cb): m_cb(cb) {}

inline HistoryRetainTrait<RetainPolicy::never>::HistoryRetainTrait(
        typename BaseHistoryRetainTrait::cb_type const& cb): BaseHistoryRetainTrait(cb) {}

inline void HistoryRetainTrait<RetainPolicy::never>::remove(void const*) noexcept {}

inline HistoryRetainTrait<RetainPolicy::always>::HistoryRetainTrait(
        typename BaseHistoryRetainTrait::cb_type const& cb): BaseHistoryRetainTrait(cb) {}

inline void HistoryRetainTrait<RetainPolicy::always>::remove(void const* addr) {
    m_cb(addr);
}

inline HistoryRetainTrait<RetainPolicy::batched>::HistoryRetainTrait(
        HistoryRetainTrait::cb_type const& cb, size_t batchSize):
    BaseHistoryRetainTrait(cb), m_batchSize(batchSize) {}

inline void HistoryRetainTrait<RetainPolicy::batched>::remove(void const* addr) {
    if (++m_size == m_batchSize) {
        for_each(m_batched.begin(), m_batched.end(), m_cb);
        m_batched.clear();
        m_size = 0;
    } else {
        m_batched.emplace_front(addr);
    }
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline TxnPreHook<Alloc, policy, C, E>::TxnPreHook(size_t tupleSize):
    m_storage(tupleSize),
    m_retainer([this](void const* addr) {
                vassert(this->m_changes.count(addr) && this->m_copied.count(addr));
                this->m_changes.erase(addr);
                this->m_copied.erase(addr);
            }) {}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::add(typename TxnPreHook<Alloc, policy, C, E>::ChangeType type,
        void const* src, void const* dst) {
    if (m_recording) {
        switch (type) {
            case ChangeType::Update:
                update(src, dst);
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

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::start() noexcept {
    m_recording = true;
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::stop() {
    m_recording = false;
    m_changes.clear();
    m_copied.clear();
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void* TxnPreHook<Alloc, policy, C, E>::copy(void const* src) {
    void* dst = m_storage.allocate();
    vassert(dst != nullptr);
    memcpy(dst, src, m_storage.tupleSize());
    m_copied.emplace(src);
    return dst;
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::update(void const* src, void const* dst) {
    // src tuple from temp table written to dst in persistent storage
    if (m_recording && ! m_changes.count(dst)) {
        m_changes.emplace(dst, copy(dst));
    }
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::insert(void const* src, void const* dst) {
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of. Just mark the position as
        // previously unused.
        m_changes.emplace(dst, nullptr);
    }
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::remove(void const* src, void const* dst) {
    // src tuple is deleted, and tuple at dst gets moved to src
    if (m_recording) {
        if (! m_changes.count(src)) {
            // Need to copy the original value that gets deleted
            m_changes.emplace(src, copy(src));
        }
        if (! m_changes.count(dst)) {
            // Only need to point to front place that holds the
            // value getting moved to the place that was deleted.
            m_changes.emplace(dst, dst);
        }
    }
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void const* TxnPreHook<Alloc, policy, C, E>::reverted(void const* src) const {
    auto const pos = m_changes.find(src);
    return pos == m_changes.cend() ? src : pos->second;
}

template<typename Alloc, RetainPolicy policy, typename C, typename E>
inline void TxnPreHook<Alloc, policy, C, E>::postReverted(void const* src) {
    m_retainer.remove(src);
}

