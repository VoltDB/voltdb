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

inline void* std_allocator::alloc(size_t len) {
    return new char[len];
}

inline void std_allocator::dealloc(void* addr) {
    delete[] reinterpret_cast<char*>(addr);
}

inline size_t ChunkHolder::chunkSize(size_t tupleSize) noexcept {
    // preferred list of chunk sizes ranging from 4KB to 1MB
    static constexpr array<size_t, 9> const preferred{
        4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024,
          128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024
    };
    // we always pick smallest preferred chunk size to calculate
    // how many tuples a chunk fits
    return *find_if(preferred.cbegin(), preferred.cend(),
            [tupleSize](size_t s) { return tupleSize <= s; }) / tupleSize * tupleSize;
}

inline ChunkHolder::ChunkHolder(size_t tupleSize):
    m_tupleSize(tupleSize), m_chunkSize(chunkSize(tupleSize)),
    m_resource(new char[m_chunkSize]),
    m_begin(m_resource.get()),
    m_end(reinterpret_cast<char*const>(m_begin) + m_chunkSize),
    m_next(m_begin) {
    vassert(tupleSize <= 1024 * 1024);
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

inline NonCompactingChunk::NonCompactingChunk(NonCompactingChunk&& o):
    ChunkHolder(move(o)), m_freed(move(o.m_freed)) {}

inline void* NonCompactingChunk::allocate() noexcept {
    if (! m_freed.empty()) {               // allocate from free list first, in LIFO order
        void* r = m_freed.top();
        vassert(r < m_next && r >= m_begin);
        m_freed.pop();
        return r;
    } else {
        return ChunkHolder::allocate();
    }
}

inline void NonCompactingChunk::free(void* src) {
    if (reinterpret_cast<char*>(src) + m_tupleSize == m_next) {     // last element: decrement boundary ptr
        m_next = src;
    } else {                               // hole in the middle: keep track of it
        m_freed.push(src);
    }
}

inline bool NonCompactingChunk::full() const noexcept {
    return ChunkHolder::full() && m_freed.empty();
}

inline NonCompactingChunks::NonCompactingChunks(size_t tupleSize): m_tupleSize(tupleSize) {}

inline size_t NonCompactingChunks::tupleSize() const noexcept {
    return m_tupleSize;
}

inline void* NonCompactingChunks::allocate() noexcept {
    // linear search for non-full chunk
    auto iter = find_if(m_storage.begin(), m_storage.end(),
            [](NonCompactingChunk const& c) { return ! c.full(); });
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

inline void NonCompactingChunks::free(void* src) {
    // linear search for containing chunk
    auto iter = find_if(m_storage.begin(), m_storage.end(),
            [&src](NonCompactingChunk const& c) { return c.contains(src); });
    vassert(iter != m_storage.end());
    iter->free(src);
}

inline SelfCompactingChunk::SelfCompactingChunk(size_t s) : ChunkHolder(s) {}

inline void const* SelfCompactingChunk::begin() const noexcept {
    return m_begin;
}

inline void const* SelfCompactingChunk::end() const noexcept {
    return m_next;
}

inline void SelfCompactingChunk::free(void* dst, void const* src) {
    vassert(contains(dst));
    vassert(! contains(src));
    memcpy(dst, src, m_tupleSize);
    // current chunk is the last in the list: update alloc cursor
    reinterpret_cast<char*&>(m_next) -= m_tupleSize;
    vassert(m_next >= m_begin);
}

inline void* SelfCompactingChunk::free(void* dst) {                            // witin-chunk free
    vassert(contains(dst));
    vassert(m_next > m_begin);
    reinterpret_cast<char*&>(m_next) -= m_tupleSize;
    if (dst != m_next) {
        memcpy(dst, m_next, m_tupleSize);
    }                                      // else freeing last tuple in the chunk: no movement needed
    return m_next;
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
    if (iter->begin() == iter->end()) {
        l.erase(iter);
    }
}

inline boost::optional<typename CompactingStorageTrait<ShrinkDirection::tail>::iterator_type>
CompactingStorageTrait<ShrinkDirection::tail>::lastReleased() const {
    return {};
}

inline void CompactingStorageTrait<ShrinkDirection::head>::snapshot(bool snapshot) {
    if (! snapshot && m_inSnapshot) {      // snapshot finishes: releases all front chunks that were free.
        // This is the forcing of lazy-release
        if (m_last) {
            // invariant: if free() has ever been called, then we know from first free() invocation
            // on which chunk it was requested.
            vassert(m_list);
            m_list->erase(m_list->begin(),
                    (**m_last).begin() == (**m_last).end() ?              // is the tail of lazily-freed chunk empty?
                    *m_last : prev(*m_last));                             // if not, then we free up to (exclusively) m_last
            m_last = m_list->begin();
        }
    }
    m_inSnapshot = snapshot;
}

inline void CompactingStorageTrait<ShrinkDirection::head>::released(
        typename CompactingStorageTrait<ShrinkDirection::head>::list_type& l,
        typename CompactingStorageTrait<ShrinkDirection::head>::iterator_type iter) {
    // invariant: release() can only be called on the same list
    vassert(m_list == nullptr || m_list == &l);
    if (m_inSnapshot) {                    // Update iterator only when snapshot in progress
        m_last = iter;
        if (m_list == nullptr) {
            m_list = &l;
        }
    }
    // not in snapshot: eagerly release chunk memory if applicable
    if (! m_inSnapshot && iter->begin() == iter->end()) {
        l.erase(iter);
    }
}

inline boost::optional<typename CompactingStorageTrait<ShrinkDirection::head>::iterator_type>
CompactingStorageTrait<ShrinkDirection::head>::lastReleased() const {
    return m_last;
}

template<ShrinkDirection dir> inline SelfCompactingChunks<dir>::SelfCompactingChunks(size_t tupleSize) :
    m_tupleSize(tupleSize) {}

template<ShrinkDirection dir> inline void SelfCompactingChunks<dir>::snapshot(bool s) {
    m_trait.snapshot(s);
}

template<ShrinkDirection dir>
inline void* SelfCompactingChunks<dir>::allocate() noexcept {                  // always allocates from tail
    if (m_list.empty() || m_list.back().full()) {
        m_list.emplace_back(m_tupleSize);
    }
    return m_list.back().allocate();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::head>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::head>::compactFrom() {
    auto lastReleased = m_trait.lastReleased();
    return lastReleased ? *lastReleased : m_list.begin();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::tail>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::tail>::compactFrom() {
    return prev(m_list.end());
}

template<ShrinkDirection dir>
void* SelfCompactingChunks<dir>::free(void* dst) {
    auto dst_iter = find_if(m_list.begin(), m_list.end(),
            [dst](SelfCompactingChunk const& c) { return c.contains(dst); });
    if (dst_iter == m_list.cend()) {
        throw std::runtime_error("Address not found");
    }
    auto& from = compactFrom();       // the tuple from which to memmove
    void* src;
    if (dst_iter == from) {       // no cross-chunk movement needed
        src = dst_iter->free(dst);
    } else {
        void* src = from.free(
                reinterpret_cast<char*>(const_cast<void*>(from.end())) - m_tupleSize);
        dst_iter->free(dst, src);
    }
    m_trait.released(dst_iter);
    return src;
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::tail>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::tail>::chunkBegin() {
    return m_list.begin();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::head>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::head>::chunkBegin() {
    return compactFrom();
}

template<ShrinkDirection dir>
bool SelfCompactingChunks<dir>::less(void const* lhs, void const* rhs) const {
    if (lhs == rhs) {
        return false;
    } else {
        // linear search in chunks
        bool found1 = false, found2 = false;
        auto const pos = find_if(chunkBegin(), m_list.end(),
                [&found1, &found2, lhs, rhs](SelfCompactingChunk const& c) {
                    found1 |= c.contains(lhs);
                    found2 |= c.contains(rhs);
                    return found1 || found2;
                });
        vassert(pos != m_list.end());     // neither address is found anywhere
        if (found1 && !found2) {
            return true;
        } else {
            return found1 && found2 &&     // in the same chunk
                lhs < rhs;
        }
    }
}

template<bool Const>
inline IterableTableTupleChunks::iterator_type<Const>::iterator_type(
        typename IterableTableTupleChunks::iterator_type<Const>::container_type src):
    m_offset(src.m_tupleSize), m_list(src.m_list),
    m_iter(m_list.begin()),
    m_cursor(const_cast<value_type>(m_iter->begin())) {
    static_assert(is_reference<container_type>::value,
            "IterableTableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "IterableTableTupleChunks::value_type is not a pointer");
}

template<bool Const>
inline bool IterableTableTupleChunks::iterator_type<Const>::operator==(
        iterator_type const& o) const noexcept {
    return m_cursor == o.m_cursor;
}

template<bool Const>
void IterableTableTupleChunks::iterator_type<Const>::advance() {
    // Need to maintain invariant that m_cursor is nullptr iff
    // iterator points to end().
    if (m_iter != m_list.end()) {
        if (m_cursor >= m_iter->begin() && m_cursor < m_iter->end()) {     // within chunk
            m_cursor += m_offset;
        } else {                           // cross chunk
            ++m_iter;
            if (m_iter != m_list.end()) {
                m_cursor = m_iter->begin();
            } else {
                m_cursor = nullptr;
            }
        }
    } else {
        m_cursor = nullptr;
    }
}

template<bool Const>
inline IterableTableTupleChunks::iterator_type<Const>&
IterableTableTupleChunks::iterator_type<Const>::operator++() {
    advance();
    return *this;
}

template<bool Const>
inline IterableTableTupleChunks::iterator_type<Const>
IterableTableTupleChunks::iterator_type<Const>::operator++(int) {
    typename IterableTableTupleChunks::iterator_type<Const> copy(*this);
    advance();
    return copy;
}

template<bool Const>
inline typename IterableTableTupleChunks::template iterator_type<Const>::reference
IterableTableTupleChunks::iterator_type<Const>::operator*() noexcept {
    return m_cursor;
}

inline typename IterableTableTupleChunks::iterator IterableTableTupleChunks::begin() {
    return iterator(*this);
}

inline typename IterableTableTupleChunks::iterator IterableTableTupleChunks::end() {
    iterator iter(*this);
    iter.m_cursor = nullptr;               // see iterator::operator== and iterator::advance()
    return iter;
}

inline typename IterableTableTupleChunks::const_iterator IterableTableTupleChunks::cbegin() const {
    return const_iterator(*this);
}

inline typename IterableTableTupleChunks::const_iterator IterableTableTupleChunks::cend() const {
    const_iterator iter(*this);
    const_cast<typename remove_const<decltype(iter.m_cursor)>::type&>(iter.m_cursor) = nullptr;
    return iter;
}

inline typename IterableTableTupleChunks::const_iterator IterableTableTupleChunks::begin() const {
    return cbegin();
}

inline typename IterableTableTupleChunks::const_iterator IterableTableTupleChunks::end() const {
    return cend();
}

template<bool Const>
inline IterableTableTupleChunks::iterator_cb_type<Const>::iterator_cb_type(
        container_type c, cb_type cb): super(c), m_cb(cb) {}

template<bool Const>
inline typename IterableTableTupleChunks::template iterator_cb_type<Const>::value_type
IterableTableTupleChunks::iterator_cb_type<Const>::operator*() {
    return m_cb(super::operator*());
}

template<typename Alloc, typename Retainer, bool Const>
inline IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::time_traveling_iterator_type(
        typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::time_traveling_iterator_type::history_type h) :
    super(c, [&h](typename super::value_type c){
                using type = typename super::value_type;
                return const_cast<type>(h.reverted(const_cast<type>(c)));
            }) {}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::iterator_cb<Alloc, Retainer> IterableTableTupleChunks::begin(
        typename IterableTableTupleChunks::iterator_cb<Alloc, Retainer>::history_type h) {
    return iterator_cb<Alloc, Retainer>(*this, h);
}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::iterator_cb<Alloc, Retainer> IterableTableTupleChunks::end(
        typename IterableTableTupleChunks::iterator_cb<Alloc, Retainer>::history_type h) {
    iterator_cb<Alloc, Retainer> iter(*this, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer> IterableTableTupleChunks::cbegin(
        typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::history_type h) const {
    return const_iterator_cb<Alloc, Retainer>(*this, h);
}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer> IterableTableTupleChunks::cend(
        typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::history_type h) const {
    const_iterator_cb<Alloc, Retainer> iter(*this, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer> IterableTableTupleChunks::begin(
        typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::history_type h) const {
    return cbegin(h);
}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer> IterableTableTupleChunks::end(
        typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::history_type h) const {
    return cend(h);
}

inline typename IterableTableTupleChunks::iterator begin(IterableTableTupleChunks& c) {
    return c.begin();
}
inline typename IterableTableTupleChunks::iterator end(IterableTableTupleChunks& c) {
    return c.end();
}

inline typename IterableTableTupleChunks::const_iterator cbegin(IterableTableTupleChunks const& c) {
    return c.cbegin();
}
inline typename IterableTableTupleChunks::const_iterator cend(IterableTableTupleChunks const& c) {
    return c.cend();
}

inline typename IterableTableTupleChunks::const_iterator begin(IterableTableTupleChunks const& c) {
    return c.cbegin();
}
inline typename IterableTableTupleChunks::const_iterator end(IterableTableTupleChunks const& c) {
    return c.cend();
}

template<typename Alloc, typename Retainer, bool Const>
inline typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>
begin(typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::container_type c,
        typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::history_type h) {
    return c.begin(h);
}
template<typename Alloc, typename Retainer, bool Const>
inline typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>
end(typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::container_type c,
        typename IterableTableTupleChunks::time_traveling_iterator_type<Alloc, Retainer, Const>::history_type h) {
    return c.end(h);
}

template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>
cbegin(typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::container_type c,
        typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::history_type h) {
    return c.cbegin(h);
}
template<typename Alloc, typename Retainer>
inline typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>
cend(typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::container_type c,
        typename IterableTableTupleChunks::const_iterator_cb<Alloc, Retainer>::history_type h) {
    return c.cend(h);
}

inline BaseHistoryRetainTrait::BaseHistoryRetainTrait(
        typename BaseHistoryRetainTrait::cb_type const& cb): m_cb(cb) {}

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

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline TxnPreHook<Alloc, Retainer, E1, E2>::TxnPreHook(size_t tupleSize): m_storage(tupleSize) {}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::add(
        typename TxnPreHook<Alloc, Retainer, E1, E2>::ChangeType type,
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

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::start() noexcept {
    m_recording = true;
}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::stop() {
    m_recording = false;
    m_changes.clear();
    m_copied.clear();
}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void* TxnPreHook<Alloc, Retainer, E1, E2>::copy(void const* src) {
    void* dst = m_storage.allocate();
    vassert(dst != nullptr);
    memcpy(dst, src, m_storage.tupleSize());
    m_copied.emplace(src);
    return dst;
}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::update(void const* src, void const* dst) {
    // src tuple from temp table written to dst in persistent storage
    if (m_recording && ! m_changes.count(dst)) {
        m_changes.emplace(dst, copy(dst));
    }
}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::insert(void const* src, void const* dst) {
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of. Just mark the position as
        // previously unused.
        m_changes.emplace(dst, nullptr);
    }
}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::remove(void const* src, void const* dst) {
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

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void const* TxnPreHook<Alloc, Retainer, E1, E2>::reverted(void const* src) const {
    auto const pos = m_changes.find(src);
    return pos == m_changes.cend() ? src : pos->second;
}

template<typename Alloc, typename Retainer, typename E1, typename E2>
inline void TxnPreHook<Alloc, Retainer, E1, E2>::postReverted(void const* src) {
    // TODO
}
