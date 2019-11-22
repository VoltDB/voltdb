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

inline size_t ChunkHolder::chunkSize(size_t tupleSize) noexcept {
    // preferred list of chunk sizes ranging from 4KB to 4MB
    static constexpr array<size_t, 11> const preferred{                 // 0x400 == 1024
        4 * 0x400, 8 * 0x400, 16 * 0x400, 32 * 0x400, 64 * 0x400,       // 0x100_000 == 1024 * 1024
          128 * 0x400, 256 * 0x400, 512 * 0x400, 0x100000, 2 * 0x100000,
          4 * 0x100000
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

inline EagerNonCompactingChunk::EagerNonCompactingChunk(size_t s): ChunkHolder(s) {}

inline EagerNonCompactingChunk::EagerNonCompactingChunk(EagerNonCompactingChunk&& o):
    ChunkHolder(move(o)), m_freed(move(o.m_freed)) {}

inline void* EagerNonCompactingChunk::allocate() noexcept {
    if (! m_freed.empty()) {               // allocate from free list first, in LIFO order
        void* r = m_freed.top();
        vassert(r < m_next && r >= m_begin);
        m_freed.pop();
        return r;
    } else {
        return ChunkHolder::allocate();
    }
}

inline void EagerNonCompactingChunk::free(void* src) {
    if (reinterpret_cast<char*>(src) + m_tupleSize == m_next) {     // last element: decrement boundary ptr
        m_next = src;
    } else {                               // hole in the middle: keep track of it
        m_freed.push(src);
    }
}

inline bool EagerNonCompactingChunk::empty() const noexcept {
    return ChunkHolder::empty() || m_tupleSize * m_freed.size() ==
        reinterpret_cast<char const*>(m_next) - reinterpret_cast<char const*>(m_begin);
}

inline bool EagerNonCompactingChunk::full() const noexcept {
    return ChunkHolder::full() && m_freed.empty();
}

inline LazyNonCompactingChunk::LazyNonCompactingChunk(size_t tupleSize) : ChunkHolder(tupleSize) {}

inline void LazyNonCompactingChunk::free(void* src) {
    vassert(src >= m_begin && src < m_next);
    if (reinterpret_cast<char*>(src) + m_tupleSize == m_next) {     // last element: decrement boundary ptr
        m_next = src;
    } else {
        ++m_freed;
    }
    if (m_freed * m_tupleSize ==
            reinterpret_cast<char const*>(m_next) - reinterpret_cast<char const*>(m_begin)) {
        // everything had been freed, the chunk becomes empty
        m_next = m_begin;
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
    // linear search for containing chunk, keeping knowledge of
    // previous chunk in singly linked list
    auto iter = m_storage.begin(), prev = iter;
    while (iter != m_storage.end() && ! iter->contains(src)) {
        prev = iter++;
    }
    vassert(iter != m_storage.end());
    iter->free(src);
    if (iter->empty()) {                   // free the chunk
        if (iter == m_storage.begin()) {
            m_storage.pop_front();         // free first chunk
        } else {
            m_storage.erase_after(prev);   // free middle chunk
        }
    }
}

template<typename C, typename E>
inline bool NonCompactingChunks<C, E>::empty() const noexcept {
    return m_storage.empty();
}

template class NonCompactingChunks<EagerNonCompactingChunk>;
template class NonCompactingChunks<LazyNonCompactingChunk>;

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

template<> inline typename SelfCompactingChunks<ShrinkDirection::head>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::head>::compactFrom() {
    auto lastReleased = m_trait.lastReleased();
    return lastReleased ? *lastReleased : m_list.begin();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::tail>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::tail>::compactFrom() {
    return prev(m_list.end());
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::head>::list_type::const_iterator
SelfCompactingChunks<ShrinkDirection::head>::compactFrom() const {
    auto lastReleased = m_trait.lastReleased();
    return lastReleased ? *lastReleased : m_list.begin();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::tail>::list_type::const_iterator
SelfCompactingChunks<ShrinkDirection::tail>::compactFrom() const {
    return prev(m_list.cend());
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::tail>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::tail>::chunkBegin() {
    return m_list.begin();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::head>::list_type::iterator
SelfCompactingChunks<ShrinkDirection::head>::chunkBegin() {
    return compactFrom();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::tail>::list_type::const_iterator
SelfCompactingChunks<ShrinkDirection::tail>::chunkBegin() const {
    return m_list.cbegin();
}

template<> inline typename SelfCompactingChunks<ShrinkDirection::head>::list_type::const_iterator
SelfCompactingChunks<ShrinkDirection::head>::chunkBegin() const {
    return compactFrom();
}

template<typename Tag, typename E> Tag IterableTableTupleChunks<Tag, E>::s_tagger;

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::iterator_type(
        typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>::container_type src):
    m_offset(src.m_tupleSize), m_list(src.m_list),
    m_iter(TxnView ? src.chunkBegin() : m_list.begin()),
    m_cursor(const_cast<value_type>(m_iter->begin())) {
    static_assert(is_reference<container_type>::value,
            "IterableTableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "IterableTableTupleChunks::value_type is not a pointer");
    // iterator initial value (first chunk) should have been
    // allocated. This check is in case snapshot is in progress
    // and we are using head compaction scheme. In that case,
    // some leading chunks may be empty (all allocations are
    // freed as a result of hole-filling delete), and we don't
    // want the initial iterator to point to any empty chunk.
    vassert(m_iter->begin() != m_iter->end());
}

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>
IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::begin(
        typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>::container_type c) {
    return typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>(c);
}

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>
IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::end(
        typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>::container_type c) {
    typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView> cur(c);
    const_cast<void*&>(cur.m_cursor) = nullptr;
    return cur;
}

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline bool IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::operator==(
        iterator_type<Const, TxnView> const& o) const noexcept {
    return m_cursor == o.m_cursor;
}

template<typename Tag, typename E>
template<bool Const, bool TxnView>
void IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::advance() {
    // Need to maintain invariant that m_cursor is nullptr iff
    // iterator points to end().
    bool finished = true;
    if (m_iter != m_list.end()) {
        const_cast<void*&>(m_cursor) =
            reinterpret_cast<decltype(m_cursor)>(reinterpret_cast<char*>(m_cursor) + m_offset);
        if (m_cursor < m_iter->end()) {
            finished = false;              // within chunk
        } else {                           // cross chunk
            ++m_iter;
            if (m_iter != m_list.end()) {
                const_cast<void*&>(m_cursor) = const_cast<decltype(m_cursor)>(m_iter->begin());
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

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>&
IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::operator++() {
    advance();
    return *this;
}

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>
IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::operator++(int) {
    typename IterableTableTupleChunks::template iterator_type<Const, TxnView> copy(*this);
    advance();
    return copy;
}

template<typename Tag, typename E>
template<bool Const, bool TxnView>
inline typename IterableTableTupleChunks<Tag, E>::template iterator_type<Const, TxnView>::reference
IterableTableTupleChunks<Tag, E>::iterator_type<Const, TxnView>::operator*() noexcept {
    return m_cursor;
}

template<typename Tag, typename E>
inline typename IterableTableTupleChunks<Tag, E>::iterator IterableTableTupleChunks<Tag, E>::begin(
        typename IterableTableTupleChunks<Tag, E>::cont_type& c) {
    return iterator(c);
}

template<typename Tag, typename E>
inline typename IterableTableTupleChunks<Tag, E>::iterator IterableTableTupleChunks<Tag, E>::end(
        typename IterableTableTupleChunks<Tag, E>::cont_type& c) {
    return iterator::end(c);
}

template<typename Tag, typename E>
inline typename IterableTableTupleChunks<Tag, E>::const_iterator IterableTableTupleChunks<Tag, E>::cbegin(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c) {
    return const_iterator(c);
}

template<typename Tag, typename E>
inline typename IterableTableTupleChunks<Tag, E>::const_iterator IterableTableTupleChunks<Tag, E>::cend(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c) {
    return const_iterator::end(c);
}

template<typename Tag, typename E>
inline typename IterableTableTupleChunks<Tag, E>::const_iterator IterableTableTupleChunks<Tag, E>::begin(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c) {
    return cbegin(c);
}

template<typename Tag, typename E>
inline typename IterableTableTupleChunks<Tag, E>::const_iterator IterableTableTupleChunks<Tag, E>::end(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c) {
    return cend(c);
}

template<typename Tag, typename E>
template<bool Const>
inline IterableTableTupleChunks<Tag, E>::iterator_cb_type<Const>::iterator_cb_type(
        container_type c, cb_type cb): super(c, false), m_cb(cb) {}            // using snapshot view to set list iterator begin

template<typename Tag, typename E>
template<bool Const>
inline typename IterableTableTupleChunks<Tag, E>::template iterator_cb_type<Const>::value_type
IterableTableTupleChunks<Tag, E>::iterator_cb_type<Const>::operator*() {
    return m_cb(super::operator*());
}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy, bool Const, typename C>
inline IterableTableTupleChunks<Tag, E>::time_traveling_iterator_type<Alloc, policy, Const, C>::time_traveling_iterator_type(
        typename IterableTableTupleChunks<Tag, E>::template time_traveling_iterator_type<Alloc, policy, Const, C>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Tag, E>::template time_traveling_iterator_type<Alloc, policy, Const, C>::time_traveling_iterator_type::history_type h) :
    super(c, [&h](typename super::value_type c){
                using type = typename super::value_type;
                return const_cast<type>(h.reverted(const_cast<type>(c)));
            }) {}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Tag, E>::template iterator_cb<Alloc, policy> IterableTableTupleChunks<Tag, E>::begin(
        typename IterableTableTupleChunks<Tag, E>::cont_type& c,
        typename IterableTableTupleChunks<Tag, E>::template iterator_cb<Alloc, policy>::history_type h) {
    return iterator_cb<Alloc, policy>(c, h);
}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Tag, E>::template iterator_cb<Alloc, policy> IterableTableTupleChunks<Tag, E>::end(
        typename IterableTableTupleChunks<Tag, E>::cont_type& c,
        typename IterableTableTupleChunks<Tag, E>::template iterator_cb<Alloc, policy>::history_type h) {
    iterator_cb<Alloc, policy> iter(c, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy> IterableTableTupleChunks<Tag, E>::cbegin(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c,
        typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy>::history_type h) {
    return const_iterator_cb<Alloc, policy>(c, h);
}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy> IterableTableTupleChunks<Tag, E>::cend(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c,
        typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy>::history_type h) {
    const_iterator_cb<Alloc, policy> iter(c, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy> IterableTableTupleChunks<Tag, E>::begin(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c,
        typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy>::history_type h) {
    return cbegin(c, h);
}

template<typename Tag, typename E>
template<typename Alloc, RetainPolicy policy>
inline typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy> IterableTableTupleChunks<Tag, E>::end(
        typename IterableTableTupleChunks<Tag, E>::cont_type const& c,
        typename IterableTableTupleChunks<Tag, E>::template const_iterator_cb<Alloc, policy>::history_type h) {
    return cend(c, h);
}

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

template<typename iterator_type, typename Chunks, bool constness, typename, typename>
inline void for_each(Chunks& c, iterator_fun_type<constness>& f) {
    for_each(iterator_type::begin(c), iterator_type::end(c), f);
}

template<typename iterator_type, typename Chunks, typename F, bool, typename, typename>
inline void for_each(Chunks& c, F& f) {
    for_each(iterator_type::begin(c), iterator_type::end(c), f);
}

