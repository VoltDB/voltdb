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

#include "TableTupleAllocator.hpp"
#include "common/debuglog.h"
#include <array>
#include <numeric>

using namespace voltdb;
using namespace voltdb::storage;

static char buf[128];

inline ThreadLocalPoolAllocator::ThreadLocalPoolAllocator(size_t n) : m_blkSize(n),
    m_base(reinterpret_cast<char*>(allocateExactSizedObject(m_blkSize))) {
    vassert(m_base != nullptr);
}

inline ThreadLocalPoolAllocator::~ThreadLocalPoolAllocator() {
    freeExactSizedObject(m_blkSize, const_cast<char*>(m_base));
}

inline char* ThreadLocalPoolAllocator::get() const noexcept {
    return const_cast<char*>(m_base);
}

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

/**
 * Implementation detail
 */
inline static size_t chunkSize(size_t tupleSize) noexcept {
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
    // we always pick smallest preferred chunk size to calculate
    // how many tuples a chunk fits. The picked chunk should fit
    // for >= 32 allocations
    return *find_if(preferred.cbegin(), preferred.cend(),
            [tupleSize](size_t s) noexcept {
                return tupleSize * 32 <= s;
            }) / tupleSize * tupleSize;
}

// We remove member initialization from init list to save from
// storing chunk size into object
template<allocator_enum_type T> inline ChunkHolder<T>::ChunkHolder(id_type id, size_t tupleSize, size_t storageSize) :
    allocator_type<T>(storageSize), m_id(id), m_tupleSize(tupleSize),
    m_end(allocator_type<T>::get() + storageSize), m_next(allocator_type<T>::get()) {
    vassert(tupleSize <= 4 * 0x100000);
    vassert(m_next != nullptr);
}

template<allocator_enum_type T> inline id_type ChunkHolder<T>::id() const noexcept {
    return m_id;
}

template<allocator_enum_type T> inline void* ChunkHolder<T>::allocate() noexcept {
    if (next() >= end()) {                 // chunk is full
        return nullptr;
    } else {
        void* res = next();
        reinterpret_cast<char*&>(m_next) += m_tupleSize;
        return res;
    }
}

template<allocator_enum_type T> inline bool ChunkHolder<T>::contains(void const* addr) const {
    // check alignment
    vassert(addr < begin() || addr >= end() || 0 ==
            (reinterpret_cast<char const*>(addr) - reinterpret_cast<char*const>(begin())) % m_tupleSize);
    return addr >= begin() && addr < next();
}

template<allocator_enum_type T> inline bool ChunkHolder<T>::full() const noexcept {
    return next() == end();
}

template<allocator_enum_type T> inline bool ChunkHolder<T>::empty() const noexcept {
    return next() == begin();
}

template<allocator_enum_type T> inline void* const ChunkHolder<T>::begin() const noexcept {
    return reinterpret_cast<void*>(allocator_type<T>::get());
}

template<allocator_enum_type T> inline void* const ChunkHolder<T>::end() const noexcept {
    return m_end;
}

template<allocator_enum_type T> inline void* const ChunkHolder<T>::next() const noexcept {
    return m_next;
}

template<allocator_enum_type T> inline size_t ChunkHolder<T>::tupleSize() const noexcept {
    return m_tupleSize;
}

template<allocator_enum_type T> inline allocator_type<T>& ChunkHolder<T>::get_allocator() noexcept {
    return *this;
}

template<allocator_enum_type T> inline allocator_type<T> const& ChunkHolder<T>::get_allocator() const noexcept {
    return *this;
}

inline EagerNonCompactingChunk::EagerNonCompactingChunk(id_type s1, size_t s2, size_t s3) : super(s1, s2, s3) {}

inline void* EagerNonCompactingChunk::allocate() noexcept {
    if (m_freed.empty()) {
        return super::allocate();
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
    return super::empty() || tupleSize() * m_freed.size() ==
        reinterpret_cast<char const*>(next()) - reinterpret_cast<char const*>(begin());
}

inline bool EagerNonCompactingChunk::full() const noexcept {
    return super::full() && m_freed.empty();
}

inline LazyNonCompactingChunk::LazyNonCompactingChunk(id_type s1, size_t s2, size_t s3) : super(s1, s2, s3) {}

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

template<typename K, typename Cmp> inline StxCollections::set<K, Cmp>::set(
        initializer_list<typename StxCollections::set<K, Cmp>::value_type> l) : super() {
    copy(l.begin(), l.end(), inserter(*this, end()));
}

template<typename K, typename Cmp>
template<typename InputIt> inline StxCollections::set<K, Cmp>::set(InputIt from, InputIt to) : super(from, to) {}

template<typename K, typename Cmp> inline
typename StxCollections::set<K, Cmp>::const_iterator StxCollections::set<K, Cmp>::cbegin() const {
    return super::begin();
}

template<typename K, typename Cmp> inline
typename StxCollections::set<K, Cmp>::const_iterator StxCollections::set<K, Cmp>::cend() const {
    return super::end();
}

template<typename K, typename Cmp>
template<typename... Args> inline
pair<typename StxCollections::set<K, Cmp>::iterator, bool> StxCollections::set<K, Cmp>::emplace(Args&&... args) {
    return super::insert(value_type{forward<Args>(args)...});
}

template<typename K, typename V, typename Cmp>
template<typename InputIt> inline
StxCollections::map<K, V, Cmp>::map(InputIt from, InputIt to) : super(from, to) {}

template<typename K, typename V, typename Cmp> inline StxCollections::map<K, V, Cmp>::map(
        initializer_list<typename StxCollections::map<K, V, Cmp>::value_type> l) : super() {
    copy(l.begin(), l.end(), inserter(*this, end()));
}

template<typename K, typename V, typename Cmp> inline
typename StxCollections::map<K, V, Cmp>::const_iterator StxCollections::map<K, V, Cmp>::cbegin() const {
    return super::begin();
}

template<typename K, typename V, typename Cmp> inline
typename StxCollections::map<K, V, Cmp>::const_iterator StxCollections::map<K, V, Cmp>::cend() const {
    return super::end();
}

template<typename K, typename V, typename Cmp>
template<typename... Args> inline
pair<typename StxCollections::map<K, V, Cmp>::iterator, bool> StxCollections::map<K, V, Cmp>::emplace(Args&&... args) {
    return super::insert(value_type{forward<Args>(args)...});
}

template<typename Chunk, typename E>
inline pair<bool, typename ChunkList<Chunk, E>::iterator> ChunkList<Chunk, E>::find(void const* k) const {
    // NOTE: this is a hacky method signature, and hacky way to
    // get around. Normally, we need to overload 2 versions of
    // find: one const-method that returns a const-iterator, and
    // one non-const-method that returns a normal iterator.
    // However, doing this would require downstream API
    // (CompactingChunks::find), and a couple more places to do
    // the same, and TxnLeftBoundary, etc. to maintain two sets
    // of iterators.
    auto* mutable_this = const_cast<ChunkList<Chunk, E>*>(this);
    auto r = make_pair(! m_byAddr.empty(), mutable_this->end());
    if (r.first) {
        // find first entry whose begin() > k
        auto iter = prev(mutable_this->m_byAddr.upper_bound(k));
        if (iter != mutable_this->m_byAddr.end()) {
            r.second = iter->second;
        }
    }
    return r;
}

template<typename Chunk, typename E> inline pair<bool, typename ChunkList<Chunk, E>::iterator>
ChunkList<Chunk, E>::find(id_type id) const {
    auto* mutable_this = const_cast<ChunkList<Chunk, E>*>(this);
    auto const& iter = mutable_this->m_byId.find(id);
    auto const found = iter != mutable_this->m_byId.end();
    return {found, found ? iter->second : mutable_this->end()};
}

template<typename Chunk, typename E> inline ChunkList<Chunk, E>::ChunkList(size_t tsize) noexcept :
super(), m_tupleSize(tsize), m_chunkSize(::chunkSize(m_tupleSize)) {}

template<typename Chunk, typename E> inline id_type& ChunkList<Chunk, E>::lastChunkId() {
    return m_lastChunkId;
}

template<typename Chunk, typename E> inline size_t ChunkList<Chunk, E>::tupleSize() const noexcept {
    return m_tupleSize;
}

template<typename Chunk, typename E> inline size_t ChunkList<Chunk, E>::chunkSize() const noexcept {
    return m_chunkSize;
}

template<typename Chunk, typename E> inline size_t ChunkList<Chunk, E>::size() const noexcept {
    return m_size;
}

template<typename Chunk, typename E> inline typename ChunkList<Chunk, E>::iterator const&
ChunkList<Chunk, E>::last() const noexcept {
    return m_back;
}

template<typename Chunk, typename E> inline typename ChunkList<Chunk, E>::iterator&
ChunkList<Chunk, E>::last() noexcept {
    return m_back;
}

template<typename Chunk, typename E> inline void
ChunkList<Chunk, E>::add(typename ChunkList<Chunk, E>::iterator const& iter) {
    m_byAddr.emplace(iter->begin(), iter);
    m_byId.emplace(iter->id(), iter);
}

template<typename Chunk, typename E> inline void ChunkList<Chunk, E>::remove(
        typename ChunkList<Chunk, E>::iterator const& iter) {
    m_byAddr.erase(iter->begin());
    m_byId.erase(iter->id());
}

template<typename Chunk, typename E>
template<typename... Args> inline typename ChunkList<Chunk, E>::iterator
ChunkList<Chunk, E>::emplace_back(Args&&... args) {
    if (super::empty()) {
        super::emplace_front(forward<Args>(args)...);
        m_back = super::begin();
    } else {
        m_back = super::emplace_after(m_back, forward<Args>(args)...);
    }
    add(m_back);
    ++m_size;
    return m_back;
}

template<typename Chunk, typename E> inline void ChunkList<Chunk, E>::pop_front() {
    if (super::empty()) {
        throw underflow_error("pop_front() called on empty chunk list");
    } else {
        vassert(m_byAddr.count(super::cbegin()->begin()));
        vassert(m_byId.count(super::cbegin()->id()));
        m_byAddr.erase(super::cbegin()->begin());
        m_byId.erase(super::cbegin()->id());
        --m_size;
        super::pop_front();
    }
}

template<typename Chunk, typename E> inline void ChunkList<Chunk, E>::pop_back() {
    if (super::empty()) {
        throw underflow_error("pop_back() called on empty chunk list");
    } else {
        auto const iter = find(m_back->id() - 1);
        if (iter.first) {            // original list contains more than 1 nodes
            remove(m_back);
            super::erase_after(m_back = iter.second);
            --lastChunkId();
        } else {
            clear();
        }
    }
}

template<typename Chunk, typename E>
template<typename Pred> inline void ChunkList<Chunk, E>::remove_if(Pred pred) {
    for(auto iter = begin(); iter != end(); ++iter) {
        if (pred(*iter)) {
            vassert(m_byAddr.count(iter->begin()));
            vassert(m_byId.count(iter->id()));
            m_byAddr.erase(iter->begin());
            m_byId.erase(iter->id());
            --m_size;
        } else {                           // since the last node could be invalidated, recalculate it
            m_back = iter;
        }
    }
    super::remove_if(pred);
}

template<typename Chunk, typename E> inline void
ChunkList<Chunk, E>::clear() noexcept {
    m_byId.clear();
    m_byAddr.clear();
    super::clear();
    m_back = end();
    lastChunkId() = 0;
}

template<typename C, typename E> inline
NonCompactingChunks<C, E>::NonCompactingChunks(size_t tupleSize) noexcept : list_type(tupleSize) {}

template<typename C, typename E> inline size_t NonCompactingChunks<C, E>::chunks() const noexcept {
    return ChunkList<C>::size();
}

template<typename C, typename E> inline bool NonCompactingChunks<C, E>::empty() const noexcept {
    return m_allocs == 0;
}

template<typename C, typename E>
inline void* NonCompactingChunks<C, E>::allocate() {
    // Linear search
    auto iter = find_if(list_type::begin(), list_type::end(),
            [](C const& c) noexcept { return ! c.full(); });
    void* r;
    if (iter == list_type::cend()) {        // all chunks are full
        r = list_type::emplace_back(list_type::lastChunkId()++,
                list_type::tupleSize(), list_type::chunkSize())->allocate();
    } else {
        if (iter->empty()) {
            --m_emptyChunks;
        }
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
    auto const iter = list_type::find(src);
    if (! iter.first) {
        snprintf(buf, sizeof buf, "NonCompactingChunks cannot free address %p", src);
        buf[sizeof buf - 1] = 0;
        throw runtime_error(buf);
    } else {
        iter.second->free(src);
        if (iter.second->empty() && ++m_emptyChunks >= CHUNK_REMOVAL_THRESHOLD) {
            list_type::remove_if([](ChunkHolder<> const& c) noexcept { return c.empty(); });
            m_emptyChunks = 0;
        }
        --m_allocs;
    }
}

inline CompactingChunk::CompactingChunk(id_type id, size_t s, size_t s2) : super(id, s, s2) {}

inline void CompactingChunk::free(void* dst, void const* src) {     // cross-chunk free(): update only on dst chunk
    vassert(contains(dst));
    vassert(! contains(src));
    memcpy(dst, src, tupleSize());
}

inline void* CompactingChunk::free(void* dst) {                     // within-chunk free()
    vassert(contains(dst));
    if (reinterpret_cast<char*>(dst) + tupleSize() == next()) {     // last allocation on the chunk
        return free();
    } else {                               // free in the middle
        memcpy(dst, free(), tupleSize());
        return next();
    }
}

inline void* CompactingChunk::free() {                               // within-chunk free() of last allocated
    vassert(next() > begin());
    return reinterpret_cast<char*&>(m_next) -= tupleSize();
}

inline CompactingStorageTrait::CompactingStorageTrait(typename CompactingStorageTrait::list_type* s) noexcept : m_storage(s) {
    vassert(m_storage != nullptr);
}

inline void CompactingStorageTrait::freeze() {
    if (m_frozen) {
        throw logic_error("Double freeze detected");
    } else {
        m_frozen = true;
    }
}

inline void CompactingStorageTrait::thaw() {
    if (m_frozen) {                        // release all chunks invisible to txn
        if (! m_storage->empty()) {
            auto const& beginTxn = reinterpret_cast<CompactingChunks const*>(m_storage)->beginTxn();
            bool const empty = beginTxn.empty();
            auto const stop = empty ? 0 : beginTxn.iterator()->id();
            while (! m_storage->empty() && (empty || less_rolling(m_storage->front().id(), stop))) {
                m_storage->pop_front();
            }
        }
        m_frozen = false;
    } else {
        throw logic_error("Double thaw detected");
    }
}

inline bool CompactingStorageTrait::frozen() const noexcept {
    return m_frozen;
}

inline void CompactingStorageTrait::release(
        typename CompactingStorageTrait::list_type::iterator iter, void const* p) {
    if (m_frozen && less_rolling(iter->id(),
                reinterpret_cast<CompactingChunks const*>(m_storage)->beginTxn().iterator()->id()) &&
            reinterpret_cast<char const*>(p) + iter->tupleSize() >= iter->end()) {
        vassert(iter == m_storage->begin());
        m_storage->pop_front();
    }
}

inline typename CompactingStorageTrait::list_type::iterator CompactingStorageTrait::releasable(
        typename CompactingStorageTrait::list_type::iterator iter) {
    if (iter->empty()) {
        auto iter_next = next(iter);
        if (! m_frozen) {                // safe to erase a Chunk unless frozen
            vassert(iter == m_storage->begin());
            m_storage->pop_front();
        }
        return iter_next;
    } else {
        return iter;
    }
}

CompactingChunks::CompactingChunks(size_t tupleSize) noexcept :
    list_type(tupleSize), CompactingStorageTrait(this), m_txnFirstChunk(*this), m_batched(*this) {}

inline CompactingChunks::TxnLeftBoundary::TxnLeftBoundary(ChunkList<CompactingChunk>& chunks) noexcept :
    m_chunks(chunks), m_iter(chunks.end()), m_next(nullptr) {
    vassert(m_chunks.empty());
}

inline typename ChunkList<CompactingChunk>::iterator const& CompactingChunks::TxnLeftBoundary::iterator() const noexcept {
    return m_iter;
}

inline typename ChunkList<CompactingChunk>::iterator& CompactingChunks::TxnLeftBoundary::iterator() noexcept {
    return m_iter;
}

inline typename ChunkList<CompactingChunk>::iterator const& CompactingChunks::TxnLeftBoundary::iterator(
        typename ChunkList<CompactingChunk>::iterator const& iter) noexcept {
    m_next = m_chunks.end() == iter ? nullptr : iter->next();
    return m_iter = iter;
}

inline void const*& CompactingChunks::TxnLeftBoundary::next() noexcept {
    return m_next;
}

inline bool CompactingChunks::TxnLeftBoundary::empty() const noexcept {
    return m_next == nullptr;
}

inline id_type CompactingChunks::id() const noexcept {
    return m_id;
}

size_t CompactingChunks::size() const noexcept {
    return m_allocs;
}

size_t CompactingChunks::chunks() const noexcept {
    return list_type::size();
}

size_t CompactingChunks::chunkSize() const noexcept {
    return list_type::chunkSize();
}

inline pair<bool, typename CompactingChunks::list_type::iterator>
CompactingChunks::find(void const* p) noexcept {
    auto const iter = list_type::find(p);
    return ! iter.first ||
        less<CompactingChunks::list_type::iterator>()(iter.second, beginTxn().iterator()) ?
        make_pair(false, list_type::end()) :
        iter;
}

inline pair<bool, typename CompactingChunks::list_type::iterator>
CompactingChunks::find(id_type id) noexcept {
    auto const iter = list_type::find(id);
    return ! iter.first ||
        less<CompactingChunks::list_type::iterator>()(iter.second, beginTxn().iterator()) ?
        make_pair(false, list_type::end()) : iter;
}

inline typename CompactingChunks::list_type::iterator CompactingChunks::releasable() {
    return beginTxn().iterator(CompactingStorageTrait::releasable(beginTxn().iterator()));
}

inline void CompactingChunks::pop_front() {
    list_type::pop_front();
    beginTxn().iterator(begin());
}

inline void CompactingChunks::pop_back() {
    list_type::pop_back();
    if (empty()) {
        beginTxn().iterator(end());
    }
}

inline void CompactingChunks::freeze() {
    CompactingStorageTrait::freeze();
    if (last() != end()) {
        m_frozenTxnBoundaries = *this;
    }
}

inline void CompactingChunks::thaw() {
    m_frozenTxnBoundaries.clear();
    CompactingStorageTrait::thaw();
}

template<typename Remove_cb>
inline void CompactingChunks::clear(Remove_cb const& cb) {
    if (m_lastFreeFromHead != nullptr) {
        throw logic_error("Unfinished free(remove_direction::from_head, ?)");
    } else if (! m_batched.empty()) {
        throw logic_error("Unfinished free_add(?)");
    } else if (frozen()) {                        // slow clear path
        // first, apply call back on all txn tuples (in order)
        fold<IterableTableTupleChunks<CompactingChunks, truth>::const_iterator>(
                static_cast<CompactingChunks const&>(*this),
                [&cb] (void const* p) noexcept {cb(p);});
        // since the last chunk may not be full, we need to restore its pointer later.
        // This is a hack to get the correct behavior for
        // snapshot -> clear -> finish snapshot
        void* last_next = last()->next();
        // then, release all chunks from txn view
        while (! beginTxn().empty()) {
            beginTxn().next() = beginTxn().iterator()->m_next = beginTxn().iterator()->begin();
            releasable();
        }
        last()->m_next = last_next;
        m_allocs = 0;
    } else {                               // fast clear path
        list_type::clear();
        m_allocs = 0;
        m_txnFirstChunk.iterator(list_type::begin());
    }
}

inline void* CompactingChunks::allocate() {
    void* r;
    if (empty() || last()->full()) {
        // Under some circumstances (e.g. truncation when frozen, followed by
        // allocation (still frozen)), the inserted chunk is not
        // the only chunk.
        auto const& beg = emplace_back(lastChunkId()++,
                list_type::tupleSize(), list_type::chunkSize());
        r = beg->allocate();
        if (empty()) {
            beginTxn().iterator(beg);
        }
    } else {
        r = last()->allocate();
        if (last()->id() == beginTxn().iterator()->id()) {
            beginTxn().next() = last()->next();
        }
    }
    ++m_allocs;
    return r;
}

inline bool CompactingChunks::empty() const noexcept {
    return beginTxn().empty();
}

void* CompactingChunks::free(void* dst) {
    auto pos = find(dst);                 // binary search in txn region
    if (! pos.first) {
        if (cbegin()->next() == dst) {
            // When shrinking from head, since e.g. for_each<iterator_type>(...)
            // retrieves the address, advances iterator, then calls callable,
            // it is possible that the advanced address is invalidated if it is
            // (effectively) removed by the memory movement. When
            // this occurs, current free() call is an no-op
            return nullptr;
        } else {
            snprintf(buf, sizeof buf, "CompactingChunks::free(%p): invalid address.", dst);
            buf[sizeof buf - 1] = 0;
            throw range_error(buf);
        }
    } else {
        void* src = beginTxn().iterator()->free();
        auto& dst_iter = pos.second;
        if (dst_iter != beginTxn().iterator()) {    // cross-chunk movement needed
            dst_iter->free(dst, src);        // memcpy()
        } else if (src != dst) {             // within-chunk movement (not happened in the previous free() call)
            memcpy(dst, src, tupleSize());
        }
        releasable();
        --m_allocs;
        return src;
    }
}

namespace std {                                    // Need to declare these before using (for Mac clang++)
    using namespace voltdb::storage;
    template<> struct less<position_type> {
        inline bool operator()(position_type const& lhs, position_type const& rhs) const noexcept {
            bool const e1 = lhs.empty(), e2 = rhs.empty();
            if (e1 || e2) { // NOTE: anything but empty < empty, and empty < anything but empty.
                return ! (e1 && e2);               // That is, empty !< empty (since empty == empty)
            } else {
                auto const id1 = lhs.chunkId(), id2 = rhs.chunkId();
                return (id1 == id2 && lhs.address() < rhs.address()) || less_rolling(id1, id2);
            }
        }
    };
    template<> struct less_equal<position_type> {
        inline bool operator()(position_type const& lhs, position_type const& rhs) const noexcept {
            return ! less<position_type>()(rhs, lhs);
        }
    };
    template<> struct less<ChunkHolder<>> {
        inline bool operator()(ChunkHolder<> const& lhs, ChunkHolder<> const& rhs) const noexcept {
            return less_rolling(lhs.id(), rhs.id());
        }
    };
}

ChunksIdValidatorImpl ChunksIdValidatorImpl::s_singleton{};
/**
 * The implementation just forward IteratorPermissible
 */
inline bool ChunksIdValidatorImpl::validate(id_type id) {
    lock_guard<mutex> g{m_mapMutex};
    auto const& iter = m_inUse.find(id);
    if (iter == m_inUse.end()) {            // add entry
        m_inUse.emplace_hint(iter, id);
        return true;
    } else {
        snprintf(buf, sizeof buf,
                "Cannot create RW snapshot iterator on chunk list id %lu",
                static_cast<size_t>(id));
        buf[sizeof buf - 1] = 0;
        throw logic_error(buf);
    }
}

inline bool ChunksIdValidatorImpl::remove(id_type id) {
    // TODO: we need to also guard against "double deletion" case;
    // but eecheck is currently failing mysteriously
    lock_guard<mutex> g{m_mapMutex};
    m_inUse.erase(id);
    return true;
}

inline ChunksIdValidatorImpl& ChunksIdValidatorImpl::instance() {
    return s_singleton;
}

inline id_type ChunksIdValidatorImpl::id() {
    return m_id++;
}

ChunksIdNonValidator ChunksIdNonValidator::s_singleton{};

inline ChunksIdNonValidator& ChunksIdNonValidator::instance() {
    return s_singleton;
}

inline void CompactingChunks::free(typename CompactingChunks::remove_direction dir, void const* p) {
    switch (dir) {
        case remove_direction::from_head:
            // Schema change: it is called in the same
            // order as txn iterator.
            //
            // NOTE: since we only do chunk-wise drop, any reads
            // from the allocator when these dispersed calls are
            // occurring *will* get spurious data.
            if (p == nullptr) {                         // marks completion
                if (m_lastFreeFromHead != nullptr && beginTxn().iterator()->contains(m_lastFreeFromHead)) {
                    // effects deletions in 1st chunk
                    vassert(reinterpret_cast<char const*>(beginTxn().next()) >= m_lastFreeFromHead + tupleSize());
                    auto const offset = reinterpret_cast<char const*>(beginTxn().next()) - m_lastFreeFromHead - tupleSize();
                    if (offset) {                              // some memory ops are unavoidable
                        char* dst = reinterpret_cast<char*>(beginTxn().iterator()->begin());
                        char const* src = reinterpret_cast<char const*>(beginTxn().next()) - offset;
                        if (dst + offset < src) {
                            memcpy(dst, src, offset);
                        } else {
                            memmove(dst, src, offset);
                        }
                        const_cast<char*&>(reinterpret_cast<char const*&>(beginTxn().next())) =
                            reinterpret_cast<char*&>(beginTxn().iterator()->m_next) =
                            dst + offset;
                    } else {                                   // right on the boundary
                        pop_front();
                    }
                    m_lastFreeFromHead = nullptr;
                }
            } else if (empty()) {
                snprintf(buf, sizeof buf, "CompactingChunks::remove(from_head, %p): empty allocator", p);
                buf[sizeof buf - 1] = 0;
                throw underflow_error(buf);
            } else {
                vassert((m_lastFreeFromHead == nullptr && p == beginTxn().iterator()->begin()) ||       // called for the first time?
                        (beginTxn().iterator()->contains(p) && m_lastFreeFromHead + tupleSize() == p) ||// same chunk,
                        next(beginTxn().iterator())->begin() == p);                                     // or next chunk
                if (! beginTxn().iterator()->contains(m_lastFreeFromHead = reinterpret_cast<char const*>(p))) {
                    pop_front();
                }
                --m_allocs;
            }
            break;
        case remove_direction::from_tail:
            // Undo insert: it is called in the exactly
            // opposite order of txn iterator.
            if (empty()) {
                snprintf(buf, sizeof buf, "CompactingChunks::remove(from_tail, %p): empty allocator", p);
                buf[sizeof buf - 1] = 0;
                throw underflow_error(buf);
            } else {
                vassert(reinterpret_cast<char const*>(p) + tupleSize() == last()->next());
                if (last()->begin() == (last()->m_next = const_cast<void*>(p))) { // delete last chunk
                    pop_back();
                }
                vassert(! frozen() || empty() ||
                        less_equal<position_type>()(m_frozenTxnBoundaries.right(), *last()));
                --m_allocs;
            }
            break;
        default:;
    }
}

inline typename CompactingChunks::TxnLeftBoundary const& CompactingChunks::beginTxn() const noexcept {
    return m_txnFirstChunk;
}

inline typename CompactingChunks::TxnLeftBoundary& CompactingChunks::beginTxn() noexcept {
    return m_txnFirstChunk;
}

inline CompactingChunks::FrozenTxnBoundaries::FrozenTxnBoundaries(ChunkList<CompactingChunk> const& l) noexcept {
    if (! l.empty()) {
        m_left = *l.begin();
        m_right = *l.last();
    }
}

inline void CompactingChunks::FrozenTxnBoundaries::clear() {
    m_left = {};
    m_right = {};
}

inline position_type const& CompactingChunks::FrozenTxnBoundaries::left() const noexcept {
    return m_left;
}

inline position_type const& CompactingChunks::FrozenTxnBoundaries::right() const noexcept {
    return m_right;
}

inline typename CompactingChunks::FrozenTxnBoundaries const& CompactingChunks::frozenBoundaries() const noexcept {
    return m_frozenTxnBoundaries;
}

/**
 * B+ tree does not allow in-place modification of "data" value
 * of an iterator, unlike std::map
 */
template<typename Iter, collections_enum_type E> struct BatchRemoveMapValueRetriever {
    inline vector<void*>& operator()(Iter& iter) const noexcept {
        return iter->second;
    }
};

template<typename Iter> struct BatchRemoveMapValueRetriever<Iter, collections_enum_type::stx_collections> {
    inline vector<void*>& operator()(Iter& iter) const noexcept {
        return iter.data();
    }
};

inline CompactingChunks::DelayedRemover::RemovableRegion::RemovableRegion(
        char const* next, size_t tupleSize, size_t n) noexcept : m_beg(next - tupleSize * n), m_mask(n) {
    // bitset: false => taken (remove requested); true => untaken (hole)
    m_mask.set();
}

inline char const* CompactingChunks::DelayedRemover::RemovableRegion::begin() const noexcept {
    return m_beg;
}

inline typename CompactingChunks::DelayedRemover::RemovableRegion::bitset_t&
CompactingChunks::DelayedRemover::RemovableRegion::mask() noexcept {
    return m_mask;
}

inline typename CompactingChunks::DelayedRemover::RemovableRegion::bitset_t const&
CompactingChunks::DelayedRemover::RemovableRegion::mask() const noexcept {
    return m_mask;
}

inline vector<void*> CompactingChunks::DelayedRemover::RemovableRegion::holes(size_t tupleSize) const noexcept {
    vector<void*> r(mask().count(), nullptr);
    size_t h_index = 0, m_index = 0;
    if (mask().test(m_index)) {
        r[h_index++] = const_cast<char*>(begin());
    }
    while ((m_index = mask().find_next(m_index)) != bitset_t::npos) {
        r[h_index++] = const_cast<char*>(begin()) + tupleSize * m_index;
    }
    vassert(r.size() == mask().count());
    return r;
}

inline CompactingChunks::DelayedRemover::DelayedRemover(CompactingChunks& s) : m_chunks(s) {}

inline void CompactingChunks::DelayedRemover::reserve(size_t n) {
    if (m_chunks.empty()) {
        throw underflow_error("CompactingChunks is empty, cannot reserve for batch removal");
    } else if (m_size > 0) {
        throw logic_error("Double reserve called on CompactingChunks::DelayedRemover::reserve()");
    } else {
        vassert(m_removedRegions.empty() && m_moved.empty() && m_movements.empty() && m_removed.empty());
        m_size = n;
        auto iter = m_chunks.beginTxn().iterator();
        auto const chunkSize = m_chunks.chunkSize(),
             tupleSize = m_chunks.tupleSize(),
             tuplesPerChunk = chunkSize / tupleSize;
        auto const offset = reinterpret_cast<char const*>(iter->next()) -
            reinterpret_cast<char const*>(iter->begin());
        auto chunksTBReleasedFull = n / tuplesPerChunk,
             tuplesTBReleasedPartial = n % tuplesPerChunk;
        if (tuplesTBReleasedPartial * tupleSize >= offset) {
            ++chunksTBReleasedFull;
            tuplesTBReleasedPartial -= offset / tupleSize;
        } else if (chunksTBReleasedFull) {
            tuplesTBReleasedPartial = (m_size - offset / tupleSize) % tuplesPerChunk;
        }
        for (size_t i = 0; i < chunksTBReleasedFull; ++i) {
            auto *beg = reinterpret_cast<char const*>(iter->begin()),
                 *end = reinterpret_cast<char const*>(iter->next());
            m_removedRegions.emplace(iter->id(),
                    RemovableRegion{end, tupleSize, i == 0 ? (end - beg) / tupleSize : tuplesPerChunk});
            if (++iter == m_chunks.end() && i + 1 == chunksTBReleasedFull && tuplesTBReleasedPartial) {
                snprintf(buf, sizeof buf,
                        "CompactingChunks::DelayedRemover::reserve(%lu): insufficient space to be reserved", n);
                buf[sizeof buf - 1] = 0;
                throw underflow_error(buf);
            }
        }
        if (tuplesTBReleasedPartial > 0) {
            if (iter == m_chunks.end()) {
                snprintf(buf, sizeof buf,
                        "CompactingChunks::DelayedRemover::reserve(%lu): insufficient space to be reserved", n);
                buf[sizeof buf - 1] = 0;
                throw underflow_error(buf);
            }
            m_removedRegions.emplace(iter->id(), RemovableRegion{
                    reinterpret_cast<char const*>(iter->next()),
                    tupleSize, tuplesTBReleasedPartial});
        }
    }
}

inline void CompactingChunks::DelayedRemover::add(void* p) {
    if (m_size == 0) {
        throw underflow_error("CompactingChunks::DelayedRemover::add() called more times than reserved");
    } else {
        auto const iter = m_chunks.find(p);
        if (! iter.first) {
            snprintf(buf, sizeof buf, "CompactingChunk::DelayedRemover::add(%p): invalid address", p);
            buf[sizeof buf - 1] = 0;
            throw range_error(buf);
        } else {
            auto const removed_iter = m_removedRegions.find(iter.second->id());
            auto const tupleSize = m_chunks.tupleSize();
            RemovableRegion* region = nullptr;
            if (removed_iter == m_removedRegions.end() ||
                    ((region = &removed_iter->second) &&
                     (p >= region->begin() + tupleSize * region->mask().size() ||
                      p < region->begin()))) {
                m_moved.emplace_back(p);
            } else {
                m_removed.emplace_back(p);
                region = &removed_iter->second;
                auto const offset =
                    (reinterpret_cast<char*>(p) - region->begin()) / tupleSize;
                vassert(region->mask().test(offset));
                region->mask().reset(offset);
            }
            if (--m_size == 0) {
                mapping();
            }
        }
    }
}

inline void CompactingChunks::DelayedRemover::validate() const {
    if (m_size > 0) {
        snprintf(buf, sizeof buf,
                "Cannot force batch removal with insufficient addresses: "
                "still missing %lu addresses", m_size);
        buf[sizeof buf - 1] = 0;
        throw logic_error(buf);
    }
}

inline void CompactingChunks::DelayedRemover::mapping() {
    vassert(m_movements.empty());
    auto const len = m_moved.size();
    if (len > 0) {
        m_movements.reserve(len);
        auto const tupleSize = m_chunks.tupleSize();
        if (accumulate(m_removedRegions.cbegin(), m_removedRegions.cend(), 0lu,
                [len, tupleSize, this](size_t n, typename map_type::value_type const& kv) {
                  if (n < len) {
                      auto const& h = kv.second.holes(tupleSize);
                      if (n + h.size() > len) {
                          throw overflow_error(
                                  "CompactingChunks::DelayedRemover::mapping(): "
                                  "found more holes than expected");
                      } else {
                          transform(h.cbegin(), h.cend(), next(m_moved.cbegin(), n),
                                  back_inserter(m_movements),
                                  [](void* r, void* l) noexcept { return make_pair(l, r); });
                          return n + h.size();
                      }
                  } else {
                      vassert(n == len);
                      return n;
                  }
                }) < m_moved.size()) {
            throw underflow_error("CompactingChunks::DelayedRemover::mapping(): "
                    "insufficient holes");
        }
    }
}

inline void CompactingChunks::DelayedRemover::shift() {
    if (! m_removedRegions.empty()) {
        std::for_each(m_removedRegions.cbegin(), prev(m_removedRegions.cend()),
                [this](typename map_type::value_type const& entry) {
                    auto& iter = m_chunks.beginTxn().iterator();
                    vassert(iter->id() == entry.first);
                    reinterpret_cast<char*&>(iter->m_next) = reinterpret_cast<char*>(iter->begin());
                    m_chunks.releasable();
                });
        auto last = m_chunks.find(m_removedRegions.rbegin()->first);
        vassert(last.first);
        vassert(reinterpret_cast<char const*>(last.second->next()) -
                reinterpret_cast<char const*>(last.second->begin()) >=
                m_chunks.tupleSize() * m_removedRegions.rbegin()->second.mask().size());
        reinterpret_cast<char*&>(last.second->m_next) -=
            m_chunks.tupleSize() * m_removedRegions.rbegin()->second.mask().size();
        m_chunks.releasable();
        m_chunks.m_allocs -= m_removed.size() + m_moved.size();
    }
}

inline vector<pair<void*, void*>> const& CompactingChunks::DelayedRemover::movements() const {
    validate();
    return m_movements;
}

inline vector<void*> const& CompactingChunks::DelayedRemover::removed() const {
    validate();
    return m_removed;
}

inline size_t CompactingChunks::DelayedRemover::clear() noexcept {
    vassert(m_size == 0);
    auto const r = m_removed.size() + m_moved.size();
    m_moved.clear();
    m_removed.clear();
    m_movements.clear();
    m_removedRegions.clear();
    return r;
}

inline size_t CompactingChunks::DelayedRemover::force() {
    validate();
    shift();
    return clear();
}

inline bool CompactingChunks::DelayedRemover::empty() const noexcept {
    return m_size == 0;
}

template<typename Chunks, typename Tag, typename E> Tag IterableTableTupleChunks<Chunks, Tag, E>::s_tagger{};

template<typename Cont, iterator_permission_type perm, iterator_view_type view, typename = typename Cont::Compact>
struct iterator_begin {
    using iterator = typename conditional<perm == iterator_permission_type::ro,
          typename Cont::const_iterator, typename Cont::iterator>::type;
    iterator operator()(Cont& c) const noexcept {
        return c.begin();
    }
};

template<typename Cont, iterator_permission_type perm>
struct iterator_begin<Cont, perm, iterator_view_type::txn, true_type> {
    using iterator = typename conditional<perm == iterator_permission_type::ro,
          typename Cont::const_iterator, typename Cont::iterator>::type;
    iterator operator()(Cont& c) const noexcept {
        return c.beginTxn().iterator();
    }
};

template<typename Chunks, iterator_view_type view, typename = typename Chunks::Compact>
struct HasTxnInvisibleChunks {
    inline constexpr bool operator()(Chunks const&) const noexcept {
        return false;
    }
};

template<typename Chunks>
struct HasTxnInvisibleChunks<Chunks, iterator_view_type::snapshot, true_type> {
    inline bool operator()(Chunks const& c) const noexcept {
        return c.frozen() && (c.empty() ||
                less_rolling(c.frozenBoundaries().left().chunkId(), c.begin()->id()));
    }
};

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type src) :
    m_offset(src.tupleSize()), m_storage(src),
    m_iter(iterator_begin<typename remove_reference<container_type>::type, perm, view>()(src)),
    m_hasTxnInvisibleChunks(HasTxnInvisibleChunks<Chunks, view>()(src)),
    m_cursor(const_cast<value_type>(m_iter == m_storage.end() ? nullptr : m_iter->begin())) {
    // paranoid type check
    static_assert(is_lvalue_reference<container_type>::value,
            "IterableTableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "IterableTableTupleChunks::value_type is not a pointer");
    constructible_type::instance().validate(src.id());
    while (m_cursor != nullptr && ! s_tagger(m_cursor)) {
        advance();         // calibrate to first non-skipped position
    }
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::~iterator_type() {
    constructible_type::instance().remove(static_cast<container_type>(m_storage).id());
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::storage() const noexcept {
    static_assert(is_lvalue_reference<container_type>::value, "container_type must be reference type");
    return reinterpret_cast<container_type>(m_storage);
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::list_iterator_type const&
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::list_iterator() const noexcept {
    return m_iter;
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::list_iterator_type&
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::list_iterator() noexcept {
    return m_iter;
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
inline bool IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::operator==(
        iterator_type<perm, view> const& o) const noexcept {
    return m_cursor == o.m_cursor;
}


template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::operator position_type() const noexcept {
    return {m_cursor, m_iter};
}

/**
 * Helper to check if the iterator needs to advance to next
 * chunk. The only special occasion is snapshot view of
 * head-compacting, when there are tuple deletions occurring
 * during snapshot.
 */
template<typename ChunkList, typename Iter, iterator_view_type view, typename Comp>
struct ChunkBoundary {
    inline void const* operator()(ChunkList const&, Iter const& iter, bool) const noexcept {
        return iter->next();
    }
};

template<typename ChunkList, typename Iter>
struct ChunkBoundary<ChunkList, Iter, iterator_view_type::snapshot, true_type> {
    inline void const* operator()(ChunkList const& l, Iter const& iter, bool hasTxnInvisibleChunks) const noexcept {
        auto const& frozenBoundaries = reinterpret_cast<CompactingChunks const&>(l).frozenBoundaries();
        if (frozenBoundaries.left().empty()) {        // not frozen
            return iter->next();
        } else {
            auto const& beginTxn = reinterpret_cast<CompactingChunks const&>(l).beginTxn();
            auto const leftId = frozenBoundaries.left().chunkId(),
                 rightId = frozenBoundaries.right().chunkId(),
                 txnBeginChunkId = beginTxn.empty() ? 0 : beginTxn.iterator()->id(),
                 iterId = iter->id();
            if (beginTxn.empty()) {                // txn view is empty
                // Under this circumstances, since the last chunk
                // in txn view may not be full when it is
                // frozen and clear()-ed, only the last chunk
                // needs to use the next() position.
                vassert((iter->begin() == iter->next()) == (next(iter) != l.end()));
                return iter->begin() == iter->next() ? iter->end() : iter->next();
            } else if(less_rolling(iterId, txnBeginChunkId)) {  // in chunk visible to frozen iterator only
                if (less_rolling(iterId, rightId)) {
                    return iter->end();
                } else {
                    vassert(iterId == rightId);
                    return frozenBoundaries.right().address();
                }
            } else if (leftId == iterId) {      // in the left boundary of frozen state
                return hasTxnInvisibleChunks ? iter->end() : frozenBoundaries.left().address();
            } else if (rightId == iterId) {     // in the right boundary
                return frozenBoundaries.right().address();
            } else if (txnBeginChunkId == iterId) {
                return iter->end();
            } else {
                return iter->next();
            }
        }
    }
};

/**
 * Action when iterator is done with current chunk.
 * When using snapshot RW iterator on compacting chunks, this means
 * releasing the chunk (to OS).
 */
template<typename ChunkList, typename Iter, iterator_permission_type perm, iterator_view_type view, typename Comp>
struct ChunkDeleter {
    inline void operator()(ChunkList const&, Iter& iter) const noexcept {
        ++iter;
    }
};

template<typename ChunkList, typename Iter>
struct ChunkDeleter<ChunkList, Iter, iterator_permission_type::rw, iterator_view_type::snapshot, true_type> {
    inline void operator()(ChunkList& l, Iter& iter) const noexcept {
        if (reinterpret_cast<CompactingChunks const&>(l).frozen() &&
                less<Iter>()(iter, reinterpret_cast<CompactingChunks const&>(l).beginTxn().iterator())) {
            vassert(l.front().id() == iter->id());
            ++iter;
            l.pop_front();
        } else {
            ++iter;
        }
    }
};

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
void IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::advance() {
    static constexpr ChunkBoundary<list_type, decltype(m_iter), view, typename Chunks::Compact> const boundary{};
    static constexpr ChunkDeleter<list_type, decltype(m_iter), perm, view, typename Chunks::Compact> const advance_iter{};
    if (! drained()) {
        // Need to maintain invariant that m_cursor is nullptr iff
        // iterator points to end().
        bool finished = true;
        // we need to check emptyness since iterator could be
        // invalidated when non-lazily releasing the only available chunk.
        if (! m_storage.empty() && m_iter != m_storage.end()) {
            const_cast<void*&>(m_cursor) =
                reinterpret_cast<char*>(const_cast<void*>(m_cursor)) + m_offset;
            if (m_cursor < boundary(m_storage, m_iter, m_hasTxnInvisibleChunks)) {
                finished = false;              // within chunk
            } else {
                advance_iter(m_storage, m_iter); // cross chunk
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
    typename remove_reference<decltype(*this)>::type const copy(*this);
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

template<typename Chunks, typename Tag, typename E> inline
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::elastic_iterator(
        typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::container_type c) :
    super(c), m_empty(c.empty()),
    m_txnBoundary(m_empty ? decltype(m_txnBoundary){} : *c.last()),
    m_chunkId(m_empty ? 0 : super::list_iterator()->id()) {}

template<typename Chunks, typename Tag, typename E> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::container_type c) {
    return {c};
}

// Helper for elastic_iterator::refresh() on compacting chunks.
// 1. Since C++17, we will be able to inline these less-graceful
// workarounds with if-constexpr.
// 2. A even better solution is to use type refinement of
// IterableTableTupleChunks, but that would be even more heavy
// weight.
template<typename IterableTableTupleChunks, typename ElasticIterator,
    typename Compact = typename IterableTableTupleChunks::chunk_type::Compact>
struct ElasticIterator_refresh {
    inline void operator()(ElasticIterator const&, bool, id_type const&,
            typename IterableTableTupleChunks::chunk_type const&,
            typename IterableTableTupleChunks::chunk_type::iterator const&, position_type const&,
            typename IterableTableTupleChunks::chunk_type::list_type::const_iterator&,
            void const*&) const {
        throw logic_error("elastic_iterator can only iterate over compacting chunks");
    }
};

template<typename I, typename ElasticIterator>
struct ElasticIterator_refresh<I, ElasticIterator, true_type> {
    inline void operator()(ElasticIterator& iter, bool& isEmpty, id_type& chunkId,
            typename I::chunk_type const& storage,
            typename I::chunk_type::iterator const& last, position_type const& boundary,
            ChunkList<CompactingChunk>::const_iterator& chunkIter,
            void const*& cursor) const {
        if (isEmpty) {                             // last time checked, allocator was empty
            isEmpty = storage.empty();             // check again,
            if (! isEmpty) {   // if it has something now, set cursor to 1st
                chunkIter = storage.beginTxn().iterator();
                chunkId = chunkIter->id();
                cursor = chunkIter->begin();
                const_cast<position_type&>(boundary) = *last;
            }
        } else if (! iter.drained()) {
            auto const& indexBeg = storage.beginTxn().iterator();
            if (less_rolling(chunkId, indexBeg->id())) {
                // current chunk list iterator is stale
                chunkId = (chunkIter = indexBeg)->id();
                cursor = chunkIter->begin();
            } else if (! chunkIter->contains(cursor)) {
                // Current chunk has been partially compacted,
                // to the extent that cursor position is stale
                cursor =
                    ++chunkIter == storage.end() ||
                    less<position_type>()(iter.txnBoundary(), iter) ?        // drained
                    nullptr : chunkIter->begin();
            }
        }
    }
};

template<typename Chunks, typename Tag, typename E> inline void
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::refresh() {
    static constexpr ElasticIterator_refresh<
        IterableTableTupleChunks<Chunks, Tag, E>,
        typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator> refresher{};
    refresher(*this, m_empty, m_chunkId, super::storage(), super::storage().last(), m_txnBoundary,
            super::list_iterator(), super::m_cursor);
}

template<typename Chunks, typename Tag, typename E> inline position_type const&
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::txnBoundary() const noexcept {
    return m_txnBoundary;
}

template<typename Chunks, typename Tag, typename E> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::super::value_type
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::operator*() {
    refresh();
    return super::operator*();
}

template<typename Chunks, typename Tag, typename E> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator&
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::operator++() {
    refresh();
    super::advance();
    if (! drained()) {
        m_chunkId = super::list_iterator()->id();
    }
    return *this;
}

template<typename Chunks, typename Tag, typename E> inline
typename IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::operator++(int) {
    typename remove_reference<decltype(*this)>::type const copy(*this);
    refresh();
    super::advance();
    if (! drained()) {
        m_chunkId = super::list_iterator()->id();
    }
    return copy;
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::iterator
IterableTableTupleChunks<Chunks, Tag, E>::begin(Chunks& c) {
    return iterator::begin(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E>::cbegin(Chunks const& c) {
    return const_iterator::begin(c);
}

template<typename Chunks, typename Tag, typename E>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::const_iterator
IterableTableTupleChunks<Chunks, Tag, E>::begin(Chunks const& c) {
    return cbegin(c);
}

template<typename Chunks, typename Tag, typename E>
template<typename Trans, iterator_permission_type perm>
inline IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<Trans, perm>::iterator_cb_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<Trans, perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<Trans, perm>::cb_type cb) :
    super(c), m_cb(cb) {}

template<typename Chunks, typename Tag, typename E>
template<typename Trans, iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<Trans, perm>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<Trans, perm>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<Trans, perm>::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<Trans, perm>::cb_type cb) {
    return {c, cb};
}

template<typename Chunks, typename Tag, typename E>
template<typename Trans, iterator_permission_type perm>
inline typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_cb_type<Trans, perm>::value_type
IterableTableTupleChunks<Chunks, Tag, E>::iterator_cb_type<Trans, perm>::operator*() noexcept {
    return const_cast<void*>(m_cb(super::operator*()));
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm> inline
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::hooked_iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>::container_type c) :
super(c, c) {}

inline position_type::position_type(ChunkHolder<> const& c) noexcept : m_chunkId(c.id()), m_addr(c.next()) {}

inline position_type::position_type(CompactingChunks const& c, void const* p) : m_addr(p) {
    auto const iter = const_cast<CompactingChunks&>(c).find(p);
    if (! iter.first || ! (iter.second->contains(p) || iter.second->end() > p)) {
        // NOTE: it is possible that the txn view does not
        // contain the ptr, as the chunk has been removed. In
        // that case, we simply "empty" it, and note the
        // semantics of less<position_type>.
        const_cast<void*&>(m_addr) = nullptr;
    } else {
        const_cast<remove_const<decltype(m_chunkId)>::type&>(m_chunkId) = iter.second->id();
    }
}

template<typename iterator>
inline position_type::position_type(void const* p, iterator const& iter) noexcept :
m_chunkId(iter->id()), m_addr(p) {}

inline id_type position_type::chunkId() const noexcept {
    return m_chunkId;
}
inline void const* position_type::address() const noexcept {
    return m_addr;
}
inline bool position_type::empty() const noexcept {
    return m_addr == nullptr;
}
inline bool position_type::operator==(position_type const& o) const noexcept {
    return m_addr == o.address();                                      // optimized comparison
}

inline position_type& position_type::operator=(position_type const& o) noexcept {
    const_cast<id_type&>(m_chunkId) = o.chunkId();
    m_addr = o.address();
    return *this;
}

template<typename Chunks, typename Tag, typename E> inline bool
IterableTableTupleChunks<Chunks, Tag, E>::elastic_iterator::drained() noexcept {
    if (super::drained()) {
        return true;
    } else {
        auto const& s = super::storage();
        if (s.empty() ||
                less_rolling(s.last()->id(), m_chunkId) ||             // effectively less<position_type>()(*s.last(), *this);
                (s.last()->id() == m_chunkId && s.last()->next() <= super::m_cursor) ||    // but that could cause use-after-release
                less_equal<position_type>()(m_txnBoundary, *this)) {
            super::m_cursor = nullptr;
            return true;
        } else {
            return false;
        }
    }
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm> inline typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::begin(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template hooked_iterator_type<perm>::container_type c) {
    return {c};
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm> inline bool
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::drained() const noexcept {
    return super::drained() || (super::storage().frozen() &&
            (super::storage().frozenBoundaries().right().address() == super::m_cursor ||
             less<position_type>()(super::storage().frozenBoundaries().right(), *this)));
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
    if (++m_size == static_cast<unsigned char>(gc_policy::batched)) {
        for_each(m_batched.begin(), m_batched.end(), m_cb);
        m_batched.clear();
        m_size = 0;
    } else {
        m_batched.emplace_front(addr);
    }
}

template<typename Alloc, typename Trait, typename E>
inline TxnPreHook<Alloc, Trait, E>::TxnPreHook(size_t tupleSize) :
    Trait([this](void const* key) {
                auto const& iter = m_changes.find(key);
                if (iter != m_changes.end()) {
                    m_changes.erase(iter);
                    m_copied.erase(key);
                    m_storage.free(const_cast<void*>(key));
                }
            }),
    m_storage(tupleSize) {}

template<typename Alloc, typename Trait, typename E> inline bool const&
TxnPreHook<Alloc, Trait, E>::hasDeletes() const noexcept {
    return m_hasDeletes;
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::copy(void const* p) {     // API essential
    if (m_recording && ! m_changes.count(p)) {                        // make a copy only if the addr to be
        if (m_last == nullptr) {                                      // overwritten hadn't been logged
            m_last = m_storage.allocate();
            vassert(m_last != nullptr);
        }
        memcpy(m_last, p, m_storage.tupleSize());
    }
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::add(CompactingChunks const& t,
        typename TxnPreHook<Alloc, Trait, E>::ChangeType type, void const* dst) {
    if (m_recording) {     // ignore changes beyond boundary
        switch (type) {
            case ChangeType::Update:
                update(dst);
                break;
            case ChangeType::Insertion:
                insert(dst);
                break;
            case ChangeType::Deletion:
            default:
                remove(dst);
        }
    }
}

template<typename Alloc, typename Trait, typename E> inline void TxnPreHook<Alloc, Trait, E>::freeze() {
    if (m_recording) {
        throw logic_error("Double freeze detected");
    } else {
        m_recording = true;
    }
}

template<typename Alloc, typename Trait, typename E> inline void TxnPreHook<Alloc, Trait, E>::thaw() {
    if (m_recording) {
        m_changes.clear();
        m_copied.clear();
        m_storage.clear();
        m_last = nullptr;      // since m_storage is cleared
        m_hasDeletes = m_recording = false;
    } else {
        throw logic_error("Double thaw detected");
    }
}

template<typename Alloc, typename Trait, typename E>
inline void* TxnPreHook<Alloc, Trait, E>::_copy(void const* src, bool) {
    void* dst = m_storage.allocate();
    vassert(dst != nullptr);
    memcpy(dst, src, m_storage.tupleSize());
    m_copied.emplace(src);
    return dst;
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::update(void const* dst) {
    // src tuple from temp table written to dst in persistent storage
    if (m_recording && ! m_changes.count(dst)) {
        m_changes.emplace(dst, _copy(dst, false));
    }
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::insert(void const* dst) {
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of. Just mark the position as
        // previously unused.
        m_changes.emplace(dst, nullptr);
    }
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::remove(void const* src) {
    // src tuple is deleted, and tuple at dst gets moved to src
    if (m_recording && m_changes.count(src) == 0) {
        // Need to copy the original value that gets deleted
        vassert(m_last != nullptr);
        m_changes.emplace(src, m_last);
        m_last = nullptr;
        m_hasDeletes = true;
    }
}

template<typename Alloc, typename Trait, typename E>
inline void const* TxnPreHook<Alloc, Trait, E>::operator()(void const* src) const {
    auto const& pos = m_changes.find(src);
    return pos == m_changes.cend() ? src : pos->second;
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::release(void const* src) {
    Trait::remove(src);
}

template<typename Hook, typename E> inline
HookedCompactingChunks<Hook, E>::HookedCompactingChunks(size_t s) noexcept : CompactingChunks(s), Hook(s) {}

template<typename Hook, typename E>
template<typename Tag> inline shared_ptr<typename
    IterableTableTupleChunks<HookedCompactingChunks<Hook, E>, Tag, void>::hooked_iterator>
HookedCompactingChunks<Hook, E>::freeze() {
    CompactingChunks::freeze();
    Hook::freeze();
    return make_shared<typename
        IterableTableTupleChunks<HookedCompactingChunks<Hook, E>, Tag, void>::hooked_iterator>(*this);
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::thaw() {
    Hook::thaw();
    CompactingChunks::thaw();
}

template<typename Hook, typename E> inline void* HookedCompactingChunks<Hook, E>::allocate() {
    return CompactingChunks::allocate();
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::update(void* dst) {
    Hook::add(*this, Hook::ChangeType::Update, dst);
}

template<typename Hook, typename E> inline void const*
HookedCompactingChunks<Hook, E>::remove(void* dst) {
    if (frozen()) {
        Hook::copy(dst);
    }
    void const* src = CompactingChunks::free(dst);
    Hook::add(*this, Hook::ChangeType::Deletion, dst);
    return src;
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::remove(typename CompactingChunks::remove_direction dir, void const* p) {
    if (frozen() && dir == remove_direction::from_head) {
        throw logic_error("Cannot remove from head when frozen");
    } else {
        free(dir, p);
    }
}

template<typename Hook, typename E> inline void HookedCompactingChunks<Hook, E>::remove_add(void* p) {
    CompactingChunks::m_batched.add(p);
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::remove_reserve(size_t n) {
    CompactingChunks::m_batched.reserve(n);
}

template<typename Hook, typename E> inline size_t HookedCompactingChunks<Hook, E>::remove_force(
        function<void(vector<pair<void*, void*>> const&)> const& cb) {
    // hook registration
    std::for_each(CompactingChunks::m_batched.removed().cbegin(),
            CompactingChunks::m_batched.removed().cend(),
            [this](void* s) noexcept {
                Hook::copy(s);
                Hook::add(*this, Hook::ChangeType::Deletion, s);
            });
    std::for_each(CompactingChunks::m_batched.movements().cbegin(),
            CompactingChunks::m_batched.movements().cend(),
            [this](pair<void*, void*> const& entry) noexcept {
                Hook::copy(entry.first);
                Hook::add(*this, Hook::ChangeType::Deletion, entry.first);
                memcpy(entry.first, entry.second, tupleSize());
            });
    cb(CompactingChunks::m_batched.movements());    // NOTE: memcpy before the call back
    return CompactingChunks::m_batched.force();
}

template<typename Hook, typename E> inline void HookedCompactingChunks<Hook, E>::clear() {
    CompactingChunks::clear([this] (void const* s) noexcept {
                Hook::copy(s);
                Hook::add(*this, Hook::ChangeType::Deletion, s);
            });
}

// # # # # # # # # # # # # # # # # # Codegen: begin # # # # # # # # # # # # # # # # # # # # # # #
namespace __codegen__ {    // clumsy hack around macro arg arity check
template<typename Alloc, gc_policy gc>
using mt = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<Alloc>, HistoryRetainTrait<gc>>>;

using t1 = mt<EagerNonCompactingChunk, gc_policy::never>;
using t2 = mt<EagerNonCompactingChunk, gc_policy::always>;
using t3 = mt<EagerNonCompactingChunk, gc_policy::batched>;

using t4 = mt<LazyNonCompactingChunk, gc_policy::never>;
using t5 = mt<LazyNonCompactingChunk, gc_policy::always>;
using t6 = mt<LazyNonCompactingChunk, gc_policy::batched>;
}
#define range8(ranger)                                                     \
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
#define ComposeByteAndBitCodegen(m)                                        \
    ComposeByteAndBit0(m, 0); ComposeByteAndBit0(m, 1); ComposeByteAndBit0(m, 2); ComposeByteAndBit0(m, 3)
range8(ComposeByteAndBitCodegen);
#undef ComposeByteAndBitCodegen
#undef ComposeByteAndBit0
#undef range8

// Chunks
template class voltdb::storage::NonCompactingChunks<EagerNonCompactingChunk>;
template class voltdb::storage::NonCompactingChunks<LazyNonCompactingChunk>;
// HookedCompactingChunks : 2 x 2 x 3 = 12 instantiations
#define HookedChunksCodegen2(alloc, gc)                                     \
    template class voltdb::storage::HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>>
#define HookedChunksCodegen1(alloc)                                         \
    HookedChunksCodegen2(alloc, gc_policy::never);                          \
    HookedChunksCodegen2(alloc, gc_policy::always);                         \
    HookedChunksCodegen2(alloc, gc_policy::batched)
HookedChunksCodegen1(NonCompactingChunks<EagerNonCompactingChunk>);
HookedChunksCodegen1(NonCompactingChunks<LazyNonCompactingChunk>);
#undef HookedChunksCodegen1
#undef HookedChunksCodegen2
// iterators
#define IteratorTagCodegen2(perm, chunks, tag)                                   \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>            \
    ::template iterator_type<perm, iterator_view_type::txn>;                     \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>            \
    ::template iterator_type<perm, iterator_view_type::snapshot>

#define IteratorTagCodegen12(perm, chunks, tag, txn_alloc, gc)                   \
template class voltdb::storage::IterableTableTupleChunks<chunks, tag>            \
    ::template iterator_cb_type<TxnPreHook<txn_alloc, HistoryRetainTrait<gc>>, perm>
#define IteratorTagCodegen11(perm, chunks, tag, txn_alloc)                       \
    IteratorTagCodegen12(perm, chunks, tag, txn_alloc, gc_policy::never);        \
    IteratorTagCodegen12(perm, chunks, tag, txn_alloc, gc_policy::always);       \
    IteratorTagCodegen12(perm, chunks, tag, txn_alloc, gc_policy::batched)
#define IteratorTagCodegen10(perm, chunks, tag)                                  \
    IteratorTagCodegen11(perm, chunks, tag, NonCompactingChunks<EagerNonCompactingChunk>); \
    IteratorTagCodegen11(perm, chunks, tag, NonCompactingChunks<LazyNonCompactingChunk>)

#define IteratorTagCodegen1(chunks, tag)                                         \
    IteratorTagCodegen2(iterator_permission_type::rw, chunks, tag);              \
    IteratorTagCodegen2(iterator_permission_type::ro, chunks, tag);              \
    IteratorTagCodegen10(iterator_permission_type::rw, chunks, tag);             \
    IteratorTagCodegen10(iterator_permission_type::ro, chunks, tag);             \
    template class voltdb::storage::IterableTableTupleChunks<chunks, tag>::elastic_iterator

#define IteratorTagCodegen(tag)                                                  \
    IteratorTagCodegen1(NonCompactingChunks<EagerNonCompactingChunk>, tag);      \
    IteratorTagCodegen1(NonCompactingChunks<LazyNonCompactingChunk>, tag);       \
    IteratorTagCodegen1(CompactingChunks, tag);                                  \
    IteratorTagCodegen1(__codegen__::t1, tag);                                   \
    IteratorTagCodegen1(__codegen__::t2, tag);                                   \
    IteratorTagCodegen1(__codegen__::t3, tag);                                   \
    IteratorTagCodegen1(__codegen__::t4, tag);                                   \
    IteratorTagCodegen1(__codegen__::t5, tag);                                   \
    IteratorTagCodegen1(__codegen__::t6, tag)

IteratorTagCodegen(truth);
IteratorTagCodegen(NthBitChecker<0>); IteratorTagCodegen(NthBitChecker<1>);
IteratorTagCodegen(NthBitChecker<2>); IteratorTagCodegen(NthBitChecker<3>);
IteratorTagCodegen(NthBitChecker<4>); IteratorTagCodegen(NthBitChecker<5>);
IteratorTagCodegen(NthBitChecker<6>); IteratorTagCodegen(NthBitChecker<7>);
#undef IteratorTagCodegen
#undef IteratorTagCodegen1
#undef IteratorTagCodegen2
#undef IteratorTagCodegen12
#undef IteratorTagCodegen11
#undef IteratorTagCodegen10
// TxnPreHook: 2 x 3 = 6 instantiations
#define TxnPreHookCodegen(alloc)                                                 \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::always>>;       \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::batched>>;      \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::never>>
// we do not use compacting chunk for underlying storage
TxnPreHookCodegen(NonCompactingChunks<EagerNonCompactingChunk>);
TxnPreHookCodegen(NonCompactingChunks<LazyNonCompactingChunk>);
#undef TxnPreHookCodegen
// hooked_iterator_type : 8 x 2 x 3 = 48 instantiations
#define HookedIteratorCodegen3(tag, alloc, gc)                                           \
template class voltdb::storage::IterableTableTupleChunks<                                \
    HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>>, \
    tag>::template hooked_iterator_type<iterator_permission_type::rw>;                   \
template class voltdb::storage::IterableTableTupleChunks<                                \
    HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>>, \
    tag>::template hooked_iterator_type<iterator_permission_type::ro>
#define HookedIteratorCodegen2(tag, alloc)                                               \
    HookedIteratorCodegen3(tag, alloc, gc_policy::never);                                \
    HookedIteratorCodegen3(tag, alloc, gc_policy::always);                               \
    HookedIteratorCodegen3(tag, alloc, gc_policy::batched)
#define HookedIteratorCodegen(tag)                                                       \
    HookedIteratorCodegen2(tag, NonCompactingChunks<EagerNonCompactingChunk>);           \
    HookedIteratorCodegen2(tag, NonCompactingChunks<LazyNonCompactingChunk>)
HookedIteratorCodegen(truth);
HookedIteratorCodegen(NthBitChecker<0>); HookedIteratorCodegen(NthBitChecker<1>);
HookedIteratorCodegen(NthBitChecker<2>); HookedIteratorCodegen(NthBitChecker<3>);
HookedIteratorCodegen(NthBitChecker<4>); HookedIteratorCodegen(NthBitChecker<5>);
HookedIteratorCodegen(NthBitChecker<6>); HookedIteratorCodegen(NthBitChecker<7>);
#undef HookedIteratorCodegen
#undef HookedIteratorCodegen2
#undef HookedIteratorCodegen3
// template member function
#define HookedFreeze2(tag, alloc, gc)                                                    \
template shared_ptr<typename IterableTableTupleChunks<HookedCompactingChunks<TxnPreHook<alloc,  \
         HistoryRetainTrait<gc>>, void>, tag, void>::hooked_iterator>                    \
HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>, void>::freeze<tag>()
#define HookedFreeze1(tag, alloc)                                                        \
    HookedFreeze2(tag, alloc, gc_policy::never);                                         \
    HookedFreeze2(tag, alloc, gc_policy::always);                                        \
    HookedFreeze2(tag, alloc, gc_policy::batched)
#define HookedFreeze(tag)                                                                \
    HookedFreeze1(tag, NonCompactingChunks<EagerNonCompactingChunk>);                    \
    HookedFreeze1(tag, NonCompactingChunks<LazyNonCompactingChunk>)
HookedFreeze(truth);
#undef HookedFreeze
#undef HookedFreeze1
#undef HookedFreeze2
// # # # # # # # # # # # # # # # # # Codegen: end # # # # # # # # # # # # # # # # # # # # # # #

