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
    if (range_next() >= range_end()) {                 // chunk is full
        return nullptr;
    } else {
        void* res = range_next();
        reinterpret_cast<char*&>(m_next) += m_tupleSize;
        return res;
    }
}

template<allocator_enum_type T> inline bool ChunkHolder<T>::contains(void const* addr) const {
    // check alignment
    vassert(addr < range_begin() || addr >= range_end() || 0 ==
            (reinterpret_cast<char const*>(addr) - reinterpret_cast<char*const>(range_begin())) % m_tupleSize);
    return addr >= range_begin() && addr < range_next();
}

template<allocator_enum_type T> inline bool ChunkHolder<T>::full() const noexcept {
    return range_next() == range_end();
}

template<allocator_enum_type T> inline bool ChunkHolder<T>::empty() const noexcept {
    return range_next() == range_begin();
}

template<allocator_enum_type T> inline void* const ChunkHolder<T>::range_begin() const noexcept {
    return reinterpret_cast<void*>(allocator_type<T>::get());
}

template<allocator_enum_type T> inline void* const ChunkHolder<T>::range_end() const noexcept {
    return m_end;
}

template<allocator_enum_type T> inline void* const ChunkHolder<T>::range_next() const noexcept {
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
        vassert(r < range_next() && r >= range_begin());
        m_freed.pop();
        return r;
    }
}

inline void EagerNonCompactingChunk::free(void* src) {
    if (reinterpret_cast<char*>(src) + tupleSize() == range_next()) {     // last element: decrement boundary ptr
        m_next = src;
    } else {                               // hole in the middle: keep track of it
        m_freed.emplace(src);
    }
    if (empty()) {
        m_next = range_begin();
        m_freed = decltype(m_freed){};
    }
}

inline bool EagerNonCompactingChunk::empty() const noexcept {
    return super::empty() || tupleSize() * m_freed.size() ==
        reinterpret_cast<char const*>(range_next()) - reinterpret_cast<char const*>(range_begin());
}

inline bool EagerNonCompactingChunk::full() const noexcept {
    return super::full() && m_freed.empty();
}

inline LazyNonCompactingChunk::LazyNonCompactingChunk(id_type s1, size_t s2, size_t s3) : super(s1, s2, s3) {}

inline void LazyNonCompactingChunk::free(void* src) {
    vassert(src >= range_begin() && src < range_next());
    if (reinterpret_cast<char*>(src) + tupleSize() == range_next()) {     // last element: decrement boundary ptr
        m_next = src;
    } else {
        ++m_freed;
    }
    if (m_freed * tupleSize() ==
            reinterpret_cast<char const*>(range_next()) - reinterpret_cast<char const*>(range_begin())) {
        // everything had been freed, the chunk becomes empty
        m_next = range_begin();
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

template<typename Iter> inline typename ChunkListIdSeeker<Iter, true_type>::iterator
ChunkListIdSeeker<Iter, true_type>::emplace(id_type id, Iter const& iter) {        // add to rear
    vassert(super::empty() || (iter->id() == super::back()->id() + 1 && id == iter->id()));
    super::emplace_back(iter);
    return prev(end());
}

template<typename Iter> inline bool ChunkListIdSeeker<Iter, true_type>::contains(id_type id) const noexcept {
    return ! super::empty() && ! less_rolling(id, super::front()->id()) &&
        ! less_rolling(super::back()->id(), id);
}

template<typename Iter> inline typename ChunkListIdSeeker<Iter, true_type>::iterator
ChunkListIdSeeker<Iter, true_type>::find(id_type id) {
    if (contains(id)) {
        vassert(super::at(id - super::front()->id())->id() == id);
        return next(super::begin(), id - super::front()->id());
    } else {
        return end();
    }
}

template<typename Iter> inline size_t ChunkListIdSeeker<Iter, true_type>::erase(id_type id) {
    if (super::empty()) {
        throw underflow_error("empty deque");
    } else if (id == super::front()->id()) {
        super::pop_front();
    } else if (id == super::back()->id()) {
        super::pop_back();
    } else {
        throw logic_error("ChunkListIdSeeker cannot be used to erase middle node");
    }
    return 1;
}

template<typename Iter> inline Iter&
ChunkListIdSeeker<Iter, true_type>::get(typename ChunkListIdSeeker<Iter, true_type>::iterator const& iter) {
    if (iter == super::end()) {
        throw range_error("ChunkListIdSeeker::get(): iterator out of range");
    } else {
        return *iter;
    }
}

template<typename Iter> inline Iter&
ChunkListIdSeeker<Iter, false_type>::get(typename ChunkListIdSeeker<Iter, false_type>::iterator const& iter) {
    if (iter == super::end()) {
        throw range_error("ChunkListIdSeeker::get(): iterator out of range");
    } else {
        return iter->second;
    }
}

template<typename Chunk, typename Compact, typename E>
inline pair<bool, typename ChunkList<Chunk, Compact, E>::iterator> ChunkList<Chunk, Compact, E>::find(void const* k) const {
    // NOTE: this is a hacky method signature, and hacky way to
    // get around. Normally, we need to overload 2 versions of
    // find: one const-method that returns a const-iterator, and
    // one non-const-method that returns a normal iterator.
    // However, doing this would require downstream API
    // (CompactingChunks::find), and a couple more places to do
    // the same, and TxnLeftBoundary, etc. to maintain two sets
    // of iterators.
    auto* mutable_this = const_cast<ChunkList<Chunk, Compact, E>*>(this);
    if (! m_byAddr.empty()) {
        // find first entry whose begin() > k
        auto iter = mutable_this->m_byAddr.upper_bound(k);
        if (iter != mutable_this->m_byAddr.begin()) {
            iter = prev(iter);
            if (iter != mutable_this->m_byAddr.end()) {
                return {true, iter->second};
            }
        }
    }
    return {false, mutable_this->end()};
}

template<typename Chunk, typename Compact, typename E> inline pair<bool, typename ChunkList<Chunk, Compact, E>::iterator>
ChunkList<Chunk, Compact, E>::find(id_type id) const {
    auto* mutable_this = const_cast<ChunkList<Chunk, Compact, E>*>(this);
    auto const& iter = mutable_this->m_byId.find(id);
    auto const found = iter != mutable_this->m_byId.end();
    return {found, found ? mutable_this->m_byId.get(iter) : mutable_this->end()};
}

template<typename Chunk, typename Compact, typename E> inline ChunkList<Chunk, Compact, E>::ChunkList(size_t tsize) noexcept :
super(), m_tupleSize(tsize), m_chunkSize(::chunkSize(m_tupleSize)) {}

template<typename Chunk, typename Compact, typename E> inline id_type& ChunkList<Chunk, Compact, E>::lastChunkId() {
    return m_lastChunkId;
}

template<typename Chunk, typename Compact, typename E> inline size_t ChunkList<Chunk, Compact, E>::tupleSize() const noexcept {
    return m_tupleSize;
}

template<typename Chunk, typename Compact, typename E> inline size_t ChunkList<Chunk, Compact, E>::chunkSize() const noexcept {
    return m_chunkSize;
}

template<typename Chunk, typename Compact, typename E> inline size_t
ChunkList<Chunk, Compact, E>::size() const noexcept {
    return m_size;
}

template<typename Chunk, typename Compact, typename E> inline typename ChunkList<Chunk, Compact, E>::iterator const&
ChunkList<Chunk, Compact, E>::last() const noexcept {
    return m_back;
}

template<typename Chunk, typename Compact, typename E> inline typename ChunkList<Chunk, Compact, E>::iterator&
ChunkList<Chunk, Compact, E>::last() noexcept {
    return m_back;
}

template<typename Chunk, typename Compact, typename E> inline void
ChunkList<Chunk, Compact, E>::add(typename ChunkList<Chunk, Compact, E>::iterator const& iter) {
    m_byAddr.emplace(iter->range_begin(), iter);
    m_byId.emplace(iter->id(), iter);
}

template<typename Chunk, typename Compact, typename E> inline void
ChunkList<Chunk, Compact, E>::remove(typename ChunkList<Chunk, Compact, E>::iterator const& iter) {
    m_byAddr.erase(iter->range_begin());
    m_byId.erase(iter->id());
}

template<typename Chunk, typename Compact, typename E>
template<typename... Args> inline typename ChunkList<Chunk, Compact, E>::iterator
ChunkList<Chunk, Compact, E>::emplace_back(Args&&... args) {
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

template<typename Chunk, typename Compact, typename E> inline void ChunkList<Chunk, Compact, E>::pop_front() {
    if (super::empty()) {
        throw underflow_error("pop_front() called on empty chunk list");
    } else {
        remove(super::begin());
        if (--m_size) {
            super::pop_front();
        } else {
            clear();
        }
    }
}

template<typename Chunk, typename Compact, typename E> inline void ChunkList<Chunk, Compact, E>::pop_back() {
    if (super::empty()) {
        throw underflow_error("pop_back() called on empty chunk list");
    } else {
        auto const iter = find(m_back->id() - 1);
        if (iter.first) {            // original list contains more than 1 nodes
            remove(m_back);
            super::erase_after(m_back = iter.second);
            --lastChunkId();
            --m_size;
        } else {
            clear();
        }
    }
}

template<typename Chunk, typename Compact, typename E>
template<typename Pred> inline void ChunkList<Chunk, Compact, E>::remove_if(Pred pred) {
    for(auto iter = begin(); iter != end(); ++iter) {
        if (pred(*iter)) {
            remove(iter);
            --m_size;
        } else {                           // since the last node could be invalidated, recalculate it
            m_back = iter;
        }
    }
    if (empty()) {
        clear();
    } else {
        super::remove_if(pred);
    }
}

template<typename Chunk, typename Compact, typename E> inline void
ChunkList<Chunk, Compact, E>::clear() noexcept {
    m_byId.clear();
    m_byAddr.clear();
    super::clear();
    m_back = end();
    lastChunkId() = 0;
    m_size = 0;
}

template<typename C, typename E> inline
NonCompactingChunks<C, E>::NonCompactingChunks(size_t tupleSize) noexcept : list_type(tupleSize) {}

template<typename C, typename E> inline size_t NonCompactingChunks<C, E>::chunks() const noexcept {
    return ChunkList<C, false_type>::size();
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
        throw range_error(buf);
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
    if (reinterpret_cast<char*>(dst) + tupleSize() == range_next()) {     // last allocation on the chunk
        return free();
    } else {                               // free in the middle
        memcpy(dst, free(), tupleSize());
        return range_next();
    }
}

inline void* CompactingChunk::free() {                               // within-chunk free() of last allocated
    vassert(range_next() > range_begin());
    return reinterpret_cast<char*&>(m_next) -= tupleSize();
}

inline CompactingStorageTrait::CompactingStorageTrait(
        typename CompactingStorageTrait::list_type& s) noexcept : m_storage(s) {}

inline void CompactingStorageTrait::freeze() {
    if (m_frozen) {
        throw logic_error("CompactingStorageTrait::freeze(): double freeze detected");
    } else {
        m_frozen = true;
    }
}

template<typename Pred> inline void CompactingStorageTrait::thaw(Pred&& pred) {
    if (m_frozen) {                        // release all chunks invisible to txn
        if (! m_storage.empty()) {
            auto& storage = reinterpret_cast<CompactingChunks&>(m_storage);
            auto const& beginTxn = storage.beginTxn();
            bool const empty = beginTxn.empty();
            auto const stop = empty ? 0 : beginTxn.iterator()->id();
            if (! storage.frozenBoundaries() || // easy case: degenerated frozen boundary, or
                    ! storage.finalizerAndCopier()) {                  // no finalizer involved
                while (! m_storage.empty() && (empty || less_rolling(m_storage.front().id(), stop))) {
                    assert(storage.begin()->range_begin() == storage.begin()->range_next());
                    storage.pop_front();
                }
            } else {                       // harder case: has frozen boundary
                auto const& left = storage.frozenBoundaries()->left(),
                     &right = storage.frozenBoundaries()->right();
                auto const& left_id = left.chunkId(), &right_id = right.chunkId();
                while (! m_storage.empty() && (empty || less_rolling(m_storage.front().id(), stop))) {
                    auto const& id = m_storage.front().id();
                    storage.pop_finalize(id == left_id ? left.address() :
                                (id == right_id ? right.address() : m_storage.front().range_end()),
                            pred);
                }
                if (! m_storage.empty()) {                             // finalize on first txn chunk
                    auto const& front = m_storage.front();
                    auto const& beg_id = front.id();
                    void const* dst = nullptr;
                    if (beg_id == right_id) {
                        dst = right.address();
                    } else if (beg_id == left_id) {
                        dst = left.address();
                    } else if (less<id_type>()(left_id, beg_id) &&
                            less<id_type>()(beg_id, right_id)) {
                        dst = front.range_end();
                    }
                    if (dst != nullptr) {
                        for (char const* ptr = reinterpret_cast<char const*>(front.range_next());
                                ptr < dst; ptr += storage.tupleSize()) {
                            if (pred(ptr)) {
                                storage.finalizerAndCopier().finalize(ptr);
                            }
                        }
                    }
                }
            }
        }
        m_frozen = false;
    } else {
        throw logic_error("CompactingStorageTrait::freeze(): double thaw detected");
    }
}

inline bool CompactingStorageTrait::frozen() const noexcept {
    return m_frozen;
}

inline void CompactingStorageTrait::release(
        typename CompactingStorageTrait::list_type::iterator iter, void const* p) {
    if (m_frozen && less_rolling(iter->id(),
                reinterpret_cast<CompactingChunks const&>(m_storage).beginTxn().iterator()->id()) &&
            reinterpret_cast<char const*>(p) + iter->tupleSize() >= iter->range_end()) {
        vassert(iter == m_storage.begin());
        m_storage.pop_front();
    }
}

inline typename CompactingStorageTrait::list_type::iterator CompactingStorageTrait::releasable(
        typename CompactingStorageTrait::list_type::iterator iter) {
    if (iter->empty()) {
        auto iter_next = next(iter);
        if (! m_frozen) {                // safe to erase a Chunk unless frozen
            vassert(iter == m_storage.begin());
            m_storage.pop_front();
        }
        return iter_next;
    } else {
        return iter;
    }
}

namespace std {                                    // Need to declare these before using (for Mac clang++)
    using namespace voltdb::storage;
    template<> struct less<position_type> {
        inline bool operator()(position_type const& lhs, position_type const& rhs) const noexcept {
            auto const id1 = lhs.chunkId(), id2 = rhs.chunkId();
            return (id1 == id2 && lhs.address() < rhs.address()) || less_rolling(id1, id2);
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

FinalizerAndCopier::FinalizerAndCopier(function<void(void const*)> const& finalize,
        function<void*(void*, void const*)> const& copy) noexcept : m_complicated(true),
       m_finalize(finalize), m_copy(copy) {}

inline FinalizerAndCopier::operator bool() const noexcept {
    return m_complicated;
}

inline void FinalizerAndCopier::finalize(void const* p) const {
    assert(m_complicated);
    m_finalize(p);
}

inline void* FinalizerAndCopier::copy(void* dst, void const* src) const {
    assert(m_complicated);
    return m_copy(dst, src);
}

CompactingChunks::CompactingChunks(size_t tupleSize, FinalizerAndCopier const& cb) noexcept :
    list_type(tupleSize), CompactingStorageTrait(static_cast<list_type&>(*this)),
    m_txnFirstChunk(*this), m_finalizerAndCopier(cb), m_batched(*this) {}

CompactingChunks::CompactingChunks(size_t tupleSize) noexcept :
    list_type(tupleSize), CompactingStorageTrait(static_cast<list_type&>(*this)),
    m_txnFirstChunk(*this), m_finalizerAndCopier{}, m_batched(*this) {}

CompactingChunks::~CompactingChunks() {
    while (cbegin() != cend()) {
        pop_finalize();
    }
}

inline FinalizerAndCopier const& CompactingChunks::finalizerAndCopier() const noexcept {
    return m_finalizerAndCopier;
}

inline CompactingChunks::TxnLeftBoundary::TxnLeftBoundary(ChunkList<CompactingChunk, true_type>& chunks) noexcept :
    m_chunks(chunks), m_iter(chunks.end()), m_next(nullptr) {
    vassert(m_chunks.empty());
}

inline typename ChunkList<CompactingChunk, true_type>::iterator const& CompactingChunks::TxnLeftBoundary::iterator() const noexcept {
    return m_iter;
}

inline typename ChunkList<CompactingChunk, true_type>::iterator& CompactingChunks::TxnLeftBoundary::iterator() noexcept {
    return m_iter;
}

inline typename ChunkList<CompactingChunk, true_type>::iterator const& CompactingChunks::TxnLeftBoundary::iterator(
        typename ChunkList<CompactingChunk, true_type>::iterator const& iter) noexcept {
    m_next = m_chunks.end() == iter ? nullptr : iter->range_next();
    return m_iter = iter;
}

inline void const*& CompactingChunks::TxnLeftBoundary::range_next() noexcept {
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

inline pair<bool, typename CompactingChunks::list_type::iterator>
CompactingChunks::find(id_type id, bool) noexcept {
    return list_type::find(id);
}

inline pair<bool, typename CompactingChunks::list_type::iterator>
CompactingChunks::find(void const* p, bool) noexcept {
    return list_type::find(p);
}

inline typename CompactingChunks::list_type::iterator CompactingChunks::releasable() {
    return beginTxn().iterator(CompactingStorageTrait::releasable(beginTxn().iterator()));
}

inline void CompactingChunks::pop_finalize() {
    assert(begin() != end());
    pop_finalize(begin()->range_next(), truth{});
}

template<typename Pred> inline void
CompactingChunks::pop_finalize(const void* to, Pred&& pred) {
    assert(begin() != end());
    if (m_finalizerAndCopier && begin()->range_begin() < to) {
        for (char const* ptr = reinterpret_cast<char const*>(begin()->range_begin());
                ptr < to; ptr += tupleSize()) {
            if (pred(ptr)) {
                m_finalizerAndCopier.finalize(ptr);
            }
        }
    }
    pop_front();
}

/**
 * Poping front chunks should never trigger any finalize action,
 * whether frozen or not.
 *
 * When not frozen, the chunk addresses had been moved over to
 * somewhere in the middle of the allocation, and the finalizer
 * need to be called at that new addresss;
 *
 * When frozen, it has also been moved to TXN region, so in
 * neither case should the popped chunk finalize.
 */
inline void CompactingChunks::pop_front() {
    list_type::pop_front();
    beginTxn().iterator(begin());
}

inline void CompactingChunks::pop_frozen() {
    assert(frozenBoundaries() && begin() != end() &&
            (beginTxn().empty() ||
             less<id_type>()(begin()->id(), beginTxn().iterator()->id())));
    list_type::pop_front();
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
        m_frozenTxnBoundaries = FrozenTxnBoundaries{*this};
    }
}

template<typename Pred>
inline void CompactingChunks::thaw(Pred&& pred) {
    CompactingStorageTrait::thaw(pred);
    m_frozenTxnBoundaries = {};
}

template<typename Remove_cb>
inline void CompactingChunks::clear(Remove_cb const& cb) {
    if (m_lastFreeFromHead != nullptr) {
        throw logic_error("Unfinished remove(remove_direction::from_head, ?)");
    } else if (! m_batched.empty()) {
        throw logic_error("Unfinished remove_add(?) or remove_force()");
    } else  {                        // slow clear path
        // first, apply call back on all txn tuples (in order)
        fold<IterableTableTupleChunks<CompactingChunks, truth>::const_iterator>(
                static_cast<CompactingChunks const&>(*this),
                [&cb] (void const* p) noexcept {cb(p);});
        if (frozen()) {
            assert(frozenBoundaries());
            if (m_finalizerAndCopier) {              // finalize the region between frozen right, and txn end
                auto const& frozenRight = frozenBoundaries()->right();
                if (less<position_type>()(frozenRight, *last())) {
                    for (auto id = frozenRight.chunkId();
                            less_rolling(id, last()->id());
                            ++id) {
                        auto const iterp = list_type::find(id);
                        vassert(iterp.first);
                        for (auto const* ptr = reinterpret_cast<char const*>(
                                    id == frozenRight.chunkId() ? frozenRight.address() :
                                    iterp.second->range_begin());
                                ptr < iterp.second->range_next();
                                ptr += tupleSize()) {
                            m_finalizerAndCopier.finalize(ptr);
                        }
                    }
                }
            }
            // then, release all chunks from txn view
            while (! beginTxn().empty()) {
                beginTxn().range_next() = beginTxn().iterator()->m_next =
                    beginTxn().iterator()->range_begin();
                releasable();
            }
            m_allocs = 0;
        } else {                               // fast clear path
            while (begin() != end()) {
                pop_finalize();
            }
            list_type::clear();
            m_allocs = 0;
            m_txnFirstChunk.iterator(list_type::begin());
        }
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
            beginTxn().range_next() = last()->range_next();
        }
    }
    ++m_allocs;
    return r;
}

inline bool CompactingChunks::empty() const noexcept {
    return m_allocs == 0;
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
        snprintf(buf, sizeof buf, "Cannot create RW snapshot iterator on chunk list id %lu",
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

inline void CompactingChunks::finalize(void const* p) const {
    if (m_finalizerAndCopier) {
        m_finalizerAndCopier.finalize(p);
    }
}

inline void CompactingChunks::free(typename CompactingChunks::remove_direction dir, void const* p) {
    switch (dir) {
        case remove_direction::from_head:
            /**
             * Schema change: it is called in the same
             * order as txn iterator.
             *
             * NOTE: since we only do chunk-wise drop, any reads
             * from the allocator when these dispersed calls are
             * occurring *will* get spurious data.
             *
             * NOTE: finalizer is not called when deleting from
             * head, considering 3 possible schema changes on
             * VARCHAR columns:
             * 1. inlined column => non-inlined column: the
             * column is registered as a exploded column, and new
             * object is allocated in the string pool.
             * 2. inlined column => inlined column: finalizer
             * does not apply on the old schema column.
             * 3. non-inlined column => inlined column:
             * the schema change is prohibited when altering
             * a non-empty table.
             */
            if (p == nullptr) {                         // marks completion
                if (m_lastFreeFromHead != nullptr && beginTxn().iterator()->contains(m_lastFreeFromHead)) {
                    // effects deletions in 1st chunk
                    vassert(reinterpret_cast<char const*>(beginTxn().range_next()) >= m_lastFreeFromHead + tupleSize());
                    auto const offset = reinterpret_cast<char const*>(beginTxn().range_next()) - m_lastFreeFromHead - tupleSize();
                    if (offset) {                              // some memory ops are unavoidable
                        char* dst = reinterpret_cast<char*>(beginTxn().iterator()->range_begin());
                        char const* src = reinterpret_cast<char const*>(beginTxn().range_next()) - offset;
                        // NOTE: finalize do not need to be called in this case
                        if (dst + offset < src) {
                            memcpy(dst, src, offset);
                        } else {
                            memmove(dst, src, offset);
                        }
                        const_cast<char*&>(reinterpret_cast<char const*&>(beginTxn().range_next())) =
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
                vassert((m_lastFreeFromHead == nullptr && p == beginTxn().iterator()->range_begin()) ||       // called for the first time?
                        (beginTxn().iterator()->contains(p) && m_lastFreeFromHead + tupleSize() == p) ||      // same chunk,
                        next(beginTxn().iterator())->range_begin() == p);                                     // or next chunk
                if (! beginTxn().iterator()->contains(m_lastFreeFromHead = reinterpret_cast<char const*>(p))) {
                    pop_front();
                }
                --m_allocs;
            }
            break;
        case remove_direction::from_tail:
            /**
             * Undo insert: it is called in the exactly
             * opposite order of txn iterator. No need to call
             * finalizer.
             */
            if (empty()) {
                snprintf(buf, sizeof buf, "CompactingChunks::remove(from_tail, %p): empty allocator", p);
                buf[sizeof buf - 1] = 0;
                throw underflow_error(buf);
            } else {
                vassert(reinterpret_cast<char const*>(p) + tupleSize() == last()->range_next());
                if (last()->range_begin() == (last()->m_next = const_cast<void*>(p))) { // delete last chunk
                    pop_back();
                    if (m_allocs ==  1) {
                        beginTxn().iterator(list_type::end());
                    }
                }
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

inline CompactingChunks::FrozenTxnBoundaries::FrozenTxnBoundaries(ChunkList<CompactingChunk, true_type> const& l) noexcept {
    assert(! l.empty());
    const_cast<position_type&>(m_left) = *l.begin();
    const_cast<position_type&>(m_right) = *l.last();
}

inline typename CompactingChunks::FrozenTxnBoundaries&
CompactingChunks::FrozenTxnBoundaries::operator=(FrozenTxnBoundaries const& o) noexcept {
    const_cast<position_type&>(left()) = o.left();
    const_cast<position_type&>(right()) = o.right();
    return *this;
}

inline position_type const& CompactingChunks::FrozenTxnBoundaries::left() const noexcept {
    return m_left;
}

inline position_type const& CompactingChunks::FrozenTxnBoundaries::right() const noexcept {
    return m_right;
}

inline boost::optional<typename CompactingChunks::FrozenTxnBoundaries> const&
CompactingChunks::frozenBoundaries() const noexcept {
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

inline char const* CompactingChunks::DelayedRemover::RemovableRegion::range_begin() const noexcept {
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
        r[h_index++] = const_cast<char*>(range_begin());
    }
    while ((m_index = mask().find_next(m_index)) != bitset_t::npos) {
        r[h_index++] = const_cast<char*>(range_begin()) + tupleSize * m_index;
    }
    vassert(r.size() == mask().count());
    return r;
}

inline CompactingChunks::DelayedRemover::DelayedRemover(CompactingChunks& s) : m_chunks(s) {}

inline void CompactingChunks::DelayedRemover::reserve(size_t n) {
    if (m_chunks.empty()) {
        throw underflow_error("CompactingChunks is empty, cannot reserve for batch removal");
    } else if (m_size > 0) {
        throw logic_error("CompactingChunks::DelayedRemover::reserve(): double reserve called");
    } else {
        vassert(m_removedRegions.empty() && m_moved.empty() && m_movements.empty() && m_removed.empty());
        m_size = n;
        auto iter = m_chunks.beginTxn().iterator();
        auto const chunkSize = m_chunks.chunkSize(),
             tupleSize = m_chunks.tupleSize(),
             tuplesPerChunk = chunkSize / tupleSize;
        auto const offset = reinterpret_cast<char const*>(iter->range_next()) -
            reinterpret_cast<char const*>(iter->range_begin());
        auto chunksTBReleasedFull = n / tuplesPerChunk,
             tuplesTBReleasedPartial = n % tuplesPerChunk;
        if (tuplesTBReleasedPartial * tupleSize >= offset) {
            ++chunksTBReleasedFull;
            tuplesTBReleasedPartial -= offset / tupleSize;
        } else if (chunksTBReleasedFull) {
            tuplesTBReleasedPartial = (m_size - offset / tupleSize) % tuplesPerChunk;
        }
        for (size_t i = 0; i < chunksTBReleasedFull; ++i) {
            auto *beg = reinterpret_cast<char const*>(iter->range_begin()),
                 *end = reinterpret_cast<char const*>(iter->range_next());
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
                    reinterpret_cast<char const*>(iter->range_next()),
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
                     (p >= region->range_begin() + tupleSize * region->mask().size() ||
                      p < region->range_begin()))) {
                m_moved.emplace_back(p);
            } else {
                m_removed.emplace_back(p);
                region = &removed_iter->second;
                auto const offset =
                    (reinterpret_cast<char*>(p) - region->range_begin()) / tupleSize;
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

inline size_t CompactingChunks::DelayedRemover::finalize() const {
    if (m_chunks.finalizerAndCopier() && ! (m_chunks.frozen() && m_chunks.frozenBoundaries())) {
        // finalizer in frozen state must be postponed
        if (! m_removed.empty()) {
            // removed addresses need to be finalized right away,
            // unless frozen
            for_each(m_removed.cbegin(), m_removed.cend(),
                    [this](void const* p) { m_chunks.finalizerAndCopier().finalize(p); });
        }
        if (! m_movements.empty()) {
            /**
             * The frozen case: finalize need to by called on
             * destination unless a fresh entry is added to hook
             * memory. This depends on CompactingChunks::add(...) to handle it
             */
            for_each(m_movements.cbegin(), m_movements.cend(),
                    [this](pair<void*, void*> const& entry) {
                        m_chunks.finalizerAndCopier().finalize(entry.first);
                    });
        }
        return m_removed.size() + m_movements.size();
    } else {
        return 0;
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
                    reinterpret_cast<char*&>(iter->m_next) = reinterpret_cast<char*>(iter->range_begin());
                    m_chunks.releasable();
                });
        auto last = m_chunks.find(m_removedRegions.rbegin()->first);
        vassert(last.first);
        vassert(reinterpret_cast<char const*>(last.second->range_next()) -
                reinterpret_cast<char const*>(last.second->range_begin()) >=
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

inline size_t CompactingChunks::DelayedRemover::clear(bool force) noexcept {
    assert(force || m_size == 0);
    if (force) {
        m_size = 0;
    }
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
    return clear(false);
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

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type src) :
    m_offset(src.tupleSize()), m_storage(src),
    m_iter(iterator_begin<typename remove_reference<container_type>::type, perm, view>()(src)),
    m_cursor(const_cast<value_type>(m_iter == m_storage.end() ? nullptr : m_iter->range_begin())) {
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
template<iterator_permission_type perm, iterator_view_type view> inline boost::optional<position_type>
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::to_position() const noexcept {
    if (m_cursor == nullptr) {
        return {};
    } else {
        return position_type{m_cursor, m_iter};
    }
}

/**
 * Helper to check if the iterator needs to advance to next
 * chunk. The only special occasion is snapshot view of
 * head-compacting, when there are tuple deletions occurring
 * during snapshot.
 */
template<typename ChunkList, typename Iter, iterator_view_type view, typename Comp>
struct ChunkBoundary {
    inline void const* operator()(ChunkList const&, Iter const& iter) const noexcept {
        return iter->range_next();
    }
};

template<typename ChunkList, typename Iter>
struct ChunkBoundary<ChunkList, Iter, iterator_view_type::snapshot, true_type> {
    inline void const* operator()(ChunkList const& l, Iter const& iter) const noexcept {
        auto const& frozenBoundaries = reinterpret_cast<CompactingChunks const&>(l).frozenBoundaries();
        if (! frozenBoundaries) {        // not frozen
            return iter->range_next();
        } else {
            auto const& beginTxn = reinterpret_cast<CompactingChunks const&>(l).beginTxn();
            auto const leftId = frozenBoundaries->left().chunkId(),
                 rightId = frozenBoundaries->right().chunkId(),
                 txnBeginChunkId = beginTxn.empty() ? 0 : beginTxn.iterator()->id(),
                 iterId = iter->id();
            if (beginTxn.empty()) {                // txn view is empty
                vassert(iter->range_begin() == iter->range_next());
                return iterId == rightId ? frozenBoundaries->right().address() : iter->range_end();
            } else if (leftId == iterId) {      // in the left boundary of frozen state
                return frozenBoundaries->left().address();
            } else if(less_rolling(iterId, txnBeginChunkId)) {  // in chunk visible to frozen iterator only
                if (less_rolling(iterId, rightId)) {
                    return iter->range_end();
                } else {
                    vassert(iterId == rightId);
                    return frozenBoundaries->right().address();
                }
            } else if (rightId == iterId) {     // in the right boundary
                return frozenBoundaries->right().address();
            } else if (txnBeginChunkId == iterId) {
                return iter->range_end();
            } else {
                return iter->range_next();
            }
        }
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
            if (m_cursor < boundary(m_storage, m_iter)) {
                finished = false;              // within chunk
            } else {
                ++m_iter;                      // cross chunk
                if (m_iter != m_storage.end()) {
                    const_cast<void*&>(m_cursor) = const_cast<void*>(m_iter->range_begin());
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
    m_txnBoundary(m_empty ? boost::none : boost::optional<position_type>{*c.last()}),
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
            typename IterableTableTupleChunks::chunk_type::iterator const&,
            boost::optional<position_type> const&,
            typename IterableTableTupleChunks::chunk_type::list_type::const_iterator&,
            void const*&) const {
        throw logic_error("elastic_iterator can only iterate over compacting chunks");
    }
};

template<typename I, typename ElasticIterator>
struct ElasticIterator_refresh<I, ElasticIterator, true_type> {
    inline void operator()(ElasticIterator& iter, bool& isEmpty, id_type& chunkId,
            typename I::chunk_type const& storage,
            typename I::chunk_type::iterator const& last,
            boost::optional<position_type> const& boundary,
            ChunkList<CompactingChunk, true_type>::const_iterator& chunkIter,
            void const*& cursor) const {
        if (isEmpty) {                             // last time checked, allocator was empty
            isEmpty = storage.empty();             // check again,
            if (! isEmpty) {   // if it has something now, set cursor to 1st
                chunkIter = storage.beginTxn().iterator();
                chunkId = chunkIter->id();
                cursor = chunkIter->range_begin();
                const_cast<boost::optional<position_type>&>(boundary) = *last;
            }
        } else if (! iter.drained()) {
            if ((isEmpty = storage.empty())) {
                cursor = nullptr;
            } else {
                auto const& indexBeg = storage.beginTxn().iterator();
                if (less_rolling(chunkId, indexBeg->id())) {
                    // current chunk list iterator is stale
                    chunkId = (chunkIter = indexBeg)->id();
                    cursor = chunkIter->range_begin();
                } else if (! chunkIter->contains(cursor)) {
                    assert(iter.txnBoundary());
                    assert(iter.to_position());
                    // Current chunk has been partially compacted,
                    // to the extent that cursor position is stale
                    cursor =
                        ++chunkIter == storage.end() ||
                        less<position_type>()(*iter.txnBoundary(), *iter.to_position()) ?        // drained
                        nullptr : chunkIter->range_begin();
                }
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

template<typename Chunks, typename Tag, typename E> inline boost::optional<position_type> const&
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

inline position_type::position_type(ChunkHolder<> const& c) noexcept : m_chunkId(c.id()), m_addr(c.range_next()) {}

inline position_type::position_type(CompactingChunks const& c, void const* p) : m_addr(p) {
    // search in "global" region, which also includes txn-invisible region if frozen.
    auto const iter = const_cast<CompactingChunks&>(c).find(p, true);
    if (! iter.first) {
        snprintf(buf, sizeof buf, "position_type::position_type(): cannot find address %p\n", p);
        buf[sizeof buf - 1] = 0;
        throw range_error(buf);
    }
    const_cast<remove_const<decltype(m_chunkId)>::type&>(m_chunkId) = iter.second->id();
}

template<typename iterator>
inline position_type::position_type(void const* p, iterator const& iter) : m_chunkId(iter->id()), m_addr(p) {}

inline id_type position_type::chunkId() const noexcept {
    return m_chunkId;
}
inline void const* position_type::address() const noexcept {
    return m_addr;
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
                (s.last()->id() == m_chunkId && s.last()->range_next() <= super::m_cursor)) {// but that could cause use-after-release
            return true;
        } else {
            if (m_txnBoundary) {
                auto const& rhs = super::to_position();
                if (rhs && less_equal<position_type>()(*m_txnBoundary, *rhs)) {
                    super::m_cursor = nullptr;
                    return true;
                }
            }
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
    if (super::drained()) {
        return true;
    } else if (! super::storage().frozen()) {
        return false;
    } else {
        assert(super::storage().frozenBoundaries());
        assert(super::to_position());
        return super::storage().frozenBoundaries()->right().address() == super::m_cursor ||
            less<position_type>()(super::storage().frozenBoundaries()->right(), *super::to_position());
    }
}

template<typename Chunks, typename Tag, typename E> inline
IterableTableTupleChunks<Chunks, Tag, E>::IteratorObserver::IteratorObserver(
        shared_ptr<typename IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator> const& o) noexcept :
super(o) {}

template<typename Chunks, typename Tag, typename E> inline bool
IterableTableTupleChunks<Chunks, Tag, E>::IteratorObserver::operator()(void const* p) const {
    auto const& o = super::lock();
    if (o == nullptr) {
        return false;
    } else if (o->drained() || ! o->storage().frozenBoundaries()) {
        return true;
    } else {
        assert(o->to_position());
        position_type const pos_p{o->storage(), p};
        return less<position_type>()(pos_p, *o->to_position()) ||               // p < iterator,
            less<position_type>()(o->storage().frozenBoundaries()->right(),     // or p > frozen right boundary
                    pos_p);
    }
}

template<typename Chunks, typename Tag, typename E> inline boost::optional<position_type>
IterableTableTupleChunks<Chunks, Tag, E>::IteratorObserver::to_position() const noexcept {
    auto const& o = super::lock();
    if (o == nullptr) {
        return {};
    } else {
        return o->to_position();
    }
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
FinalizerAndCopier const TxnPreHook<Alloc, Trait, E>::EMPTY_FINALIZER{};

template<typename Alloc, typename Trait, typename E>
inline TxnPreHook<Alloc, Trait, E>::~TxnPreHook() {
    if (m_finalizerAndCopier) {
        for_each(m_changes.cbegin(), m_changes.cend(),
                [this] (typename map_type::value_type const& entry) {
                    m_finalizerAndCopier.finalize(entry.second);
                });
    }
}

template<typename Alloc, typename Trait, typename E> inline
TxnPreHook<Alloc, Trait, E>::TxnPreHook(size_t tupleSize) :
    Trait([this](void const* key) {
                auto const& iter = m_changes.find(key);
                if (iter != m_changes.end()) {
                    m_changes.erase(iter);
                    m_changeStore.free(const_cast<void*>(key));
                }
            }),
    m_changeStore(tupleSize), m_finalizerAndCopier(EMPTY_FINALIZER) {}

template<typename Alloc, typename Trait, typename E> inline
TxnPreHook<Alloc, Trait, E>::TxnPreHook(size_t tupleSize, FinalizerAndCopier const& cb) :
    Trait([this](void const* key) {
                auto const& iter = m_changes.find(key);
                if (iter != m_changes.end()) {
                    m_finalizerAndCopier.finalize(iter->second);
                    m_changes.erase(iter);
                    m_changeStore.free(const_cast<void*>(key));
                }
            }),
    m_changeStore(tupleSize), m_finalizerAndCopier(cb) {}

template<typename Alloc, typename Trait, typename E1>
template<typename IteratorObserver, typename E2> inline void TxnPreHook<Alloc, Trait, E1>::add(
        void const* dst, IteratorObserver& obs) {
    if (m_recording && ! obs(dst)) {
        auto const iter = m_changes.lower_bound(dst);
        if (iter == m_changes.cend() || iter->first != dst) {   // create a fresh copy
            void* fresh = m_changeStore.allocate();
            m_changes.emplace_hint(iter, dst,
                    m_finalizerAndCopier ? m_finalizerAndCopier.copy(fresh, dst) :
                    memcpy(fresh, dst, m_changeStore.tupleSize()));
        }
    }
//    if (m_recording && m_finalizerAndCopier) {
//        m_finalizerAndCopier.finalize(dst);
//    }
}

template<typename Alloc, typename Trait, typename E> inline void TxnPreHook<Alloc, Trait, E>::freeze() {
    if (m_recording) {
        throw logic_error("TxnPreHook::freeze(): double freeze detected");
    } else {
        m_recording = true;
    }
}

template<typename Alloc, typename Trait, typename E> inline void TxnPreHook<Alloc, Trait, E>::thaw() {
    if (m_recording) {
        if (m_finalizerAndCopier) {
            for_each(m_changes.begin(), m_changes.end(),
                    [this](typename map_type::value_type& p) { m_finalizerAndCopier.finalize(p.second); });
        }
        m_moved.clear();
        m_changes.clear();
        m_changeStore.clear();
        m_recording = false;
    } else {
        throw logic_error("TxnPreHook::thaw(): double thaw detected");
    }
}

template<typename Alloc, typename Trait, typename E>
inline void const* TxnPreHook<Alloc, Trait, E>::operator()(void const* src) const {
    auto const& pos = m_changes.find(src);
    return pos == m_changes.cend() ? src : pos->second;
}

template<typename Alloc, typename Trait, typename E> inline bool
TxnPreHook<Alloc, Trait, E>::moved_add(void const* src) {
    return m_moved.emplace(src).second;
}

template<typename Alloc, typename Trait, typename E> inline bool
TxnPreHook<Alloc, Trait, E>::moved_contains(void const* p) const noexcept {
    return m_moved.count(p);
}

template<typename Alloc, typename Trait, typename E>
inline void TxnPreHook<Alloc, Trait, E>::release(void const* src) {
    Trait::remove(src);
}

template<typename Hook, typename E> inline
HookedCompactingChunks<Hook, E>::HookedCompactingChunks(size_t s) noexcept : CompactingChunks(s),
    Hook(s, CompactingChunks::finalizerAndCopier()) {}

template<typename Hook, typename E> inline
HookedCompactingChunks<Hook, E>::HookedCompactingChunks(size_t s, FinalizerAndCopier const& cb) noexcept :
CompactingChunks(s, cb), Hook(s, CompactingChunks::finalizerAndCopier()) {}

template<typename Hook, typename E> inline void* HookedCompactingChunks<Hook, E>::allocate() {
    void* r = CompactingChunks::allocate();
    VOLT_TRACE("allocate() => %p", r);
    return r;
}

template<typename Hook, typename E>
template<typename Tag> inline void HookedCompactingChunks<Hook, E>::clear() {
    CompactingChunks::clear([this] (void const* s) noexcept {
                Hook::add(s, reinterpret_cast<observer_type<Tag>&>(m_iterator_observer));
            });
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::remove(typename CompactingChunks::remove_direction dir, void const* p) {
    if (frozen() && dir == remove_direction::from_head) {
        throw logic_error("HookedCompactingChunks::remove(dir, ptr): "
                "Cannot remove from head when frozen");
    } else {
        VOLT_TRACE("remove(%s, %p)", dir == remove_direction::from_head ? "from_head" : "from_tail", p);
        free(dir, p);
    }
}

template<typename Hook, typename E>
template<typename Tag> inline void HookedCompactingChunks<Hook, E>::update(void* dst) {
    VOLT_TRACE("update(%p)", dst);
    Hook::add(dst, reinterpret_cast<observer_type<Tag>&>(m_iterator_observer));
}

template<typename Hook, typename E>
template<typename Tag> inline
shared_ptr<typename IterableTableTupleChunks<HookedCompactingChunks<Hook, E>, Tag, void>::hooked_iterator>
HookedCompactingChunks<Hook, E>::freeze() {
    CompactingChunks::freeze();
    Hook::freeze();
    auto ptr = make_shared<typename
        IterableTableTupleChunks<HookedCompactingChunks<Hook, E>, Tag, void>::hooked_iterator>(*this);
    // placement new for type erasure. Note that dtor cannot be
    // called explicitly in thaw.
    new (&m_iterator_observer) observer_type<Tag>(ptr);
    return ptr;
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::finalize_range(void const* beg, void const* end) const {
    assert(beg <= end);
    for(auto const* ptr = reinterpret_cast<char const*>(beg); ptr < end; ptr += tupleSize()) {
        if (! this->moved_contains(ptr)) {
            finalizerAndCopier().finalize(ptr);
        }
    }
}

template<typename Hook, typename E>
template<typename Tag> inline void HookedCompactingChunks<Hook, E>::thaw() {
    CompactingChunks::thaw([this](void const* p) noexcept {
            return ! this->moved_contains(p);
        });
    Hook::thaw();
    reinterpret_cast<observer_type<Tag>&>(m_iterator_observer).reset();
}

template<typename Hook, typename E>
inline void HookedCompactingChunks<Hook, E>::remove_add(void* p) {
    CompactingChunks::m_batched.add(p);
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::remove_reserve(size_t n) {
    CompactingChunks::m_batched.reserve(n);
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::remove_reset() {
    CompactingChunks::m_batched.clear(true);
}

template<typename Hook, typename E>
template<typename Tag> inline pair<size_t, size_t>
HookedCompactingChunks<Hook, E>::remove_force(
        function<void(vector<pair<void*, void*>> const&)> const& cb) {
#ifndef NDEBUG
    ostringstream oss;
    oss << "remove_force([removed]: ";
    for_each(CompactingChunks::m_batched.removed().cbegin(),
            CompactingChunks::m_batched.removed().cend(),
            [&oss] (void* s) noexcept { oss << s << ", "; });
    oss.seekp(-2, ios_base::end);
    oss << "\n[moved]: ";
    for_each(CompactingChunks::m_batched.movements().cbegin(),
            CompactingChunks::m_batched.movements().cend(),
            [&oss] (pair<void*, void*> const& s) noexcept {
            oss << s.first << " <- " << s.second << ", ";
            });
    oss.seekp(-2, ios_base::end);
    oss << ")\n";
    VOLT_TRACE("%s", oss.str().c_str());
#endif
    // finalize before memcpy
    auto finalized = CompactingChunks::m_batched.finalize();
    if (frozen()) {            // hook registration on movements only
        for_each(CompactingChunks::m_batched.movements().cbegin(),
                CompactingChunks::m_batched.movements().cend(),
                [this](pair<void*, void*> const& entry) {
                    if (! this->moved_add(entry.second)) {
                        snprintf(buf, sizeof buf, "Failed to add to Hook::m_moved: %p\n",
                                entry.second);
                        buf[sizeof buf - 1] = 0;
                        throw runtime_error(buf);
                    }
                    Hook::add(entry.first,
                            reinterpret_cast<observer_type<Tag>&>(m_iterator_observer));
                });
        finalized += CompactingChunks::m_batched.movements().size();
    }
    cb(CompactingChunks::m_batched.movements());    // memcpy by call back
    return make_pair(CompactingChunks::m_batched.force(), finalized);
}

template<typename Hook, typename E> inline string HookedCompactingChunks<Hook, E>::info(void const* p) const {
    auto* mutable_this = const_cast<HookedCompactingChunks<Hook, E>*>(this);
    auto const iterp = mutable_this->find(p, true);
    if (! iterp.first) {
        snprintf(buf, sizeof buf, "Cannot find address %p\n", p);
        buf[sizeof buf - 1] = 0;
        return buf;
    } else {
        vassert(! beginTxn().empty());
        auto const& iter = iterp.second;
        ostringstream oss;
        oss << "Address " << p << " found at chunk " << iter->id() << ", offset "
            << (reinterpret_cast<char const*>(p) - reinterpret_cast<char const*>(iter->range_begin())) / tupleSize()
            << ", txn 1st chunk = " << beginTxn().iterator()->id() << " [" << beginTxn().iterator()->range_begin()
            << " - " << beginTxn().iterator()->range_next() << "], last chunk = " << last()->id() << " ["
            << last()->range_begin() << " - " << last()->range_next() << "], ";
        if (! frozen()) {
            oss << "not frozen at the call time";
        } else {
            vassert(frozenBoundaries());
            auto const& boundaries = *frozenBoundaries();
            auto const left = mutable_this->find(boundaries.left().chunkId(), true),
                 right = mutable_this->find(boundaries.right().chunkId(), true);
            vassert(left.first);

            auto const* right_begin = right.first ? right.second->range_begin() : nullptr,
                 *right_end = right.first ? right.second->range_end() : nullptr;
            oss << " currently frozen at (" << boundaries.left().chunkId() << " <"
                << boundaries.left().address() << " of " << left.second->range_begin() << " - "
                << left.second->range_end() << ">, " << boundaries.right().chunkId() << " <"
                << boundaries.right().address() << " of " << right_begin << " - "
                << right_end << ">)";
        }
        return oss.str();
    }
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
// HookedCompactingChunks
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
// TxnPreHook
#define TxnPreHookCodegen(alloc)                                                 \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::always>>;       \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::batched>>;      \
template class voltdb::storage::TxnPreHook<alloc, HistoryRetainTrait<gc_policy::never>>
// we do not use compacting chunk for underlying storage
TxnPreHookCodegen(NonCompactingChunks<EagerNonCompactingChunk>);
TxnPreHookCodegen(NonCompactingChunks<LazyNonCompactingChunk>);
#undef TxnPreHookCodegen
// hooked_iterator_type
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
// template member methods
#define HookedMethods4(tag, alloc, gc, alloc2)                                           \
template void TxnPreHook<alloc, HistoryRetainTrait<gc>>::add<typename                    \
        IterableTableTupleChunks<alloc2, tag, void>::IteratorObserver, void>(            \
            void const*,                                                                 \
            typename IterableTableTupleChunks<alloc2, tag, void>::IteratorObserver&)
#define HookedMethods3(tag, alloc, gc)                                                   \
    HookedMethods4(tag, alloc, gc, __codegen__::t1);                                     \
    HookedMethods4(tag, alloc, gc, __codegen__::t2);                                     \
    HookedMethods4(tag, alloc, gc, __codegen__::t3);                                     \
    HookedMethods4(tag, alloc, gc, __codegen__::t4);                                     \
    HookedMethods4(tag, alloc, gc, __codegen__::t5);                                     \
    HookedMethods4(tag, alloc, gc, __codegen__::t6)

#define HookedMethods2(tag, alloc, gc)                                                   \
template shared_ptr<typename IterableTableTupleChunks<                                   \
        HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>, void>, tag, void>::hooked_iterator>    \
HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>, void>::freeze<tag>();  \
template void HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>, void>::thaw<tag>();              \
template void HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>, void>::update<tag>(void*);       \
template void HookedCompactingChunks<TxnPreHook<alloc, HistoryRetainTrait<gc>>, void>::clear<tag>();             \
template pair<size_t, size_t> HookedCompactingChunks<TxnPreHook<alloc,                   \
        HistoryRetainTrait<gc>>, void>::remove_force<tag>(                               \
        function<void(vector<pair<void*, void*>> const&)> const&);                       \
HookedMethods3(tag, alloc, gc)

#define HookedMethods1(tag, alloc)                                                       \
    HookedMethods2(tag, alloc, gc_policy::never);                                        \
    HookedMethods2(tag, alloc, gc_policy::always);                                       \
    HookedMethods2(tag, alloc, gc_policy::batched)
#define HookedMethods(tag)                                                               \
    HookedMethods1(tag, NonCompactingChunks<EagerNonCompactingChunk>);                   \
    HookedMethods1(tag, NonCompactingChunks<LazyNonCompactingChunk>)
HookedMethods(truth);
#undef HookedMethods
#undef HookedMethods1
#undef HookedMethods2
#undef HookedMethods3
#undef HookedMethods4
// # # # # # # # # # # # # # # # # # Codegen: end # # # # # # # # # # # # # # # # # # # # # # #

