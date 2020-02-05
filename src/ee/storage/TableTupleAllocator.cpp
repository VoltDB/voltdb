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
#include <numeric>

using namespace voltdb;
using namespace voltdb::storage;

static char buf[128];

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
inline ChunkHolder::ChunkHolder(size_t id, size_t tupleSize): m_id(id), m_tupleSize(tupleSize) {
    vassert(tupleSize <= 4 * 0x100000);    // individual tuple cannot exceeding 4MB
    auto const size = chunkSize(m_tupleSize);
    m_resource.reset(new char[size]);
    m_next = m_resource.get();
    vassert(m_next != nullptr);
    const_cast<void*&>(m_end) = reinterpret_cast<char*>(m_next) + size;
}

inline size_t ChunkHolder::id() const noexcept {
    return m_id;
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

inline EagerNonCompactingChunk::EagerNonCompactingChunk(size_t id, size_t s): ChunkHolder(id, s) {}

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

inline LazyNonCompactingChunk::LazyNonCompactingChunk(size_t id, size_t tupleSize) : ChunkHolder(id, tupleSize) {}

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

template<typename Chunk, typename E> inline char
ChunkList<Chunk, E>::compare(pair<iterator, void*> const& l, pair<iterator, void*> const& r) const {
    if (l.first == end()) {
        return r.first == end() ? 0 : 1;
    } else if (r.first == end()) {
        return -1;
    } else if (l.first->id() == r.first->id()) {
        vassert(l.first->contains(l.second) && r.first->contains(r.second));
        return l.second < r.second ? -1 : (l.second == r.second ? 0 : 1);
    } else {
        vassert(l.first->contains(l.second) && r.first->contains(r.second));
        return less<Chunk>()(l.first->id(), r.first->id()) ? -1 : 1;
    }
}

template<typename Chunk, typename E>
template<typename... Args>
inline void ChunkList<Chunk, E>::emplace_back(Args&&... args) {
    super::emplace_back(forward<Args>(args)...);
    add(prev(super::end()));
}

template<typename Chunk, typename E>
inline void ChunkList<Chunk, E>::splice(iterator pos, ChunkList& other, iterator it) noexcept {
    m_map.emplace(it->begin(), it);
    other.m_map.erase(it->begin());
    super::splice(pos, other, it);
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

inline void CompactingStorageTrait::LinearizedChunks::emplace(
        typename CompactingStorageTrait::list_type& o,
        typename CompactingStorageTrait::list_type::iterator const& it) noexcept {
    list_type::splice(end(), o, it);
}

inline typename CompactingStorageTrait::LinearizedChunks::reference
CompactingStorageTrait::LinearizedChunks::top() {
    return list_type::front();
}

inline typename CompactingStorageTrait::LinearizedChunks::const_reference
CompactingStorageTrait::LinearizedChunks::top() const {
    return list_type::front();
}

inline void CompactingStorageTrait::LinearizedChunks::pop() {
    list_type::erase(list_type::begin());
}

inline CompactingStorageTrait::LinearizedChunks::ConstExtendedIteratorHelper::ConstExtendedIteratorHelper(
       typename CompactingStorageTrait::LinearizedChunks const& c) noexcept : m_cont(c) {}

inline void CompactingStorageTrait::LinearizedChunks::ConstExtendedIteratorHelper::reset() const noexcept {
    m_val = nullptr;
}

inline void const*
CompactingStorageTrait::LinearizedChunks::ConstExtendedIteratorHelper::operator()() const {
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

inline void const* CompactingStorageTrait::LinearizedChunks::extendedIteratorHelper() {
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
inline typename CompactingStorageTrait::LinearizedChunks::iterator_type
CompactingStorageTrait::LinearizedChunks::iterator() noexcept {
    return [this]() { return extendedIteratorHelper(); };
}

inline typename CompactingStorageTrait::LinearizedChunks::iterator_type
CompactingStorageTrait::LinearizedChunks::iterator() const noexcept {
    m_iterHelper.reset();
    return [this]() { return m_iterHelper(); };
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
        list_type::emplace_back(
                list_type::empty() ? 0 : list_type::back().id() + 1,
                m_tupleSize);
        r = list_type::back().allocate();
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

inline CompactingChunk::CompactingChunk(size_t id, size_t s) : ChunkHolder(id, s) {}

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

inline CompactingStorageTrait::CompactingStorageTrait(list_type* s) noexcept : m_storage(s) {
    vassert(s != nullptr);
}

inline void CompactingStorageTrait::freeze() {
    vassert(m_storage != nullptr);
    if (m_frozen) {
        throw logic_error("Double freeze detected");
    }
    m_frozen = true;
}

inline void CompactingStorageTrait::thaw() {
    vassert(m_storage != nullptr);
    if (m_frozen) {
        m_unreleased.clear();
        m_frozen = false;
    } else {
        throw logic_error("Double thaw detected");
    }
}

/**
 * Immediately free to OS when the tail is unused
 */
inline void CompactingStorageTrait::releasable(typename CompactingStorageTrait::iterator iter) {
    vassert(m_storage != nullptr);
    if (iter->empty()) {
        if (m_frozen) {                // splice or free, depending on any active snapshot
            m_unreleased.emplace(*m_storage, iter);
        } else {
            m_storage->erase(iter);
        }
    }
}

inline function<void const*()> CompactingStorageTrait::operator()() noexcept {
    return m_unreleased.iterator();
}

inline function<void const*()> CompactingStorageTrait::operator()() const noexcept {
    return m_unreleased.iterator();
}

size_t CompactingChunks::s_id = 0;

size_t CompactingChunks::gen_id() {
    return s_id++;
}

inline size_t CompactingChunks::id() const noexcept {
    return m_id;
}

inline CompactingChunks::CompactingChunks(size_t tupleSize) noexcept :
    trait(this), m_id(gen_id()), m_tupleSize(tupleSize), m_batched(*this) {}

// returns non-null value only if in snapshot,
// and the marked position is still in the first chunk.
inline void const* CompactingChunks::endOfFirstChunk() const noexcept {
    if (m_endOfFirstChunk == nullptr || list_type::empty()) {
        return nullptr;
    } else {
        auto const& first = list_type::front();
        return first.begin() < m_endOfFirstChunk && first.end() >= m_endOfFirstChunk ?
            m_endOfFirstChunk : nullptr;
    }
}

inline void CompactingChunks::freeze() {
    if (! list_type::empty()) {
        m_endOfFirstChunk = list_type::begin()->next();
        auto iter = prev(CompactingChunks::end());
        m_frozenSentry = AllocPosition(*iter);
    }
    trait::freeze();
}

inline void CompactingChunks::thaw() {
    m_frozenSentry = AllocPosition();
    m_endOfFirstChunk = nullptr;
    trait::thaw();
}

inline AllocPosition const& CompactingChunks::boundary() const noexcept {
    return m_frozenSentry;
}

size_t CompactingChunks::size() const noexcept {
    return m_allocs;
}

inline void* CompactingChunks::allocate() {
    if (empty() || back().full()) {                  // always allocates from tail
        emplace_back(empty() ? 0 : back().id() + 1, m_tupleSize);
    }
    ++m_allocs;
    return back().allocate();
}

void* CompactingChunks::free(void* dst) {
    auto* pos = find(dst);                 // binary search
    if (pos == nullptr) {
        if (cbegin()->next() == dst) {
            // When shrinking from head, since e.g. for_each<iterator_type>(...)
            // retrieves the address, advances iterator, then calls callable,
            // it is possible that the advanced address is invalidated if it is
            // (effectively) removed by the memory movement.
            --m_allocs;
            return nullptr;
        } else {
            snprintf(buf, sizeof buf, "CompactingChunks::free(%p): invalid address.", dst);
            buf[sizeof buf - 1] = 0;
            throw range_error(buf);
        }
    } else {
        auto from_iter = list_type::begin();      // the tuple from which to memmove
        void* src = from_iter->free();
        auto& dst_iter = *pos;
        if (dst_iter != from_iter) {         // cross-chunk movement needed
            dst_iter->free(dst, src);        // memcpy()
        } else if (src != dst) {             // within-chunk movement (not happened in the previous free() call)
            memcpy(dst, src, tupleSize());
        }
        trait::releasable(from_iter);
        --m_allocs;
        return src;
    }
}

namespace batch_remove_aid {
    template<typename T, typename Cont1> inline static set<T>
    intersection(Cont1 const& t1, set<T> const&& t2) {
        return accumulate(t1.cbegin(), t1.cend(), set<T>{}, [&t2](set<T>& acc, T const& e) {
                if (t2.count(e)) {
                    acc.emplace(e);
                }
                return acc;
            });
    }

    /**
     * Assuming that inter \subset src, removes all elements from
     * src, keeping the rest in order.
     */
    template<typename T> inline static vector<T> subtract(vector<T> src, set<T> const& inter) {
        for (auto const& entry : inter) {
            auto const iter = find(src.begin(), src.end(), entry);
            vassert(iter != src.end());
            src.erase(iter);
        }
        return src;
    }

    template<typename T1, typename T2> inline
    static map<T1, T2> build_map(vector<T1> const&& v1, vector<T2> const&& v2) {
        vassert(v1.size() == v2.size());
        return inner_product(v1.cbegin(), v1.cend(), v2.cbegin(), map<void*, void*>{},
                [](map<void*, void*>& acc, pair<void*, void*> const& entry)
                { acc.emplace(entry); return acc; },
                [](void* p1, void* p2) { return make_pair(p1, p2); });
    }
}

inline CompactingChunks::BatchRemoveAccumulator::BatchRemoveAccumulator(
        CompactingChunks* o) : m_self(o) {
    vassert(o != nullptr);
}

inline CompactingChunks& CompactingChunks::BatchRemoveAccumulator::chunks() noexcept {
    return *m_self;
}

inline typename CompactingChunks::list_type::iterator CompactingChunks::BatchRemoveAccumulator::pop() {
    auto iter = m_self->begin();
    reinterpret_cast<char*&>(iter->m_next) = reinterpret_cast<char*>(iter->begin());
    m_self->releasable(iter);
    return m_self->begin();
}

inline vector<void*> CompactingChunks::BatchRemoveAccumulator::collect() const {
    return accumulate(cbegin(), cend(), vector<void*>{},
            [](vector<void*>& acc, typename map_type::value_type const& entry) {
                copy(entry.second.cbegin(), entry.second.cend(), back_inserter(acc));
                return acc;
            });
}

inline void CompactingChunks::BatchRemoveAccumulator::insert(
        typename CompactingChunks::list_type::iterator key, void* p) {
    auto iter = find(key);
    if (iter == end()) {
        emplace(key, vector<void*>{p});
    } else {
        iter->second.emplace_back(p);
    }
}

inline vector<void*> CompactingChunks::BatchRemoveAccumulator::sorted() {
    return accumulate(begin(), end(), vector<void*>{},
            [](vector<void*>& acc, typename map_type::value_type& entry) {
                auto& val = entry.second;
                std::sort(val.begin(), val.end(), greater<void*>());
                copy(val.cbegin(), val.cend(), back_inserter(acc));
                return acc;
            });
}

inline CompactingChunks::DelayedRemover::DelayedRemover(CompactingChunks& s) : super(&s) {}

inline size_t CompactingChunks::DelayedRemover::add(void* p) {
    auto const* iter = super::chunks().find(p);
    if (iter == nullptr) {
        snprintf(buf, sizeof buf, "CompactingChunk::DelayedRemover::add(%p): invalid address", p);
        buf[sizeof buf - 1] = 0;
        throw range_error(buf);
    } else {
        m_prepared = false;
        super::insert(*iter, p);
        return ++m_size;
    }
}

inline map<void*, void*> const& CompactingChunks::DelayedRemover::movements() const{
    return m_move;
}

inline set<void*> const& CompactingChunks::DelayedRemover::removed() const{
    return m_remove;
}
//
// Helper for batch-free of CompactingChunks
struct CompactingIterator {
    using list_type = ChunkList<CompactingChunk>;
    using iterator_type = list_type::iterator;
    using value_type = pair<iterator_type, void*>;

    CompactingIterator(list_type& c) noexcept : m_cont(c), m_iter(c.begin()),
        m_cursor(reinterpret_cast<char*>(m_iter->next()) - reinterpret_cast<CompactingChunks const&>(c).tupleSize()) {}
    value_type operator*() const noexcept {
        return {m_iter, m_cursor};
    }
    bool drained() const noexcept {
        return m_cursor == nullptr;
    }
    CompactingIterator& operator++() {
        advance();
        return *this;
    }
    CompactingIterator operator++(int) {
        CompactingIterator copy(*this);
        copy.advance();
        return copy;
    }
    bool operator==(CompactingIterator const& o) const noexcept {
        return m_cursor == o.m_cursor;
    }
    bool operator!=(CompactingIterator const& o) const noexcept {
        return ! operator==(o);
    }
private:
    list_type& m_cont;
    iterator_type m_iter;
    void* m_cursor;
    void advance() {
        if (m_cursor == nullptr) {
            throw runtime_error("CompactingIterator drained");
        } else if (m_cursor > m_iter->begin()) {
            reinterpret_cast<char*&>(m_cursor) -=
                reinterpret_cast<CompactingChunks const&>(m_cont).tupleSize();
        } else if (++m_iter == m_cont.end()) {           // drained now
            m_cursor = nullptr;
        } else {
            m_cursor = reinterpret_cast<char*>(m_iter->next()) -
                reinterpret_cast<CompactingChunks const&>(m_cont).tupleSize();
            vassert(m_cursor >= m_iter->begin());
        }
    }
};


typename CompactingChunks::DelayedRemover& CompactingChunks::DelayedRemover::prepare(bool dupCheck) {
    using namespace batch_remove_aid;
    if (! m_prepared) {
        // extra validation: any duplicates with add() calls?
        // This check is spared in the single-call batch-free API
        auto const ptrs = super::collect();
        size_t size = ptrs.size();
        if (dupCheck && size != set<void*>(ptrs.cbegin(), ptrs.cend()).size()) {
            throw runtime_error("Duplicate addresses detected");
        } else {
            vector<void*> addrToRemove{};
            addrToRemove.reserve(size);
            for (auto iter = CompactingIterator(super::chunks()); ! iter.drained();) {
                auto const addr = *iter;
                ++iter;
                addrToRemove.emplace_back(addr.second);
                if (--size == 0) {
                    break;
                }
            }
            m_remove = intersection(ptrs, set<void*>(addrToRemove.cbegin(), addrToRemove.cend()));
            m_move = build_map(subtract(super::sorted(), m_remove),
                    subtract(addrToRemove, m_remove));
            m_prepared = true;
        }
    }
    return *this;
}

size_t CompactingChunks::DelayedRemover::force() {
    auto hd = super::chunks().begin();
    auto const tupleSize = super::chunks().tupleSize(),
        allocsPerTuple = (reinterpret_cast<char const*>(hd->end()) -
                reinterpret_cast<char const*>(hd->begin())) / tupleSize;
    auto const total = m_move.size() + m_remove.size();
    if (total > 0) {
        // storage remapping and clean up
        for_each(m_move.cbegin(), m_move.cend(), [tupleSize](typename map<void*, void*>::value_type const& entry) {
                    memcpy(entry.first, entry.second, tupleSize);
                });
        m_move.clear();
        m_remove.clear();
        auto const offset =
            reinterpret_cast<char const*>(hd->next()) - reinterpret_cast<char const*>(hd->begin());
        // dangerous: direct manipulation on each offended chunks
        for (auto wholeChunks = total / allocsPerTuple; wholeChunks > 0; --wholeChunks) {
            hd = super::pop();
        }
        if (total >= allocsPerTuple) {       // any chunk released at all?
            reinterpret_cast<char*&>(hd->m_next) = reinterpret_cast<char*>(hd->begin()) + offset;
        }
        auto const remBytes = (total % allocsPerTuple) * tupleSize;
        if (remBytes > 0) {          // need manual cursor adjustment on the remaining chunks
            auto const rem = reinterpret_cast<char*>(hd->next()) - reinterpret_cast<char*>(hd->begin());
            if (remBytes >= rem) {
                hd = super::pop();
                reinterpret_cast<char*&>(hd->m_next) -= remBytes - rem;
            } else {
                reinterpret_cast<char*&>(hd->m_next) -= remBytes;
            }
            vassert(hd->next() >= hd->begin());
        }
        super::clear();
    }
    auto const r = m_size;
    m_size = 0;
    return r;
}

inline size_t CompactingChunks::tupleSize() const noexcept {
    return m_tupleSize;
}

template<typename Chunks, typename Tag, typename E> Tag IterableTableTupleChunks<Chunks, Tag, E>::s_tagger{};
template<typename Chunks, typename Tag, typename E> bool const IterableTableTupleChunks<Chunks, Tag, E>::FALSE_VALUE = false;

/**
 * Is it permissible to create a new iterator for the given Chunk
 * list type? We need to ensure that at most one RW iterator can
 * be created at the same time for a compacting chunk list.
 */
template<iterator_view_type, iterator_permission_type,
    typename container_type, typename = typename container_type::Compact>
struct IteratorPermissible {
    void operator()(set<size_t> const&, container_type) const noexcept {
        static_assert(is_lvalue_reference<container_type>::value, "container_type should be reference type");
    }
};

/**
 * Specialization for rw snapshot iterator: check & update map
 */
template<typename container_type>
struct IteratorPermissible<iterator_view_type::snapshot, iterator_permission_type::rw,
    container_type, integral_constant<bool, true>> {
    void operator()(set<size_t>& exists, container_type cont) const {
        static_assert(is_lvalue_reference<container_type>::value, "container_type should be reference type");
        auto iter = exists.find(cont.id());
        if (iter == exists.end()) {            // add entry
            exists.emplace_hint(iter, cont.id());
        } else {
            snprintf(buf, sizeof buf, "Cannot create RW snapshot iterator on chunk list id %lu", cont.id());
            buf[sizeof buf - 1] = 0;
            throw logic_error(buf);
        }
    }
};

/**
 * Correctly de-registers a rw snapshot iterator, allowing one to
 * be created later.
 */
template<iterator_view_type, iterator_permission_type,
    typename container_type, typename = typename container_type::Compact>
struct IteratorDeregistration {
    void operator()(set<size_t> const&, container_type) const noexcept {       // No-op for irrelavent types
        static_assert(is_lvalue_reference<container_type>::value, "container_type should be reference type");
    }
};

template<typename container_type>
struct IteratorDeregistration<iterator_view_type::snapshot, iterator_permission_type::rw,
    container_type, integral_constant<bool, true>> {
    void operator()(set<size_t>& m, container_type cont) const {
        static_assert(is_lvalue_reference<container_type>::value, "container_type should be reference type");
        auto iter = m.find(cont.id());
        if (iter != m.end()) {
            // TODO: we need to also guard against "double deletion" case;
            // but eecheck is currently failing mysteriously
            m.erase(iter);
        }
    }
};

/**
 * The implementation just forward IteratorPermissible
 */
template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline void
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::Constructible::validate(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type src) {
    static IteratorPermissible<view, perm, container_type, typename Chunks::Compact> const validator{};
    validator(m_inUse, src);
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline void
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::Constructible::remove(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type src) {
    static IteratorDeregistration<view, perm, container_type, typename Chunks::Compact> const dereg{};
    dereg(m_inUse, src);
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::Constructible
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::s_constructible;

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view>
inline IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template iterator_type<perm, view>::container_type src,
        bool const& f) :
    m_offset(src.tupleSize()), m_storage(src), m_iter(m_storage.begin()),
    m_cursor(const_cast<value_type>(m_iter == m_storage.end() ? nullptr : m_iter->begin())),
    m_deletedSnapshot(f) {
    // paranoid type check
    static_assert(is_lvalue_reference<container_type>::value,
            "IterableTableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "IterableTableTupleChunks::value_type is not a pointer");
    s_constructible.validate(src);
    while (m_cursor != nullptr && ! s_tagger(m_cursor)) {
        advance();         // calibrate to first non-skipped position
    }
}

template<typename Chunks, typename Tag, typename E>
template<iterator_permission_type perm, iterator_view_type view> inline
IterableTableTupleChunks<Chunks, Tag, E>::iterator_type<perm, view>::~iterator_type() {
    s_constructible.remove(static_cast<container_type>(m_storage));
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
template<typename ChunkList, typename Iter, iterator_view_type view, typename Comp>
struct ChunkBoundary {
    inline void const* operator()(ChunkList const&, Iter const& iter, bool) const noexcept {
        return iter->next();
    }
};

template<typename ChunkList, typename Iter>
struct ChunkBoundary<ChunkList, Iter, iterator_view_type::snapshot, integral_constant<bool, true>> {
    inline void const* operator()(ChunkList const& l, Iter const& iter, bool flag) const noexcept {
        if (flag && iter == l.begin()) {
            void const* end = reinterpret_cast<CompactingChunks const&>(l).endOfFirstChunk();
            if (end != nullptr) {              // The first chunk in txn is the first chunk when snapshot started
                return end;
            } else if (l.size() > 1) {         // The first chunk when snapshot started had been "reclaimed" in txn view,
                return iter->end();            // and the first chunk in current txn view is not the last chunk: frozen view needs
                // (and is safe) to cover the whole first chunk
            } else {                           // There is now a single chunk in txn view, which is not the first chunk
                return iter->next();           // when we froze.
            }
        } else {
            return iter->next();
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
template<typename Hook, iterator_permission_type perm>
inline IterableTableTupleChunks<Chunks, Tag, E>::time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type(
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::container_type c,
        typename IterableTableTupleChunks<Chunks, Tag, E>::template time_traveling_iterator_type<Hook, perm>::time_traveling_iterator_type::history_type h) :
    super(c, [&h](value_type c) { return const_cast<value_type>(h.reverted(const_cast<value_type>(c))); }, h.hasDeletes()),
    m_extendingCb(c()),
    m_extendingPtr(m_extendingCb()) {
    if (m_extendingPtr != nullptr) {           // perform translation layer right away
        m_extendingPtr = super::m_cb(const_cast<value_type>(m_extendingPtr));
    }
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
            if (m_extendingPtr != nullptr) {           // perform translation layer right away
                m_extendingPtr = super::m_cb(const_cast<value_type>(m_extendingPtr));
            }
        } else {
            super::advance();
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
    return m_extendingPtr == nullptr ? super::operator*() : const_cast<void*>(m_extendingPtr);
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
template<iterator_permission_type perm> inline bool
IterableTableTupleChunks<Chunks, Tag, E>::hooked_iterator_type<perm>::drained() const noexcept {
    auto const& boundary = reinterpret_cast<container_type const&>(super::storage()).boundary();
    return super::drained() || (
            super::m_extendingPtr == nullptr &&    // not in spliced chunks;
            ! boundary.empty() &&                  // snapshot in progress,
            less_equal<AllocPosition>()(boundary, {super::m_cursor, super::list_iterator()}));
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

inline AllocPosition::AllocPosition(ChunkHolder const& c) noexcept :
m_lastChunkId(c.id()), m_lastAlloc(c.next()) {}

inline AllocPosition::AllocPosition(CompactingChunks const& c, void const* p) : m_lastAlloc(p) {
    auto const* iterp = c.find(p);
    if (iterp == nullptr || ! ((*iterp)->contains(p) || (*iterp)->end() > p)) {
        // NOTE: it is possible that the txn view does not
        // contain the ptr, as the chunk has been removed. In
        // that case, we simply "empty" it, and note the
        // semantics of less<AllocPosition>.
        const_cast<void*&>(m_lastAlloc) = nullptr;
    } else {
        const_cast<size_t&>(m_lastChunkId) = (*iterp)->id();
    }
}

template<typename iterator>
inline AllocPosition::AllocPosition(void const* p, iterator const& iter) noexcept :
m_lastChunkId(iter->id()), m_lastAlloc(p) {}

inline size_t AllocPosition::lastChunkId() const noexcept {
    return m_lastChunkId;
}
inline void const* AllocPosition::lastAlloc() const noexcept {
    return m_lastAlloc;
}
inline bool AllocPosition::empty() const noexcept {
    return m_lastAlloc == nullptr;
}

inline AllocPosition& AllocPosition::operator=(AllocPosition const& o) noexcept {
    const_cast<size_t&>(m_lastChunkId) = o.lastChunkId();
    m_lastAlloc = o.lastAlloc();
    return *this;
}

namespace std {
    using namespace voltdb::storage;
    template<> struct less<AllocPosition> {
        inline bool operator()(AllocPosition const& lhs, AllocPosition const& rhs) const noexcept {
            bool const e1 = lhs.empty(), e2 = rhs.empty();
            if (e1 || e2) { // NOTE: anything but empty < empty, and empty < anything but empty.
                return ! (e1 && e2);               // That is, empty !< empty (since empty == empty)
            } else {
                auto const id1 = lhs.lastChunkId(), id2 = rhs.lastChunkId();
                return (id1 == id2 && lhs.lastAlloc() < rhs.lastAlloc()) || less_rolling(id1, id2);
            }
        }
    };
    template<> struct less_equal<AllocPosition> {
        inline bool operator()(AllocPosition const& lhs, AllocPosition const& rhs) const noexcept {
            return lhs.lastAlloc() == rhs.lastAlloc() || less<AllocPosition>()(lhs, rhs);
        }
    };
    template<> struct less<ChunkHolder> {
        inline bool operator()(ChunkHolder const& lhs, ChunkHolder const& rhs) const noexcept {
            return less_rolling(lhs.id(), rhs.id());
        }
    };
}

template<typename Alloc, typename Trait, typename C, typename E>
inline TxnPreHook<Alloc, Trait, C, E>::TxnPreHook(
        size_t tupleSize, AllocPosition const& boundary):
    Trait([this](void const* key) {
                m_changes.erase(key);                        // no-op for non-existent key
                m_copied.erase(key);
                m_storage.tryFree(const_cast<void*>(key));
            }),
    m_storage(tupleSize), m_boundary(boundary) {}

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
inline void TxnPreHook<Alloc, Trait, C, E>::add(CompactingChunks const& t,
        typename TxnPreHook<Alloc, Trait, C, E>::ChangeType type, void const* dst) {
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

template<typename Alloc, typename Trait, typename C, typename E> inline void TxnPreHook<Alloc, Trait, C, E>::freeze() {
    if (m_recording) {
        throw logic_error("Double freeze detected");
    } else {
        m_recording = true;
    }
}

template<typename Alloc, typename Trait, typename C, typename E> inline void TxnPreHook<Alloc, Trait, C, E>::thaw() {
    if (m_recording) {
        m_changes.clear();
        m_copied.clear();
        m_storage.clear();
        m_hasDeletes = m_recording = false;
    } else {
        throw logic_error("Double thaw detected");
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
inline void TxnPreHook<Alloc, Trait, C, E>::insert(void const* dst) {
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of. Just mark the position as
        // previously unused.
        m_changes.emplace(dst, nullptr);
    }
}

template<typename Alloc, typename Trait, typename C, typename E>
inline void TxnPreHook<Alloc, Trait, C, E>::remove(void const* src) {
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

template<typename Hook, typename E> inline
HookedCompactingChunks<Hook, E>::HookedCompactingChunks(size_t s) noexcept :
    CompactingChunks(s), Hook(s, CompactingChunks::boundary()) {}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::freeze() {
    Hook::freeze();
    CompactingChunks::freeze();
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::thaw() {
    Hook::thaw();
    CompactingChunks::thaw();
}

template<typename Hook, typename E> inline void const*
HookedCompactingChunks<Hook, E>::insert(void const* src) {
    void const* r = memcpy(CompactingChunks::allocate(), src, CompactingChunks::tupleSize());
    Hook::add(*this, Hook::ChangeType::Insertion, r);
    return r;
}

template<typename Hook, typename E> inline void
HookedCompactingChunks<Hook, E>::update(void* dst, void const* src) {
    Hook::add(*this, Hook::ChangeType::Update, dst);
    memcpy(dst, src, CompactingChunks::tupleSize());
}

template<typename Hook, typename E> inline void const*
HookedCompactingChunks<Hook, E>::remove(void* dst) {
    Hook::copy(dst);
    void const* src = CompactingChunks::free(dst);
    Hook::add(*this, Hook::ChangeType::Deletion, dst);
    return src;
}

template<typename Hook, typename E> inline void HookedCompactingChunks<Hook, E>::remove(
        set<void*> const& src, function<void(map<void*, void*>const&)> const& cb) {
    using Remover = typename CompactingChunks::DelayedRemover;
    auto batch = accumulate(src.cbegin(), src.cend(), Remover{*this},
            [](Remover& batch, void* p) {
                batch.add(p);
                return batch;
            }).prepare(false);
    cb(batch.movements());
    // hook registration
    for_each(batch.removed().cbegin(), batch.removed().cend(),
            [this](void* s) {
                Hook::copy(s);
                Hook::add(*this, Hook::ChangeType::Deletion, s);
            });
    for_each(batch.movements().cbegin(), batch.movements().cend(),
            [this](pair<void*, void*> const& entry) {
                Hook::copy(entry.first);
                Hook::add(*this, Hook::ChangeType::Deletion, entry.first);
            });
    batch.force();
}

template<typename Hook, typename E> inline size_t HookedCompactingChunks<Hook, E>::remove_add(void* p) {
    return CompactingChunks::m_batched.add(p);
}

template<typename Hook, typename E> inline map<void*, void*> const& HookedCompactingChunks<Hook, E>::remove_moves() {
    return CompactingChunks::m_batched.prepare(true).movements();
}

template<typename Hook, typename E> inline size_t HookedCompactingChunks<Hook, E>::remove_force() {
    // hook registration
    for_each(CompactingChunks::m_batched.removed().cbegin(), CompactingChunks::m_batched.removed().cend(),
            [this](void* s) {
                Hook::copy(s);
                Hook::add(*this, Hook::ChangeType::Deletion, s);
            });
    for_each(remove_moves().cbegin(), remove_moves().cend(),
            [this](pair<void*, void*> const& entry) {
                Hook::copy(entry.first);
                Hook::add(*this, Hook::ChangeType::Deletion, entry.first);
            });
    return CompactingChunks::m_batched.prepare(true).force();
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
TTIteratorCodegen(CompactingChunks);
TTIteratorCodegen(__codegen__::t1);
TTIteratorCodegen(__codegen__::t2);
TTIteratorCodegen(__codegen__::t3);
TTIteratorCodegen(__codegen__::t4);
TTIteratorCodegen(__codegen__::t5);
TTIteratorCodegen(__codegen__::t6);

#undef TTIteratorCodegen
#undef TTIteratorCodegen1
#undef TTIteratorCodegen2
#undef TTIteratorCodegen3
#undef TTIteratorCodegen4
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
// # # # # # # # # # # # # # # # # # Codegen: end # # # # # # # # # # # # # # # # # # # # # # #

