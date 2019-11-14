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

template<typename Alloc>
inline ChangeHistory<Alloc>::Change::Change(void const* c, size_t len):
    m_tuple(c == nullptr ? nullptr : Alloc::alloc(len), Alloc::dealloc) {
    if (c != nullptr) {
        memcpy(m_tuple.get(), c, len);
    }
}

template<typename Alloc>
inline void const* ChangeHistory<Alloc>::Change::get() const {
    return m_tuple.get();
}

template<typename Alloc>
inline ChangeHistory<Alloc>::ChangeHistory(size_t tupleSize): m_tupleSize(tupleSize) {}

template<typename Alloc>
inline void ChangeHistory<Alloc>::add(ChangeType type, void const* src, void const* dst) {
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

template<typename Alloc>
inline void ChangeHistory<Alloc>::start() {
    m_recording = true;
}

template<typename Alloc>
inline void ChangeHistory<Alloc>::stop() {
    m_recording = false;
    m_changes.clear();
}

template<typename Alloc>
inline void ChangeHistory<Alloc>::update(void const* src, void const* dst) {
    // src tuple from temp table written to dst in persistent storage
    if (m_recording && ! m_changes.count(dst)) {
        m_changes.emplace(dst, Change{dst, m_tupleSize});
    }
}

template<typename Alloc>
inline void ChangeHistory<Alloc>::insert(void const* src, void const* dst) {
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of
        m_changes.emplace(dst, Change{nullptr, m_tupleSize});
    }
}

template<typename Alloc>
inline void ChangeHistory<Alloc>::remove(void const* src, void const* dst) {
    // src tuple is deleted, and tuple at dst gets moved to src
    if (m_recording) {
        if (! m_changes.count(src)) {
            m_changes.emplace(src, Change{src, m_tupleSize});
        }
        if (! m_changes.count(dst)) {
            m_changes.emplace(dst, Change{dst, m_tupleSize});
        }
    }
}

template<typename Alloc>
inline void const* ChangeHistory<Alloc>::reverted(void const* src) const {
    auto pos = m_changes.find(src);
    if (pos == m_changes.cend()) {         // not dirty
        return src;
    } else {                               // dirty, but original tuple was empty in INSERT
        auto const* substitution = pos->second.get();
        return substitution == nullptr ? src : substitution;
    }
}

template<typename Alloc>
inline TableTupleChunk<Alloc>::TableTupleChunk(size_t tupleSize):
    m_tupleSize(tupleSize),
    m_resource(Alloc::alloc(tupleSize * ALLOCS_PER_CHUNK), Alloc::free),
    m_begin(m_resource.get()),
    m_end(reinterpret_cast<char*const>(m_begin) + tupleSize * ALLOCS_PER_CHUNK),
    m_next(m_begin) {}

template<typename Alloc>
inline TableTupleChunk<Alloc>::TableTupleChunk(TableTupleChunk&& rhs):
    m_tupleSize(rhs.m_tupleSize), m_begin(rhs.m_begin),
    m_end(rhs.m_end), m_next(rhs.m_next) {
    const_cast<void*&>(rhs.m_begin) = nullptr;
}

template<typename Alloc>
inline void* TableTupleChunk<Alloc>::allocate() noexcept {
    if (m_next >= m_end) {                 // current block full
        return nullptr;
    } else {
        void* res = m_next;
        reinterpret_cast<char*&>(m_next) += m_tupleSize;
        return res;
    }
}

template<typename Alloc>
inline bool TableTupleChunk<Alloc>::contains(void const* which) const {
    // check alignment
    vassert(which < m_begin || which >= m_end || 0 ==
            (reinterpret_cast<char const*>(which) - reinterpret_cast<char*const>(m_begin)) % m_tupleSize);
    return which >= m_begin && which < m_next;
}

template<typename Alloc>
inline void const* TableTupleChunk<Alloc>::begin() const noexcept {
    return m_begin;
}

template<typename Alloc>
inline void const* TableTupleChunk<Alloc>::end() const noexcept {
    return m_next;
}

template<typename Alloc>
inline bool TableTupleChunk<Alloc>::full() const noexcept {
    return m_next == m_end;
}

template<typename Alloc>
inline void TableTupleChunk<Alloc>::free(void* dst, void const* src) {
    vassert(contains(dst));
    vassert(! contains(src));
    memcpy(dst, src, m_tupleSize);
    // current chunk is the last in the list: update alloc cursor
    reinterpret_cast<char*&>(m_next) -= m_tupleSize;
    vassert(m_next >= m_begin);
}

template<typename Alloc>
inline void* TableTupleChunk<Alloc>::free(void* dst) {                            // witin-chunk free
    vassert(contains(dst));
    vassert(m_next > m_begin);
    reinterpret_cast<char*&>(m_next) -= m_tupleSize;
    if (dst != m_next) {
        memcpy(dst, m_next, m_tupleSize);
    }                                      // else freeing last tuple in the chunk: no movement needed
    return m_next;
}

template<typename Alloc>
template<bool Const>
inline TableTupleChunks<Alloc>::iterator_type<Const>::iterator_type(
        typename TableTupleChunks<Alloc>::template iterator_type<Const>::container_type src):
    m_offset(src.m_tupleSize), m_list(src.m_list),
    m_iter(m_list.begin()),
    m_cursor(const_cast<value_type>(m_iter->begin())) {
    static_assert(is_reference<container_type>::value,
            "TableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "TableTupleChunks::value_type is not a pointer");
}

template<typename Alloc>
template<bool Const>
inline bool TableTupleChunks<Alloc>::iterator_type<Const>::operator==(
        iterator_type const& o) const noexcept {
    return m_cursor == o.m_cursor;
}

template<typename Alloc>
template<bool Const>
void TableTupleChunks<Alloc>::iterator_type<Const>::advance() {
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

template<typename Alloc>
template<bool Const>
inline TableTupleChunks<Alloc>::iterator_type<Const>&
TableTupleChunks<Alloc>::iterator_type<Const>::operator++() {
    advance();
    return *this;
}

template<typename Alloc>
template<bool Const>
inline TableTupleChunks<Alloc>::iterator_type<Const>
TableTupleChunks<Alloc>::iterator_type<Const>::operator++(int) {
    TableTupleChunks<Alloc>::iterator_type copy(*this);
    advance();
    return copy;
}

template<typename Alloc>
template<bool Const>
inline typename TableTupleChunks<Alloc>::template iterator_type<Const>::reference
TableTupleChunks<Alloc>::iterator_type<Const>::operator*() noexcept {
    return m_cursor;
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator TableTupleChunks<Alloc>::begin() {
    return iterator(*this);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator TableTupleChunks<Alloc>::end() {
    iterator iter(*this);
    iter.m_cursor = nullptr;               // see iterator::operator== and iterator::advance()
    return iter;
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator TableTupleChunks<Alloc>::cbegin() const {
    return const_iterator(*this);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator TableTupleChunks<Alloc>::cend() const {
    const_iterator iter(*this);
    const_cast<typename remove_const<decltype(iter.m_cursor)>::type&>(iter.m_cursor) = nullptr;
    return iter;
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator TableTupleChunks<Alloc>::begin() const {
    return cbegin();
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator TableTupleChunks<Alloc>::end() const {
    return cend();
}

template<typename Alloc>
template<bool Const>
inline TableTupleChunks<Alloc>::iterator_cb_type<Const>::iterator_cb_type(
        container_type c, cb_type cb): super(c), m_cb(cb) {}

template<typename Alloc>
template<bool Const>
inline typename TableTupleChunks<Alloc>::template iterator_cb_type<Const>::value_type
TableTupleChunks<Alloc>::iterator_cb_type<Const>::operator*() {
    return m_cb(super::operator*());
}

template<typename Alloc>
template<bool Const>
inline TableTupleChunks<Alloc>::time_traveling_iterator_type<Const>::time_traveling_iterator_type(
        typename TableTupleChunks<Alloc>::template
        time_traveling_iterator_type<Const>::time_traveling_iterator_type::container_type c,
        ChangeHistory<Alloc> const& h) :
    super(c, [&h](typename super::value_type c){
                using type = typename super::value_type;
                return const_cast<type>(h.reverted(const_cast<type>(c)));
            }) {}

template<typename Alloc>
void* TableTupleChunks<Alloc>::allocate() noexcept {
    if (m_list.empty() || m_list.back().full()) {
        m_list.emplace_back(m_tupleSize);
    }
    return m_list.back().allocate();
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator_cb TableTupleChunks<Alloc>::begin(
        ChangeHistory<Alloc> const& h) {
    return iterator_cb(*this, h);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator_cb TableTupleChunks<Alloc>::end(
        ChangeHistory<Alloc> const& h) {
    iterator_cb iter(*this, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb TableTupleChunks<Alloc>::cbegin(
        ChangeHistory<Alloc> const& h) const {
    return const_iterator_cb(*this, h);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb TableTupleChunks<Alloc>::cend(
        ChangeHistory<Alloc> const& h) const {
    const_iterator_cb iter(*this, h);
    iter.m_cursor = nullptr;
    return iter;
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb TableTupleChunks<Alloc>::begin(
        ChangeHistory<Alloc> const& h) const {
    return cbegin(h);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb TableTupleChunks<Alloc>::end(
        ChangeHistory<Alloc> const& h) const {
    return cend(h);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator begin(TableTupleChunks<Alloc>& c) {
    return c.begin();
}
template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator end(TableTupleChunks<Alloc>& c) {
    return c.end();
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator cbegin(TableTupleChunks<Alloc> const& c) {
    return c.cbegin();
}
template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator cend(TableTupleChunks<Alloc> const& c) {
    return c.cend();
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator begin(TableTupleChunks<Alloc> const& c) {
    return c.cbegin();
}
template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator end(TableTupleChunks<Alloc> const& c) {
    return c.cend();
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator_cb begin(
        TableTupleChunks<Alloc>& c, ChangeHistory<Alloc> const& h) {
    return c.begin(h);
}
template<typename Alloc>
inline typename TableTupleChunks<Alloc>::iterator_cb end(
        TableTupleChunks<Alloc>& c, ChangeHistory<Alloc> const& h) {
    return c.end(h);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb cbegin(
        TableTupleChunks<Alloc> const& c, ChangeHistory<Alloc> const& h) {
    return c.cbegin(h);
}
template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb cend(
        TableTupleChunks<Alloc> const& c, ChangeHistory<Alloc> const& h) {
    return c.cend(h);
}

template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb begin(
        TableTupleChunks<Alloc> const& c, ChangeHistory<Alloc> const& h) {
    return c.begin(h);
}
template<typename Alloc>
inline typename TableTupleChunks<Alloc>::const_iterator_cb end(
        TableTupleChunks<Alloc> const& c, ChangeHistory<Alloc> const& h) {
    return c.end(h);
}

template<typename Alloc>
void* TableTupleChunks<Alloc>::free(void* dst) {
    auto which = find_if(m_list.begin(), m_list.end(),
            [dst](TableTupleChunk<Alloc> const& c) { return c.contains(dst); });
    if (which == m_list.cend()) {
        throw std::runtime_error("Address not found");
    } else if (&*which == &m_list.back()) {     // from last chunk => no cross-chunk movement needed
        return m_list.back().free(dst);
    } else {
        auto& last = m_list.back();
        void* src = last.free(
                reinterpret_cast<char*>(const_cast<void*>(last.end())) - m_tupleSize);
        which->free(dst, src);
        return src;
    }
}

template<typename Alloc>
bool TableTupleChunks<Alloc>::less(void const* lhs, void const* rhs) const {
    if (lhs == rhs) {
        return false;
    } else {
        // linear search in chunks
        bool found1 = false, found2 = false;
        auto const pos = find_if(m_list.cbegin(), m_list.cend(),
                [&found1, &found2, lhs, rhs](TableTupleChunk<Alloc> const& c) {
                    found1 |= c.contains(lhs);
                    found2 |= c.contains(rhs);
                    return found1 || found2;
                });
        vassert(pos != m_list.cend());     // neither address is found anywhere
        if (found1 && !found2) {
            return true;
        } else {
            return found1 && found2 &&     // in the same chunk
                lhs < rhs;
        }
    }
}

