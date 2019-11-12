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

inline ChangeHistory::Change::Change(void const* c, size_t len):
    m_tuple(c == nullptr ? nullptr : new char[len]) {
    if (c != nullptr) {
        memcpy(m_tuple.get(), c, len);
    }
}

inline void const* ChangeHistory::Change::get() const {
    return m_tuple.get();
}

inline ChangeHistory::ChangeHistory(size_t tupleSize): m_tupleSize(tupleSize) {}

inline void ChangeHistory::add(ChangeType type, void const* src, void const* dst) {
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

inline void ChangeHistory::start() {
    m_recording = true;
}

inline void ChangeHistory::stop() {
    m_recording = false;
    m_changes.clear();
}

inline void ChangeHistory::update(void const* src, void const* dst) {
    // src tuple from temp table written to dst in persistent storage
    if (m_recording && ! m_changes.count(dst)) {
        m_changes.emplace(dst, Change{dst, m_tupleSize});
    }
}

inline void ChangeHistory::insert(void const* src, void const* dst) {
    if (m_recording && ! m_changes.count(dst)) {
        // for insertions, since previous memory is unused, there
        // is nothing to keep track of
        m_changes.emplace(dst, Change{nullptr, m_tupleSize});
    }
}

inline void ChangeHistory::remove(void const* src, void const* dst) {
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

inline void const* ChangeHistory::reverted(void const* src) const {
    auto pos = m_changes.find(src);
    return pos == m_changes.cend() ? nullptr : pos->second.get();
}

inline TableTupleChunk::TableTupleChunk(size_t tupleSize):
    m_tupleSize(tupleSize),
    m_resource(new char(tupleSize * ALLOCS_PER_CHUNK)),
    m_begin(m_resource.get()),
    m_end(reinterpret_cast<char*const>(m_begin) + tupleSize * ALLOCS_PER_CHUNK),
    m_next(m_begin) {}

inline TableTupleChunk::TableTupleChunk(TableTupleChunk&& rhs):
    m_tupleSize(rhs.m_tupleSize), m_begin(rhs.m_begin),
    m_end(rhs.m_end), m_next(rhs.m_next) {
    const_cast<void*&>(rhs.m_begin) = nullptr;
}

inline void* TableTupleChunk::allocate() noexcept {
    if (m_next >= m_end) {                 // current block full
        return nullptr;
    } else {
        void* res = m_next;
        reinterpret_cast<char*&>(m_next) += m_tupleSize;
        return res;
    }
}

inline bool TableTupleChunk::contains(void const* which) const {
    // check alignment
    vassert(which < m_begin || which >= m_end || 0 ==
            (reinterpret_cast<char const*>(which) - reinterpret_cast<char*const>(m_begin)) % m_tupleSize);
    return which >= m_begin && which < m_next;
}

inline void const* TableTupleChunk::begin() const noexcept {
    return m_begin;
}

inline void const* TableTupleChunk::end() const noexcept {
    return m_next;
}

inline bool TableTupleChunk::full() const noexcept {
    return m_next == m_end;
}

inline void TableTupleChunk::free(void* dst, void const* src) {
    vassert(contains(dst));
    vassert(! contains(src));
    memcpy(dst, src, m_tupleSize);
    // current chunk is the last in the list: update alloc cursor
    reinterpret_cast<char*&>(m_next) -= m_tupleSize;
    vassert(m_next >= m_begin);
}

inline void* TableTupleChunk::free(void* dst) {                            // witin-chunk free
    vassert(contains(dst));
    vassert(m_next > m_begin);
    reinterpret_cast<char*&>(m_next) -= m_tupleSize;
    if (dst != m_next) {
        memcpy(dst, m_next, m_tupleSize);
    }                                      // else freeing last tuple in the chunk: no movement needed
    return m_next;
}

template<bool Const>
inline TableTupleChunks::iterator_type<Const>::iterator_type(
        typename TableTupleChunks::iterator_type<Const>::container_type src):
    m_offset(src.m_tupleSize), m_list(src.m_list),
    m_iter(m_list.begin()),
    m_cursor(const_cast<value_type>(m_iter->begin())) {
    static_assert(is_reference<container_type>::value,
            "TableTupleChunks::iterator_type::container_type is not a reference");
    static_assert(is_pointer<value_type>::value,
            "TableTupleChunks::value_type is not a pointer");
}

template<bool Const>
inline bool TableTupleChunks::iterator_type<Const>::operator==(
        iterator_type const& o) const noexcept {
    return m_cursor == o.m_cursor;
}

template<bool Const>
void TableTupleChunks::iterator_type<Const>::advance() {
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
inline TableTupleChunks::iterator_type<Const>&
TableTupleChunks::iterator_type<Const>::operator++() {
    advance();
    return *this;
}

template<bool Const>
inline TableTupleChunks::iterator_type<Const>
TableTupleChunks::iterator_type<Const>::operator++(int) {
    TableTupleChunks::iterator_type copy(*this);
    advance();
    return copy;
}

template<bool Const>
inline typename TableTupleChunks::iterator_type<Const>::reference
TableTupleChunks::iterator_type<Const>::operator*() noexcept {
    return m_cursor;
}

inline TableTupleChunks::iterator TableTupleChunks::begin() {
    return iterator(*this);
}

inline TableTupleChunks::iterator TableTupleChunks::end() {
    iterator iter(*this);
    iter.m_cursor = nullptr;               // see iterator::operator== and iterator::advance()
    return iter;
}

inline TableTupleChunks::const_iterator TableTupleChunks::cbegin() const {
    return const_iterator(*this);
}

inline TableTupleChunks::const_iterator TableTupleChunks::cend() const {
    const_iterator iter(*this);
    const_cast<remove_const<decltype(iter.m_cursor)>::type&>(iter.m_cursor) = nullptr;               // see iterator::operator== and iterator::advance()
    return iter;
}

inline TableTupleChunks::const_iterator TableTupleChunks::begin() const {
    return cbegin();
}

inline TableTupleChunks::const_iterator TableTupleChunks::end() const {
    return cend();
}

template<bool Const>
inline TableTupleChunks::iterator_cb_type<Const>::iterator_cb_type(
        container_type c, cb_type cb): super(c), m_cb(cb) {}

template<bool Const>
inline typename TableTupleChunks::iterator_cb_type<Const>::value_type
TableTupleChunks::iterator_cb_type<Const>::operator*() {
    return m_cb(super::operator*());
}

template<bool Const>
inline TableTupleChunks::time_traveling_iterator_type<Const>::time_traveling_iterator_type(
        typename TableTupleChunks::time_traveling_iterator_type<Const>::time_traveling_iterator_type::container_type c,
        ChangeHistory const& h) :
    super(c, [&h](typename super::value_type c){
                using type = typename super::value_type;
                return const_cast<type>(h.reverted(const_cast<type>(c)));
            }) {}

void* TableTupleChunks::allocate() noexcept {
    if (m_list.empty() || m_list.back().full()) {
        m_list.emplace_back(m_tupleSize);
    }
    return m_list.back().allocate();
}

inline TableTupleChunks::iterator_cb TableTupleChunks::begin(ChangeHistory const& h) {
    return iterator_cb(*this, h);
}

inline TableTupleChunks::iterator_cb TableTupleChunks::end(ChangeHistory const& h) {
    iterator_cb iter(*this, h);
    iter.m_cursor = nullptr;
    return iter;
}

inline TableTupleChunks::const_iterator_cb TableTupleChunks::cbegin(ChangeHistory const& h) const {
    return const_iterator_cb(*this, h);
}

inline TableTupleChunks::const_iterator_cb TableTupleChunks::cend(ChangeHistory const& h) const {
    const_iterator_cb iter(*this, h);
    iter.m_cursor = nullptr;
    return iter;
}

inline TableTupleChunks::const_iterator_cb TableTupleChunks::begin(ChangeHistory const& h) const {
    return cbegin(h);
}

inline TableTupleChunks::const_iterator_cb TableTupleChunks::end(ChangeHistory const& h) const {
    return cend(h);
}

inline TableTupleChunks::iterator begin(TableTupleChunks& c) {
    return c.begin();
}
inline TableTupleChunks::iterator end(TableTupleChunks& c) {
    return c.end();
}

inline TableTupleChunks::const_iterator cbegin(TableTupleChunks const& c) {
    return c.cbegin();
}
inline TableTupleChunks::const_iterator cend(TableTupleChunks const& c) {
    return c.cend();
}

inline TableTupleChunks::const_iterator begin(TableTupleChunks const& c) {
    return c.cbegin();
}
inline TableTupleChunks::const_iterator end(TableTupleChunks const& c) {
    return c.cend();
}

inline TableTupleChunks::iterator_cb begin(TableTupleChunks& c, ChangeHistory const& h) {
    return c.begin(h);
}
inline TableTupleChunks::iterator_cb end(TableTupleChunks& c, ChangeHistory const& h) {
    return c.end(h);
}

inline TableTupleChunks::const_iterator_cb cbegin(TableTupleChunks const& c, ChangeHistory const& h) {
    return c.cbegin(h);
}
inline TableTupleChunks::const_iterator_cb cend(TableTupleChunks const& c, ChangeHistory const& h) {
    return c.cend(h);
}

inline TableTupleChunks::const_iterator_cb begin(TableTupleChunks const& c, ChangeHistory const& h) {
    return c.begin(h);
}
inline TableTupleChunks::const_iterator_cb end(TableTupleChunks const& c, ChangeHistory const& h) {
    return c.end(h);
}

void* TableTupleChunks::free(void* dst) {
    auto which = find_if(m_list.begin(), m_list.end(),
            [dst](TableTupleChunk const& c) { return c.contains(dst); });
    if (which == m_list.cend()) {
        throw std::runtime_error("Address not found");
    } else if (&*which == &m_list.back()) {     // from last chunk => no cross-chunk movement needed
        return m_list.back().free(dst);
    } else {
        TableTupleChunk& last = m_list.back();
        void* src = last.free(
                reinterpret_cast<char*>(const_cast<void*>(last.end())) - m_tupleSize);
        which->free(dst, src);
        return src;
    }
}

bool TableTupleChunks::less(void const* lhs, void const* rhs) const {
    if (lhs == rhs) {
        return false;
    } else {
        // linear search in chunks
        bool found1 = false, found2 = false;
        auto const pos = find_if(m_list.cbegin(), m_list.cend(),
                [&found1, &found2, lhs, rhs](TableTupleChunk const& c) {
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

