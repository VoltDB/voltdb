/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

#include "common/tabletuple.h"

#include <boost/iterator/iterator_facade.hpp>

#include <vector>
#include <limits>

namespace voltdb {
    namespace __private__ {     // In a different namespace since we already have ValueType enum from "common/types.h"
        template<bool Const> using ValueType =
            typename std::conditional<Const, uint64_t const, uint64_t>::type;
    }

class TableTupleFilter;

/**
 * Non-const or constant TableTupleFilter Iterator. Implements FORWARD ITERATOR Concept.
 * Iterates over the tuples that have a certain value set in
 * underline TableTupleFilter.
 *
 * Whether this iterator is const or not depends on the value of second
 * template parameter.
 *
 * Parameter MARKER specifies the value to look for. Only tuples that have
 * their value set to MARKER will be iterated over
 */
template<int8_t Marker, bool Const>
class TableTupleFilterIterType : public boost::iterator_facade<
                                 TableTupleFilterIterType<Marker, Const>,
                                 __private__::ValueType<Const>,
                                 boost::forward_traversal_tag> {
public:
    using ValueType = __private__::ValueType<Const>;
private:
    const TableTupleFilter* m_tableFilter = nullptr;
    mutable ValueType m_tupleIdx{};
public:
    /**
     * Default constructor
     */
    TableTupleFilterIterType() = default;

    /**
     * Constructor. Sets the iterator position pointing to the first tuple that
     * have the TableTupleFilter value set to the Marker
     */
    inline TableTupleFilterIterType(TableTupleFilter* m_tableFilter);

    ValueType& dereference() const {
        return m_tupleIdx;
    }

    // Forward Iteration Support
    void increment();

    bool equal(TableTupleFilterIterType<Marker, Const> const& other) const {
        // Shouldn't compare iterators from different tables
        vassert(m_tableFilter == other.m_tableFilter);
        return m_tupleIdx == other.m_tupleIdx;
    }

    /**
     * Constructor. Sets the iterator position pointing to the specified position - usually the end
     */
    TableTupleFilterIterType(const TableTupleFilter* m_tableFilter, size_t tupleIdx) :
        m_tableFilter(m_tableFilter), m_tupleIdx(tupleIdx) {}
};

template<int8_t Marker>
using TableTupleFilter_iter = TableTupleFilterIterType<Marker, false>;
template<int8_t Marker>
using TableTupleFilter_const_iter = TableTupleFilterIterType<Marker, true>;

class Table;
/**
 * A lightweight representation of a table - a contiguous array where
 * each tuple (active and non-active) is represented as a byte with a
 * certain value.  This lets clients of this class "tag" rows of a
 * table with an 8-bit value.  For full outer joins, this is used to
 * keep track of which tuples in the inner table were matched and
 * which were not, so as to provide null-padded rows.
 *
 * Each block in the table is assigned a block number, where the block
 * with the lowest address is block 0.
 *
 * The physical tuple address in the real table and the corresponding
 * tuple index in the TableTupleFilter are related by the following
 * equation:
 *
 * Tuple Index = (Tuple Address - Tuple Block Address) / Tuple Size + Block Offset
 *
 * where Block Offset is the index of the first tuple in the block into the array:
 *
 * Block Offset = Block Number * Tuples Per Block
 *
 * The net effect of this is that the byte array has the byte "tag"
 * for all the rows in the 0-th block first, followed by the 1-th
 * block, and so on.  Not all blocks will be full, so there will be
 * unused entries, but this representation is compact and provides
 * relatively fast lookups.
 */
class TableTupleFilter {
public:
    const static uint64_t INVALID_INDEX = std::numeric_limits<uint64_t>::max();
private:
    // Tuples (active and not active)
    std::vector<char> m_tuples{};

    // Collection of table blocks addresses
    std::vector<uint64_t> m_blocks{};

    // (Block Address/ Block offset into the tuples array) map
    std::unordered_map<uint64_t, uint64_t> m_blockIndexes{};

    // Block/Tuple size
    uint32_t m_tuplesPerBlock{};

    // Length of tuples in this table
    uint32_t m_tupleLength = 0;

    // Previously accessed block address, cached to avoid
    // excessive searches of m_blocks
    uint64_t m_prevBlockAddress = INVALID_INDEX;

    // Previously accessed block index, cached to avoid excessive
    // lookups in m_blockIndexes
    uint64_t m_prevBlockIndex = INVALID_INDEX;

    // Index of the last ACTIVE tuple in the underlying table
    uint64_t m_lastActiveTupleIndex = INVALID_INDEX;

    void init(const std::vector<uint64_t>& blocks, uint32_t tuplesPerBlock, uint32_t tupleLength);

    uint64_t getTupleIndex(const TableTuple& tuple) {
        uint64_t tupleAddress = reinterpret_cast<uint64_t>(tuple.address());
        uint64_t blockIndex = findBlockIndex(tupleAddress);
        return (tupleAddress - m_prevBlockAddress) / m_tupleLength + blockIndex;
    }

    /**
     * Initialize an active tuple by setting its value to ACTIVE_TUPLE and advance last active tuple index.
     * This method should be called only once during the initialization.
     * To update tuple value use updateTuple method
     */
    void initActiveTuple(const TableTuple& tuple) {
        uint64_t tupleIdx = getTupleIndex(tuple);
        vassert(m_tuples[tupleIdx] == INACTIVE_TUPLE);
        m_tuples[tupleIdx] = ACTIVE_TUPLE;
        // Advance last active tuple index if necessary
        if (empty() || m_lastActiveTupleIndex < tupleIdx) {
            m_lastActiveTupleIndex = tupleIdx;
        }
    }

    uint64_t findBlockIndex(uint64_t tupleAddress);

public:
    constexpr static int8_t INACTIVE_TUPLE  = -1;
    constexpr static int8_t ACTIVE_TUPLE    =  0;

    /**
     * Default constructor
     */
    TableTupleFilter() = default;

    /**
     * Initialize TableTupleFilter from a table by setting the value for all active tuples
     * to ACTIVE_TUPLE(0) and advancing the last active tuple index
     */
    void init(Table* table);

    /**
     * Update an active tuple and return the tuple index
     */
    uint64_t updateTuple(const TableTuple& tuple, char marker) {
        uint64_t tupleIdx = getTupleIndex(tuple);
        vassert(tupleIdx <= m_lastActiveTupleIndex && ! empty());
        vassert(m_tuples[tupleIdx] != INACTIVE_TUPLE);
        m_tuples[tupleIdx] = marker;
        return tupleIdx;
    }

    size_t getLastActiveTupleIndex() const {
        return m_lastActiveTupleIndex;
    }

    /**
     * Returns the tuple value
     */
    char getTupleValue(size_t tupleIdx) const {
        vassert(tupleIdx < m_tuples.size());
        return m_tuples[tupleIdx];
    }

    /**
     * Returns the tuple address
     */
    uint64_t getTupleAddress(size_t tupleIdx) {
        vassert(tupleIdx < m_tuples.size());
        size_t blockIdx = tupleIdx / m_tuplesPerBlock;
        vassert(blockIdx < m_blocks.size());
        if (m_prevBlockIndex > tupleIdx || (tupleIdx - m_prevBlockIndex) >= m_tuplesPerBlock) {
            vassert(m_blockIndexes.find(m_blocks[blockIdx]) != m_blockIndexes.end());
            m_prevBlockIndex = m_blockIndexes.find(m_blocks[blockIdx])->second;
        }
        return m_blocks[blockIdx] + (tupleIdx - m_prevBlockIndex) * m_tupleLength;
    }

    bool empty() const {
        return m_lastActiveTupleIndex == INVALID_INDEX;
    }

    // Non-const Iterators
    template<int8_t MARKER>
    TableTupleFilter_iter<MARKER> begin() {
        return {this};
    }

    template<int8_t MARKER>
    TableTupleFilter_iter<MARKER> end() {
        return {this,
            getLastActiveTupleIndex() != TableTupleFilter::INVALID_INDEX ?
                getLastActiveTupleIndex() + 1 : TableTupleFilter::INVALID_INDEX};
    }

    // Const Iterators
    template<int8_t MARKER>
    TableTupleFilter_const_iter<MARKER> begin() const {
        return {this};
    }

    template<int8_t MARKER>
    TableTupleFilter_const_iter<MARKER> end() const {
        return {this,
            getLastActiveTupleIndex() != TableTupleFilter::INVALID_INDEX ?
                getLastActiveTupleIndex() + 1 : TableTupleFilter::INVALID_INDEX};
    }
};

template<int8_t Marker, bool Const>
TableTupleFilterIterType<Marker, Const>::TableTupleFilterIterType(
        TableTupleFilter* m_tableFilter) :
    m_tableFilter(m_tableFilter), m_tupleIdx(TableTupleFilter::INVALID_INDEX) {
    if (!m_tableFilter->empty()) {
        increment();
    }
}

template<int8_t Marker, bool Const>
void TableTupleFilterIterType<Marker, Const>::increment() {
    auto const lastActiveTupleIndex = m_tableFilter->getLastActiveTupleIndex();
    do {
        ++m_tupleIdx;
    } while(m_tupleIdx <= lastActiveTupleIndex &&
            m_tableFilter->getTupleValue(m_tupleIdx) != Marker);
}

}

