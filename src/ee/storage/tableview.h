/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#ifndef VOLTDB_TABLEVIEW_H
#define VOLTDB_TABLEVIEW_H

#include "common/tabletuple.h"

#include <boost/unordered_map.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include <vector>
#include <limits>

namespace voltdb {

class Table;

template<typename Value>
class TableView_iter;
typedef TableView_iter<uint64_t> TableView_iterator;
typedef TableView_iter<uint64_t const> TableView_const_iterator;

class TableView {

    public:

    static const char INACTIVE_TUPLE;

    friend class TableView_iter<uint64_t>;
    friend class TableView_iter<uint64_t const>;

    /**
     * Default constructor
     */
    TableView();

    /**
     * Initialize TableView with the Table data and set the value for each active tuple
     * to MARKER
     */
    void init(Table* table , char initVal);

    /**
     * Update active tuple. Returns the tuple index
     */
    uint64_t setTupleBit(const TableTuple& tuple, char marker)
    {
        uint64_t tupleIdx = getTupleIndex(tuple);
        assert(tupleIdx <= m_lastActiveTupleIndex && m_lastActiveTupleIndex != INVALID_INDEX);
        assert(m_tuples[tupleIdx] != INACTIVE_TUPLE);
        m_tuples[tupleIdx] = marker;
        return tupleIdx;
    }

    /**
     * Returns the tuple value
     */
    char getTupleBit(size_t tupleIdx) const
    {
        assert(tupleIdx < m_tuples.size());
        return m_tuples[tupleIdx];
    }

    /**
     * Returns the tuple address
     */
    uint64_t getTupleAddress(size_t tupleIdx) const
    {
        assert(tupleIdx < m_tuples.size());
        size_t blockIdx = tupleIdx / m_tuplesPerBlock;
        assert(blockIdx < m_blocks.size());
        return m_blocks[blockIdx] + (tupleIdx - blockIdx) * m_tupleLength;
    }

    bool empty() const
    {
        return m_lastActiveTupleIndex == INVALID_INDEX;
    }

    // Iterators
    TableView_iterator begin(char marker);

    TableView_iterator end(char marker);

    TableView_const_iterator begin(char marker) const;

    TableView_const_iterator end(char marker) const;

    private:

    static const uint64_t INVALID_INDEX;

    void init(const std::vector<uint64_t>& blocks, uint32_t tuplesPerBlock, uint32_t tupleLength);

    uint64_t getTupleIndex(const TableTuple& tuple)
    {
        uint64_t tupleAddress = (uint64_t) tuple.address();
        uint64_t blockIndex = findBlockIndex(tupleAddress);
        return (tupleAddress - m_prevBlockAddress) / m_tupleLength + blockIndex;
    }

    /**
     * Initialize tuple and advance last active tuple index
     * This method should be called only once during the initialization.
     * To update tuple value use setTupleBit method
     */
    void initTupleBit(const TableTuple& tuple, char marker)
    {
        assert(marker != INACTIVE_TUPLE);
        uint64_t tupleIdx = getTupleIndex(tuple);
        assert(m_tuples[tupleIdx] == INACTIVE_TUPLE);
        m_tuples[tupleIdx] = marker;
        // Advance last active tuple index if necessary
        if (m_lastActiveTupleIndex == INVALID_INDEX || m_lastActiveTupleIndex < tupleIdx)
        {
            m_lastActiveTupleIndex = tupleIdx;
        }
    }

    size_t getLastActiveTupleIndex() const
    {
        return m_lastActiveTupleIndex;
    }

    uint64_t findBlockIndex(uint64_t tupleAddress);

    // Tuples (active and not active)
    std::vector<char> m_tuples;
    // Collection of table blocks addresses
    std::vector<uint64_t> m_blocks;
    // (Block Address/ Block offset into the tuples array) map
    boost::unordered_map<uint64_t, uint64_t> m_blockIndexes;

    // Block/Tuple size
    uint32_t m_tuplesPerBlock;
    uint32_t m_tupleLength;

    // Previously accessed block address
    uint64_t m_prevBlockAddress;
    // Previously accessed block index
    uint64_t m_prevBlockIndex;

    // Index of the last ACTIVE tuple in the underlying table
    uint64_t m_lastActiveTupleIndex;
};

/**
 * TableView Iterator. Implements FORWARD ITERATOR Concept.
 * Iterates over the tuples that have a certain value set in
 * underline TableView.
 *
 * Const and Non-Const versions
 */
template<typename Value>
class TableView_iter
  : public boost::iterator_facade<
        TableView_iter<Value>
      , Value
      , boost::forward_traversal_tag>
{
    public:
    /**
     * Default constructor
     */
    TableView_iter(char marker = 0)
      : m_tableView(0), m_tupleIdx(), m_marker(marker), m_tupleIdxPtr(&m_tupleIdx)
    {}

private:

    friend class TableView;
    friend class boost::iterator_core_access;

    /**
     * Constructor. Sets the iterator position pointing to the first tuple that
     * have the TableView value set to the MARKER
     */
    explicit TableView_iter(TableView* m_tableView, char marker = 0)
      : m_tableView(m_tableView), m_tupleIdx(TableView::INVALID_INDEX), m_marker(marker), m_tupleIdxPtr(&m_tupleIdx)
    {
        if (!m_tableView->empty())
        {
            moveToNextTuple();
        }
    }

    /**
     * Constructor. Sets the iterator position pointing to the specified position - usually the end
     */
    explicit TableView_iter(const TableView* m_tableView, size_t tupleIdx, char marker = 0)
        :  m_tableView(m_tableView), m_tupleIdx(tupleIdx), m_marker(marker), m_tupleIdxPtr(&m_tupleIdx)
    {}

    // Forward Iteration Support
    void increment()
    {
        moveToNextTuple();
    }

    bool equal(TableView_iter const& other) const
    {
        return m_tableView == other.m_tableView && m_tupleIdx == other.m_tupleIdx && m_marker == other.m_marker;
    }

    Value& dereference() const
    {
        return *m_tupleIdxPtr;
    }

    void moveToNextTuple()
    {
        uint64_t lastActiveTupleIndex = m_tableView->getLastActiveTupleIndex();
        do
        {
            ++m_tupleIdx;
        } while(m_tupleIdx <= lastActiveTupleIndex && m_tableView->getTupleBit(m_tupleIdx) != m_marker);
    }

    const TableView* m_tableView;
    uint64_t m_tupleIdx;
    char m_marker;
    Value* m_tupleIdxPtr;
};

inline
TableView_iterator TableView::begin(char marker)
{
    return TableView_iterator(this, marker);
}

inline
TableView_iterator TableView::end(char marker)
{
    uint64_t lastActiveTupleIndex = (getLastActiveTupleIndex() != TableView::INVALID_INDEX) ?
        getLastActiveTupleIndex() + 1 : TableView::INVALID_INDEX;
    return TableView_iterator(this, lastActiveTupleIndex, marker);
}

inline
TableView_const_iterator TableView::begin(char marker) const
{
    return TableView_const_iterator(this, marker);
}

inline
TableView_const_iterator TableView::end(char marker) const
{
    uint64_t lastActiveTupleIndex = (getLastActiveTupleIndex() != TableView::INVALID_INDEX) ?
        getLastActiveTupleIndex() + 1 : TableView::INVALID_INDEX;
    return TableView_const_iterator(this, lastActiveTupleIndex, marker);
}

}
#endif // VOLTDB_TABLEVIEW_H
