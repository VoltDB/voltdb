/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"

#include <algorithm>
#include <limits>

namespace voltdb {

const uint64_t TableTupleFilter::INVALID_INDEX = std::numeric_limits<uint64_t>::max();


TableTupleFilter::TableTupleFilter() :
    m_tuples(),
    m_blocks(),
    m_blockIndexes(),
    m_tuplesPerBlock(),
    m_tupleLength(),
    m_prevBlockAddress(std::numeric_limits<uint64_t>::max()),
    m_prevBlockIndex(std::numeric_limits<uint64_t>::max()),
    m_lastActiveTupleIndex(std::numeric_limits<uint64_t>::max())
    {}

void TableTupleFilter::init(const std::vector<uint64_t>& blocks, uint32_t tuplesPerBlock, uint32_t tupleLength)
{
    m_tuples.insert(m_tuples.end(), blocks.size() * tuplesPerBlock, TableTupleFilter::INACTIVE_TUPLE);
    m_blocks.insert(m_blocks.end(), blocks.begin(), blocks.end());
    m_tuplesPerBlock = tuplesPerBlock;
    m_tupleLength = tupleLength;
    std::sort(m_blocks.begin(), m_blocks.end());
    m_blockIndexes.rehash(m_blocks.size());
    for(size_t i = 0; i < m_blocks.size(); ++i) {
        m_blockIndexes.insert(std::make_pair(m_blocks[i], i * m_tuplesPerBlock));
    }
}

void TableTupleFilter::init(Table* table)
{
    assert(table != NULL);

    init(table->getBlockAddresses(), table->getTuplesPerBlock(), table->getTupleLength());

    TableTuple tuple(table->schema());
    TableIterator iterator = table->iterator();
    while (iterator.next(tuple)) {
        initActiveTuple(tuple);
    }
}

uint64_t TableTupleFilter::findBlockIndex(uint64_t tupleAddress)
{
    if (m_prevBlockAddress > tupleAddress || (tupleAddress - m_prevBlockAddress)/ m_tupleLength >= m_tuplesPerBlock) {
        // This tuple belongs to a different block that the last tuple did
        assert(!m_blocks.empty());
        std::vector<uint64_t>::iterator blockIter = std::lower_bound(m_blocks.begin(), m_blocks.end(), tupleAddress);
        if (blockIter == m_blocks.end() || tupleAddress != *blockIter)
        {
            // move back a block
            assert(blockIter != m_blocks.begin());
            --blockIter;
        }
        m_prevBlockAddress = *blockIter;
        assert(m_blockIndexes.find(m_prevBlockAddress) != m_blockIndexes.end());
        m_prevBlockIndex = m_blockIndexes.find(m_prevBlockAddress)->second;
    }
    return m_prevBlockIndex;
}


}

