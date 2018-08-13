/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "tableiterator.h"
using namespace voltdb;

// Construct iterator for persistent tables
TableIterator::TableIterator(Table *parent, TBMapI start)
    : m_table(parent)
    , m_tupleLength(parent->m_tupleLength)
    , m_activeTuples((int) m_table->m_tupleCount)
    , m_iteratorType(PERSISTENT)
    , m_state(start)
{
}

// Construct iterator for temp tables
TableIterator::TableIterator(Table *parent, std::vector<TBPtr>::iterator start, bool deleteAsGo)
    : m_table(parent)
    , m_tupleLength(parent->m_tupleLength)
    , m_activeTuples((int) m_table->m_tupleCount)
    , m_iteratorType(TEMP)
    , m_state(start, deleteAsGo)
{
}

//  Construct an iterator for large temp tables
TableIterator::TableIterator(Table *parent, std::vector<LargeTempTableBlockId>::iterator start, bool deleteAsGo)
    : m_table(parent)
    , m_tupleLength(parent->m_tupleLength)
    , m_activeTuples((int) m_table->m_tupleCount)
    , m_iteratorType(LARGE_TEMP)
    , m_state(start, deleteAsGo)
{
}

// Construct an iterator from another iterator
TableIterator::TableIterator(const TableIterator &that)
    : m_table(that.m_table)
    , m_tupleLength(that.m_tupleLength)
    , m_activeTuples(that.m_activeTuples)
    , m_foundTuples(that.m_foundTuples)
    , m_dataPtr(that.m_dataPtr)
    , m_dataEndPtr(that.m_dataEndPtr)
    , m_iteratorType(that.m_iteratorType)
    , m_state(that.m_state)
{
    // This assertion could fail if we are copying an invalid iterator
    // (table changed after iterator was created)
    assert (that.m_table->m_tupleCount == that.m_activeTuples);
}

TableIterator& TableIterator::operator=(const TableIterator& that) {
    // This assertion could fail if we are copying an invalid iterator
    // (table changed after iterator was created)
    assert (that.m_table->m_tupleCount == that.m_activeTuples);

    if (*this != that) {
        if (m_iteratorType == LARGE_TEMP) {
            finishLargeTempTableScan();
        }

        m_table = that.m_table;
        m_tupleLength = that.m_tupleLength;
        m_activeTuples = that.m_activeTuples;
        m_foundTuples = that.m_foundTuples;
        m_dataPtr = that.m_dataPtr;
        m_dataEndPtr = that.m_dataEndPtr;
        m_iteratorType = that.m_iteratorType;
        m_state = that.m_state;
    }

    return *this;
}

void TableIterator::reset(TBMapI start) {
    assert(m_iteratorType == PERSISTENT);

    m_tupleLength = m_table->m_tupleLength;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_dataPtr = NULL;
    m_dataEndPtr = NULL;
    m_state.m_persBlockIterator = start;
}

void TableIterator::reset(std::vector<TBPtr>::iterator start) {
    assert(m_iteratorType == TEMP);

    m_tupleLength = m_table->m_tupleLength;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_dataPtr = NULL;
    m_dataEndPtr = NULL;
    m_state.m_tempBlockIterator = start;
    m_state.m_tempTableDeleteAsGo = false;
}

 void TableIterator::reset(std::vector<LargeTempTableBlockId>::iterator start) {
    assert(m_iteratorType == LARGE_TEMP);

    // Unpin the block of the previous scan before resetting.
    finishLargeTempTableScan();

    m_tupleLength = m_table->m_tupleLength;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_dataPtr = NULL;
    m_dataEndPtr = NULL;
    m_state.m_largeTempBlockIterator = start;
    m_state.m_tempTableDeleteAsGo = false;
}

bool TableIterator::hasNext() const {
    return m_foundTuples < m_activeTuples;
}

// This function should be replaced by specific iteration functions
// when the caller knows the table type.
bool TableIterator::next(TableTuple &out) {
    switch (m_iteratorType) {
    case TEMP:
        return tempNext(out);
    case PERSISTENT:
        return persistentNext(out);
    case LARGE_TEMP:
    default:
        assert(m_iteratorType == LARGE_TEMP);
        return largeTempNext(out);
    }
}

bool TableIterator::persistentNext(TableTuple &out) {
    while (m_foundTuples < m_activeTuples) {

        if (m_dataPtr != NULL) {
            m_dataPtr += m_tupleLength;
        }

        if (m_dataPtr == NULL || m_dataPtr >= m_dataEndPtr) {
            // We are either before first tuple (m_dataPtr is null)
            // or at the end of a block.
            m_dataPtr = m_state.m_persBlockIterator.key();

            uint32_t unusedTupleBoundary = m_state.m_persBlockIterator.data()->unusedTupleBoundary();
            m_dataEndPtr = m_dataPtr + (unusedTupleBoundary * m_tupleLength);

            m_state.m_persBlockIterator++;
        }

        assert (out.columnCount() == m_table->columnCount());
        out.move(m_dataPtr);

        const bool active = out.isActive();
        const bool pendingDelete = out.isPendingDelete();
        const bool isPendingDeleteOnUndoRelease = out.isPendingDeleteOnUndoRelease();

        // Return this tuple only when this tuple is not marked as deleted.
        if (active) {
            ++m_foundTuples;
            if (!(pendingDelete || isPendingDeleteOnUndoRelease)) {
                return true;
            }
        }
    } // end while found tuples is less than active tuples

    return false;
}

bool TableIterator::tempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {

        if (m_dataPtr != NULL) {
            m_dataPtr += m_tupleLength;
        }

        if (m_dataPtr == NULL || m_dataPtr >= m_dataEndPtr) {

            // delete the last block of tuples in this temp table when they will never be used
            if (m_state.m_tempTableDeleteAsGo) {
                m_table->freeLastScannedBlock(m_state.m_tempBlockIterator);
            }

            m_dataPtr = (*m_state.m_tempBlockIterator)->address();

            uint32_t unusedTupleBoundary = (*m_state.m_tempBlockIterator)->unusedTupleBoundary();
            m_dataEndPtr = m_dataPtr + (unusedTupleBoundary * m_tupleLength);

            ++m_state.m_tempBlockIterator;
        }

        assert (out.columnCount() == m_table->columnCount());
        out.move(m_dataPtr);

        ++m_foundTuples;
        return true;
    }

    return false;
}

bool TableIterator::largeTempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {

        if (m_dataPtr != NULL) {
            m_dataPtr += m_tupleLength;
        }

        if (m_dataPtr == NULL || m_dataPtr >= m_dataEndPtr) {
            LargeTempTableBlockCache* lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();
            auto& blockIdIterator = m_state.m_largeTempBlockIterator;

            if (m_dataPtr != NULL) {
                lttCache->unpinBlock(*blockIdIterator);

                if (m_state.m_tempTableDeleteAsGo) {
                    blockIdIterator = m_table->releaseBlock(blockIdIterator);
                }
                else {
                    ++blockIdIterator;
                }
            }

            LargeTempTableBlock* block = lttCache->fetchBlock(*blockIdIterator);
            m_dataPtr = block->tupleStorage();

            uint32_t unusedTupleBoundary = block->unusedTupleBoundary();
            m_dataEndPtr = m_dataPtr + (unusedTupleBoundary * m_tupleLength);
        }

        out.move(m_dataPtr);

        ++m_foundTuples;

        return true;
    } // end if there are still more tuples

    // Unpin (and release, if delete-as-you-go) the last block
    finishLargeTempTableScan();
    return false;
}

void TableIterator::finishLargeTempTableScan() {
    if (m_foundTuples == 0) {
        return;
    }

    LargeTempTableBlockCache* lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    auto& blockIdIterator = m_state.m_largeTempBlockIterator;

    if (lttCache->blockIsPinned(*blockIdIterator)) {
        lttCache->unpinBlock(*blockIdIterator);
    }

    if (m_foundTuples == m_activeTuples
        && m_state.m_tempTableDeleteAsGo) {

        blockIdIterator = m_table->releaseBlock(blockIdIterator);
        m_activeTuples = 0;
        m_foundTuples = 0;
        m_dataPtr = NULL;
        m_dataEndPtr = NULL;
    }
}

TableIterator::~TableIterator() {
    if (m_iteratorType == LARGE_TEMP) {
        finishLargeTempTableScan();
    }
}

