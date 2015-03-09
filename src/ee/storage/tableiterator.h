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

#ifndef HSTORETABLEITERATOR_H
#define HSTORETABLEITERATOR_H

#include <cassert>
#include "boost/shared_ptr.hpp"
#include "common/tabletuple.h"
#include "table.h"
#include "storage/TupleIterator.h"

namespace voltdb {

class TempTable;
class PersistentTable;

/**
 * Iterator for table which neglects deleted tuples.
 * TableIterator is a small and copiable object.
 * You can copy it, not passing a pointer of it.
 *
 * This class should be a virtual interface or should
 * be templated on the underlying table data iterator.
 * Either change requires some updating of the iterators
 * that are persvasively stack allocated...
 *
 */
class TableIterator : public TupleIterator {

    friend class TempTable;
    friend class PersistentTable;

public:

    /**
     * Updates the given tuple so that it points to the next tuple in the table.
     * @param out the tuple will point to the retrieved tuple if this method returns true.
     * @return true if succeeded. false if no more active tuple is there.
    */
    bool next(TableTuple &out);
    bool hasNext();
    int getLocation() const;

    void setTempTableDeleteAsGo(bool flag) {
        m_tempTableDeleteAsGo = flag;
    }

private:
    // Get an iterator via table->iterator()
    TableIterator(Table *, TBMapI);
    TableIterator(Table *, std::vector<TBPtr>::iterator);

    // avoid if possible -- less safe
    TableIterator(Table *);

    bool persistentNext(TableTuple &out);
    bool tempNext(TableTuple &out);

    void reset(TBMapI);
    void reset(std::vector<TBPtr>::iterator);
    bool continuationPredicate();

    /*
     * Configuration parameter that controls whether the table iterator
     * stops when it has found the expected number of tuples or when it has iterated
     * all the blocks. The former is able to stop sooner without having to read to the end of
     * of the block. The latter is useful when the table will be modified after the creation of
     * the iterator. It is assumed that the code invoking this iterator is handling
     * the modifications that occur after the iterator is created.
     *
     * When set to false the counting of found tuples method is used. When set to true
     * all blocks are scanned.
     */
    Table *m_table;
    TBMapI m_blockIterator;
    char *m_dataPtr;
    uint32_t m_location;
    uint32_t m_blockOffset;
    uint32_t m_activeTuples;
    uint32_t m_foundTuples;
    uint32_t m_tupleLength;
    uint32_t m_tuplesPerBlock;
    TBPtr m_currentBlock;
    std::vector<TBPtr>::iterator m_tempBlockIterator;
    bool m_tempTableIterator;
    bool m_tempTableDeleteAsGo;
};

inline TableIterator::TableIterator(Table *parent, std::vector<TBPtr>::iterator start)
    : m_table(parent),
      m_dataPtr(NULL),
      m_location(0),
      m_blockOffset(0),
      m_activeTuples((int) m_table->m_tupleCount),
      m_foundTuples(0), m_tupleLength(parent->m_tupleLength),
      m_tuplesPerBlock(parent->m_tuplesPerBlock), m_currentBlock(NULL),
      m_tempBlockIterator(start),
      m_tempTableIterator(true),
      m_tempTableDeleteAsGo(false)
    {
    }


inline TableIterator::TableIterator(Table *parent, TBMapI start)
    : m_table(parent),
      m_blockIterator(start),
      m_dataPtr(NULL),
      m_location(0),
      m_blockOffset(0),
      m_activeTuples((int) m_table->m_tupleCount),
      m_foundTuples(0),
      m_tupleLength(parent->m_tupleLength),
      m_tuplesPerBlock(parent->m_tuplesPerBlock),
      m_currentBlock(NULL),
      m_tempTableIterator(false),
      m_tempTableDeleteAsGo(false)
    {
    }

inline TableIterator::TableIterator(Table *parent)
    : m_table(parent),
      m_dataPtr(NULL),
      m_location(0),
      m_blockOffset(0),
      m_activeTuples(0),
      m_foundTuples(0),
      m_tupleLength(0),
      m_tuplesPerBlock(1),
      m_currentBlock(NULL),
      m_tempTableIterator(true),
      m_tempTableDeleteAsGo(false)
    {
    }


inline void TableIterator::reset(std::vector<TBPtr>::iterator start) {
    m_tempBlockIterator = start;
    m_dataPtr= NULL;
    m_location = 0;
    m_blockOffset = 0;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_tupleLength = m_table->m_tupleLength;
    m_tuplesPerBlock = m_table->m_tuplesPerBlock;
    m_currentBlock = NULL;
    m_tempTableIterator = true;
    m_tempTableDeleteAsGo = false;
}

inline void TableIterator::reset(TBMapI start) {
    m_blockIterator = start;
    m_dataPtr= NULL;
    m_location = 0;
    m_blockOffset = 0;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_tupleLength = m_table->m_tupleLength;
    m_tuplesPerBlock = m_table->m_tuplesPerBlock;
    m_currentBlock = NULL;
    m_tempTableIterator = false;
    m_tempTableDeleteAsGo = false;
}

inline bool TableIterator::hasNext() {
    return m_foundTuples < m_activeTuples;
}

// This function should be replaced by specific iteration functions
// when the caller knows the table type.
inline bool TableIterator::next(TableTuple &out) {
    if (m_tempTableIterator) {
        return tempNext(out);
    }
    return persistentNext(out);
}

inline bool TableIterator::persistentNext(TableTuple &out) {
    while (m_foundTuples < m_activeTuples) {
        if (m_currentBlock == NULL ||
            m_blockOffset >= m_currentBlock->unusedTupleBoundry()) {
//            assert(m_blockIterator != m_table->m_data.end());
//            if (m_blockIterator == m_table->m_data.end()) {
//                throwFatalException("Could not find the expected number of tuples during a table scan");
//            }
            m_dataPtr = m_blockIterator.key();
            m_currentBlock = m_blockIterator.data();
            m_blockOffset = 0;
            m_blockIterator++;
        } else {
            m_dataPtr += m_tupleLength;
        }
        assert (out.sizeInValues() == m_table->columnCount());
        out.move(m_dataPtr);
        assert(m_dataPtr < m_currentBlock.get()->address() + m_table->m_tableAllocationTargetSize);
        assert(m_dataPtr < m_currentBlock.get()->address() + (m_table->m_tupleLength * m_table->m_tuplesPerBlock));
        //assert(m_foundTuples == m_location);
        ++m_location;
        ++m_blockOffset;

        //assert(out.isActive());

        const bool active = out.isActive();
        const bool pendingDelete = out.isPendingDelete();
        const bool isPendingDeleteOnUndoRelease = out.isPendingDeleteOnUndoRelease();
        // Return this tuple only when this tuple is not marked as deleted.
        if (active) {
            ++m_foundTuples;
            if (!(pendingDelete || isPendingDeleteOnUndoRelease)) {
                //assert(m_foundTuples == m_location);
                return true;
            }
        }
    }
    return false;
}

inline bool TableIterator::tempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {
        if (m_currentBlock == NULL ||
            m_blockOffset >= m_currentBlock->unusedTupleBoundry())
        {
            // delete the last block of tuples in this temp table when they will never be used
            if (m_tempTableDeleteAsGo) {
                m_table->freeLastScanedBlock(m_tempBlockIterator);
            }

            m_currentBlock = *m_tempBlockIterator;
            m_dataPtr = m_currentBlock->address();
            m_blockOffset = 0;
            m_tempBlockIterator++;
        } else {
            m_dataPtr += m_tupleLength;
        }
        assert (out.sizeInValues() == m_table->columnCount());
        out.move(m_dataPtr);
        assert(m_dataPtr < m_currentBlock.get()->address() + m_table->m_tableAllocationTargetSize);
        assert(m_dataPtr < m_currentBlock.get()->address() + (m_table->m_tupleLength * m_table->m_tuplesPerBlock));


        //assert(m_foundTuples == m_location);

        ++m_location;
        ++m_blockOffset;

        //assert(out.isActive());
        ++m_foundTuples;
        //assert(m_foundTuples == m_location);
        return true;
    }

    return false;
}

inline int TableIterator::getLocation() const {
    return m_location;
}

}

#endif
