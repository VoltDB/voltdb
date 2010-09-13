/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

/**
 * Iterator for table which neglects deleted tuples.
 * TableIterator is a small and copiable object.
 * You can copy it, not passing a pointer of it.
 */
class TableIterator : public TupleIterator {
public:
    TableIterator(const Table *parent, bool scanAllBlocks = false);

    /**
     * Updates the given tuple so that it points to the next tuple in the table.
     * @param out the tuple will point to the retrieved tuple if this method returns true.
     * @return true if succeeded. false if no more active tuple is there.
    */
    bool next(TableTuple &out);
    bool hasNext();
    int getLocation() const;

private:

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
    bool m_scanAllBlocksOrCountFoundTuples;
    const Table *m_table;
    char *m_dataPtr;
    uint32_t m_location;
    uint32_t m_activeTuples;
    uint32_t m_foundTuples;
    uint32_t m_tupleLength;
    uint32_t m_tuplesPerBlock;
    uint32_t m_blockIndex;
};

inline TableIterator::TableIterator(const Table *parent, bool scanAllBlocks)
    : m_scanAllBlocksOrCountFoundTuples(scanAllBlocks),
      m_table(parent), m_dataPtr(NULL), m_location(0),
    m_activeTuples((int) m_table->m_tupleCount),
    m_foundTuples(0), m_tupleLength(parent->m_tupleLength),
    m_tuplesPerBlock(parent->m_tuplesPerBlock), m_blockIndex(0)
    {}

inline bool TableIterator::continuationPredicate() {
    if (m_scanAllBlocksOrCountFoundTuples) {
        /*
         * Scan until there are no more blocks to scan
         */
        return m_location < m_table->m_usedTuples;
    } else {
        /*
         * Scan until all active tuples have been found
         */
        return m_foundTuples < m_activeTuples;
    }
}

inline bool TableIterator::hasNext() {
    return continuationPredicate();
}

inline bool TableIterator::next(TableTuple &out) {
    while (continuationPredicate()) {
        if (m_location % m_tuplesPerBlock == 0) {
#ifdef MEMCHECK_NOFREELIST
            /*
             * A deleted tuple will have a null block entry and can be skipped.
             */
            m_dataPtr = m_table->dataPtrForTuple(m_location);
            if (m_dataPtr == NULL) {
                m_location++;
                continue;
            }
#else
            m_dataPtr = m_table->dataPtrForTuple(m_location);
#endif
        } else {
            m_dataPtr += m_tupleLength;
        }
        assert (out.sizeInValues() == m_table->columnCount());
        out.move(m_dataPtr);
        assert(m_dataPtr < m_dataPtr + m_table->m_tableAllocationTargetSize);
        //assert(m_foundTuples == m_location);
        ++m_location;

        //assert(out.isActive());

        // Return this tuple only when this tuple is not marked as deleted.
        if (out.isActive()) {
            ++m_foundTuples;
            //assert(m_foundTuples == m_location);
            return true;
        }
    }
    return false;
}

inline int TableIterator::getLocation() const {
    return m_location;
}

}

#endif
