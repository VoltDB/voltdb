/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
#include "storage/CopyOnWriteIterator.h"
#include "common/tabletuple.h"
#include "storage/table.h"

namespace voltdb {
CopyOnWriteIterator::CopyOnWriteIterator(Table *table) :
        m_table(table), m_blocks(table->m_data), m_blockIndex(0),
        m_tupleLength(table->m_tupleLength),
        m_location(m_blocks[0] - m_tupleLength), m_activeTupleCount(table->m_tupleCount),
        m_foundTuples(0),
        m_blockLength(m_tupleLength * table->m_tuplesPerBlock), m_didFirstIteration(false) {

}

/**
 * Iterate through the table blocks until all the active tuples have been found. Skip dirty tuples
 * and mark them as clean so that they can be copied during the next snapshot.
 */
bool CopyOnWriteIterator::next(TableTuple &out) {
    while (m_foundTuples < m_activeTupleCount) {
        m_location = m_location + m_tupleLength;
        const long int delta = m_location - m_blocks[m_blockIndex];
        if (m_didFirstIteration && delta >= m_blockLength) {
            m_location = m_blocks[++m_blockIndex];
        } else {
            m_didFirstIteration = true;
        }
        assert(m_location < m_blocks[m_blockIndex] + m_blockLength);
        assert (out.sizeInValues() == m_table->columnCount());
        out.move(m_location);
        const bool active = out.isActive();
        const bool dirty = out.isDirty();
        // Return this tuple only when this tuple is not marked as deleted and isn't dirty
        if (active && !dirty) {
            ++m_foundTuples;
            out.setDirtyFalse();
            return true;
        } else {
            out.setDirtyFalse();
        }
    }
    cleanBlocksAfterLastFound();
    return false;
}

void CopyOnWriteIterator::cleanBlocksAfterLastFound() {
    m_location = m_location + m_tupleLength;
    while (true) {
        long int delta = m_location - m_blocks[m_blockIndex];
        TableTuple tuple(m_table->schema());
        while (delta < m_blockLength) {
            tuple.move(m_location);
            tuple.setDirtyFalse();
            m_location = m_location + m_tupleLength;
            delta = m_location - m_blocks[m_blockIndex];
        }
        m_blockIndex++;
        if (m_blockIndex < m_blocks.size()) {
            m_location = m_blocks[m_blockIndex];
        } else {
            break;
        }
    }
}
}
