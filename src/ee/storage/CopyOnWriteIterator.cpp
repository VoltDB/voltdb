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
CopyOnWriteIterator::CopyOnWriteIterator(
        Table *table,
        TBMapI start,
        TBMapI end) :
        m_table(table), m_blockIterator(start), m_end(end),
        m_tupleLength(table->m_tupleLength),
        m_location(NULL),
        m_blockOffset(0), m_currentBlock(NULL) {
}

/**
 * Iterate through the table blocks until all the active tuples have been found. Skip dirty tuples
 * and mark them as clean so that they can be copied during the next snapshot.
 */
bool CopyOnWriteIterator::next(TableTuple &out) {
    while (true) {
        if (m_currentBlock == NULL ||
                m_blockOffset == m_currentBlock->unusedTupleBoundry()) {
            if (m_blockIterator == m_end) {
                break;
            }
            m_location = m_blockIterator.key();
            m_currentBlock = m_blockIterator.data();
            m_blockOffset = 0;
            m_blockIterator++;
        } else {
            m_location += m_tupleLength;
        }
        assert(m_location < m_currentBlock.get()->address() + m_table->m_tableAllocationSize);
        assert (out.sizeInValues() == m_table->columnCount());
        m_blockOffset++;
        out.move(m_location);
        const bool active = out.isActive();
        const bool dirty = out.isDirty();
        // Return this tuple only when this tuple is not marked as deleted and isn't dirty
        if (active && !dirty) {
            out.setDirtyFalse();
            return true;
        } else {
            out.setDirtyFalse();
        }
    }
    return false;
}
}
