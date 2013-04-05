/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "storage/CopyOnWriteIterator.h"
#include "common/tabletuple.h"
#include "storage/persistenttable.h"

namespace voltdb {
CopyOnWriteIterator::CopyOnWriteIterator(
        PersistentTable *table,
        TBMapI start,
        TBMapI end) :
        m_table(table), m_blockIterator(start), m_end(end),
        m_tupleLength(table->m_tupleLength),
        m_location(NULL),
        m_blockOffset(0),
        m_currentBlock(NULL) {
    //Prime the pump
    m_table->snapshotFinishedScanningBlock(m_currentBlock, m_blockIterator.data());
    m_location = m_blockIterator.key();
    m_currentBlock = m_blockIterator.data();
    m_blockIterator.data() = TBPtr();
    m_blockOffset = 0;
    m_blockIterator++;
}

/**
 * Iterate through the table blocks until all the active tuples have been found. Skip dirty tuples
 * and mark them as clean so that they can be copied during the next snapshot.
 */
bool CopyOnWriteIterator::next(TableTuple &out) {
    assert(m_currentBlock != NULL);
    while (true) {
        if (m_blockOffset >= m_currentBlock->unusedTupleBoundry()) {
            if (m_blockIterator == m_end) {
                m_table->snapshotFinishedScanningBlock(m_currentBlock, TBPtr());
                break;
            }
            m_table->snapshotFinishedScanningBlock(m_currentBlock, m_blockIterator.data());
            m_location = m_blockIterator.key();
            m_currentBlock = m_blockIterator.data();
            assert(m_currentBlock->address() == m_location);
            m_blockIterator.data() = TBPtr();
            m_blockOffset = 0;
            m_blockIterator++;
        }
        assert(m_location < m_currentBlock.get()->address() + m_table->m_tableAllocationSize);
        assert(m_location < m_currentBlock.get()->address() + (m_table->m_tupleLength * m_table->m_tuplesPerBlock));
        assert (out.sizeInValues() == m_table->columnCount());
        m_blockOffset++;
        out.move(m_location);
        const bool active = out.isActive();
        const bool dirty = out.isDirty();
        // Return this tuple only when this tuple is not marked as deleted and isn't dirty
        if (active && !dirty) {
            out.setDirtyFalse();
            m_location += m_tupleLength;
            return true;
        } else {
            out.setDirtyFalse();
            m_location += m_tupleLength;
        }
    }
    return false;
}

int64_t CopyOnWriteIterator::countRemaining() const {
    TableTuple out(m_table->schema());
    uint32_t blockOffset = m_blockOffset;
    char *location = m_location;
    TupleBlock *pcurrentBlock = m_currentBlock.get();
    TBPtr currentBlock(pcurrentBlock);
    TBMapI blockIterator = m_blockIterator;
    int64_t count = 0;
    while (true) {
        if (blockOffset >= currentBlock->unusedTupleBoundry()) {
            if (blockIterator == m_end) {
                break;
            }
            location = blockIterator.key();
            currentBlock = blockIterator.data();
            assert(currentBlock->address() == location);
            blockOffset = 0;
            blockIterator++;
        }
        blockOffset++;
        out.move(location);
        location += m_tupleLength;
        if (out.isActive() && !out.isDirty()) {
            count++;
        }
    }
    return count;
}
}
