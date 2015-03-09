/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
        PersistentTableSurgeon *surgeon,
        TBMap blocks) :
        m_table(table), m_surgeon(surgeon), m_blocks(blocks),
        m_blockIterator(m_blocks.begin()), m_end(m_blocks.end()),
        m_tupleLength(table->getTupleLength()),
        m_location(NULL),
        m_blockOffset(0),
        m_currentBlock(NULL),
        m_skippedDirtyRows(0),
        m_skippedInactiveRows(0) {
    //Prime the pump
    if (m_blockIterator != m_end) {
        m_surgeon->snapshotFinishedScanningBlock(m_currentBlock, m_blockIterator.data());
        m_location = m_blockIterator.key();
        m_currentBlock = m_blockIterator.data();
        m_blockIterator++;
    }
    m_blockOffset = 0;
}

/**
 * Iterate through the table blocks until all the active tuples have been found. Skip dirty tuples
 * and mark them as clean so that they can be copied during the next snapshot.
 */
bool CopyOnWriteIterator::next(TableTuple &out) {
    if (m_currentBlock == NULL) {
        return false;
    }
    while (true) {
        if (m_blockOffset >= m_currentBlock->unusedTupleBoundry()) {
            if (m_blockIterator == m_end) {
                m_surgeon->snapshotFinishedScanningBlock(m_currentBlock, TBPtr());
                break;
            }
            m_surgeon->snapshotFinishedScanningBlock(m_currentBlock, m_blockIterator.data());

            char *finishedBlock = m_currentBlock->address();

            m_location = m_blockIterator.key();
            m_currentBlock = m_blockIterator.data();
            assert(m_currentBlock->address() == m_location);
            m_blockOffset = 0;

            // Remove the finished block from the map so that it can be released
            // back to the OS if all tuples in the block is deleted.
            //
            // This invalidates the iterators, so we have to get new iterators
            // using the current block's start address. m_blockIterator has to
            // point to the next block, hence the upper_bound() call.
            m_blocks.erase(finishedBlock);
            m_blockIterator = m_blocks.upper_bound(m_currentBlock->address());
            m_end = m_blocks.end();
        }
        assert(m_location < m_currentBlock.get()->address() + m_table->getTableAllocationSize());
        assert(m_location < m_currentBlock.get()->address() + (m_table->getTupleLength() * m_table->getTuplesPerBlock()));
        assert (out.sizeInValues() == m_table->columnCount());
        m_blockOffset++;
        out.move(m_location);
        const bool active = out.isActive();
        const bool dirty = out.isDirty();

        if (dirty) m_skippedDirtyRows++;
        if (!active) m_skippedInactiveRows++;

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
    if (m_currentBlock == NULL) {
        return 0;
    }
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
