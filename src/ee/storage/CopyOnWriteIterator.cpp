/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
        PersistentTableSurgeon *surgeon) :
        m_table(table), m_surgeon(surgeon), m_blocks(m_surgeon->getData()),
        m_blockIterator(m_blocks.begin()), m_end(m_blocks.end()),
        m_tupleLength(table->getTupleLength()),
        m_location(NULL),
        m_blockOffset(0),
        m_currentBlock(NULL),
        m_tableEmpty(false),
        m_skippedDirtyRows(0),
        m_skippedInactiveRows(0) {

//    if ((m_blocks.size() == 1) && m_blockIterator.data()->isEmpty()) {
//        // Empty persistent table - no tuples in table and table only
//        // has empty tuple storage block associated with it. So no need
//        // to set it up for snapshot
//        m_blockIterator = m_end;
//        m_tableEmpty = true;
//        return;
//    }
//
//    //Prime the pump
//    if (m_blockIterator != m_end) {
//        m_surgeon->snapshotFinishedScanningBlock(m_currentBlock, m_blockIterator.data());
//        m_location = m_blockIterator.key();
//        m_currentBlock = m_blockIterator.data();
//        m_blockIterator++;
//    }
    m_blockOffset = 0;
}

/**
 * When a tuple is "dirty" it is still active, but will never be a "found" tuple
 * since it is skipped. The tuple may be dirty because it was deleted (this is why it is always skipped). In that
 * case the CopyOnWriteContext calls this to ensure that the iteration finds the correct number of tuples
 * in the used portion of the table blocks and doesn't overrun to the uninitialized block memory because
 * it skiped a dirty tuple and didn't end up with the right found tuple count upon reaching the end.
 */
bool CopyOnWriteIterator::needToDirtyTuple(char *tupleAddress) {
   return false;
}

/**
 * Iterate through the table blocks until all the active tuples have been found. Skip dirty tuples
 * and mark them as clean so that they can be copied during the next snapshot.
 */
bool CopyOnWriteIterator::next(TableTuple &out) {
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
        if (blockOffset >= currentBlock->unusedTupleBoundary()) {
            if (blockIterator == m_end) {
                break;
            }
            location = blockIterator.key();
            currentBlock = blockIterator.data();
            vassert(currentBlock->address() == location);
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
