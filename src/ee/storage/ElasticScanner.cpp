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

#include "storage/persistenttable.h"
#include "storage/ElasticScanner.h"

namespace voltdb
{
namespace elastic
{

/**
 * Constructor.
 */
Scanner::Scanner(PersistentTable &table) :
    m_table(table),
    m_blockMap(table.m_data),
    m_tupleSize(table.getTupleLength()),
    m_blockIterator(m_blockMap.begin()),
    m_blockEnd(m_blockMap.end()),
    m_currentBlockPtr(NULL),
    m_tuplePtr(NULL),
    m_tupleIndex(0)
{
}

/**
 * Internal method that handles transitions between blocks and
 * returns true as long as tuples are available.
 */
bool Scanner::continueScan() {
    bool hasMore = true;
    // First block or end of block?
    if (m_currentBlockPtr == NULL || m_tupleIndex >= m_currentBlockPtr->unusedTupleBoundry()) {
        // No more blocks?
        hasMore = (m_blockIterator != m_blockEnd);
        // Shift to the next block?
        if (hasMore) {
            m_tuplePtr = m_blockIterator.key();
            m_currentBlockPtr = m_blockIterator.data();
            assert(m_currentBlockPtr->address() == m_tuplePtr);
            m_blockIterator.data() = TBPtr();
            m_tupleIndex = 0;
            m_blockIterator++;
        }
    }
    return hasMore;
}

/**
 * Get the next tuple or return false if none is available.
 */
bool Scanner::next(TableTuple &out)
{
    bool found = false;
    while (!found && continueScan()) {
        assert(m_currentBlockPtr != NULL);
        // Sanity checks.
        assert(m_tuplePtr < m_currentBlockPtr.get()->address() + m_table.getTableAllocationSize());
        assert(m_tuplePtr < m_currentBlockPtr.get()->address() + (m_tupleSize * m_table.getTuplesPerBlock()));
        assert (out.sizeInValues() == m_table.columnCount());
        // Grab the tuple pointer.
        out.move(m_tuplePtr);
        // Shift to the next tuple in block.
        // continueScan() will check if it's the last one in the block.
        m_tupleIndex++;
        m_tuplePtr += m_tupleSize;
        // The next active/non-dirty tuple is return-worthy.
        found = out.isActive() && !out.isDirty();
    }
    return found;
}

/**
 * Block compaction hook.
 */
void Scanner::notifyBlockWasCompactedAway(TBPtr block) {
    if (m_blockIterator != m_blockEnd) {
        TBPtr nextBlock = m_blockIterator.data();
        if (nextBlock == block) {
            // The next block was compacted away.
            m_blockIterator++;
            if (m_blockIterator != m_blockEnd) {
                // There is a block to skip to.
                TBPtr newNextBlock = m_blockIterator.data();
                m_blockMap.erase(block->address());
                m_blockIterator = m_blockMap.find(newNextBlock->address());
                m_blockEnd = m_blockMap.end();
                assert(m_blockIterator != m_blockMap.end());
            }
            else {
                // There isn't a block to skip to, so we're done.
                m_blockMap.erase(block->address());
                m_blockIterator = m_blockMap.end();
                m_blockEnd = m_blockMap.end();
            }
        } else {
            // Some random block was compacted away.
            // Remove it and regenerate the iterator.
            m_blockMap.erase(block->address());
            m_blockIterator = m_blockMap.find(nextBlock->address());
            m_blockEnd = m_blockMap.end();
            assert(m_blockIterator != m_blockMap.end());
        }
    }
}

/**
 * Tuple insert hook.
 */
void Scanner::notifyTupleInsert(TableTuple &tuple) {
    // Nothing to do for insert. The caller will deal with it.
}

/**
 * Tuple update hook.
 */
void Scanner::notifyTupleUpdate(TableTuple &tuple) {
    // Nothing to do for update. The caller will deal with it.
}

} // namespace elastic
} // namespace voltdb