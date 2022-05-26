/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#ifndef COPYONWRITEITERATOR_H_
#define COPYONWRITEITERATOR_H_
#include <vector>
#include "common/tabletuple.h"
#include "storage/TupleIterator.h"
#include "stx/btree_map.h"
#include "storage/TupleBlock.h"

namespace voltdb {
class PersistentTable;
class PersistentTableSurgeon;

class CopyOnWriteIterator : public TupleIterator {
    friend class CopyOnWriteContext;

public:

    CopyOnWriteIterator(
        PersistentTable *table,
        PersistentTableSurgeon *surgeon);

    bool needToDirtyTuple(char *tupleAddress);

    bool next(TableTuple &out);

    void notifyBlockWasCompactedAway(TBPtr block) {
        if (m_blockIterator != m_end) {
            TBPtr nextBlock = m_blockIterator.data();
            //The next block is the one that was compacted away
            //Need to move the iterator forward to skip it
            if (nextBlock == block) {
                m_blockIterator++;

                //There is another block after the one that was compacted away
                if (m_blockIterator != m_end) {
                    TBPtr newNextBlock = m_blockIterator.data();
                    m_blocks.erase(block->address());
                    m_blockIterator = m_blocks.find(newNextBlock->address());
                    m_end = m_blocks.end();
                    vassert(m_blockIterator != m_end);
                } else {
                    //No block after the one compacted away
                    //set everything to end
                    m_blocks.erase(block->address());
                    m_blockIterator = m_blocks.end();
                    m_end = m_blocks.end();
                }
            } else {
                //Some random block was compacted away. Remove it and regenerate the iterator
                m_blocks.erase(block->address());
                m_blockIterator = m_blocks.find(nextBlock->address());
                m_end = m_blocks.end();
                vassert(m_blockIterator != m_end);
            }
        }
    }

    virtual ~CopyOnWriteIterator() {}

    int64_t countRemaining() const;

private:
    /**
     * Table being iterated over
     */
    PersistentTable *m_table;

    /**
     * "Surgeon" that can perform deep changes to table.
     */
    PersistentTableSurgeon *m_surgeon;

    /**
     * Copied and sorted tuple blocks that can be binary searched in order to find out. The pair
     * contains the block address as well as the original index of the block.
     */
    TBMap m_blocks;
    TBMapI m_blockIterator;
    TBMapI m_end;

    /**
     * Length of a tuple
     */
    const int m_tupleLength;

    /**
     * Address of the last tuple returned
     */
    char *m_location;

    bool m_didFirstIteration;

    uint32_t m_blockOffset;
    TBPtr m_currentBlock;
    // flag to track if the snapshot was activated when the table was empty
    bool m_tableEmpty;
public:
    int32_t m_skippedDirtyRows;
    int32_t m_skippedInactiveRows;
};
}

#endif /* COPYONWRITEITERATOR_H_ */
