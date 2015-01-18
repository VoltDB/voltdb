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
        PersistentTableSurgeon *surgeon,
        TBMap blocks);

    /**
     * When a tuple is "dirty" it is still active, but will never be a "found" tuple
     * since it is skipped. The tuple may be dirty because it was deleted (this is why it is always skipped). In that
     * case the CopyOnWriteContext calls this to ensure that the iteration finds the correct number of tuples
     * in the used portion of the table blocks and doesn't overrun to the uninitialized block memory because
     * it skiped a dirty tuple and didn't end up with the right found tuple count upon reaching the end.
     */
    bool needToDirtyTuple(const char *blockAddress, const char *tupleAddress) {
        if (blockAddress < m_currentBlock->address()) {
            return false;
        }

        if (blockAddress > m_currentBlock->address()) {
            return true;
        }

        if (tupleAddress >= m_location) {
            return true;
        } else {
            return false;
        }
    }

    bool next(TableTuple &out);

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
     * Index of the current block being iterated over
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
public:
    int32_t m_skippedDirtyRows;
    int32_t m_skippedInactiveRows;
};
}

#endif /* COPYONWRITEITERATOR_H_ */
