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
