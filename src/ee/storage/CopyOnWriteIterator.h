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
#ifndef COPYONWRITEITERATOR_H_
#define COPYONWRITEITERATOR_H_
#include <vector>
#include "common/tabletuple.h"
#include "storage/TupleIterator.h"

namespace voltdb {
class Table;

class CopyOnWriteIterator : public TupleIterator {
    friend class CopyOnWriteContext;
public:
    CopyOnWriteIterator(Table *table);

    /**
     * When a tuple is "dirty" it is still active, but will never be a "found" tuple
     * since it is skipped. The tuple may be dirty because it was deleted (this is why it is always skipped). In that
     * case the CopyOnWriteContext calls this to ensure that the iteration finds the correct number of tuples
     * in the used portion of the table blocks and doesn't overrun to the uninitialized block memory because
     * it skiped a dirty tuple and didn't end up with the right found tuple count upon reaching the end.
     */
    bool needToDirtyTuple(int blockIndex, const char *address, const bool newTuple) {
        if (blockIndex < m_blockIndex) {
            return false;
        }

        if (blockIndex > m_blockIndex) {
            if (!newTuple) {
                m_foundTuples++;
            }
            return true;
        }

        if (address > m_location) {
            if (!newTuple) {
                m_foundTuples++;
            }
            return true;
        } else {
            return false;
        }
    }

    bool next(TableTuple &out);

    virtual ~CopyOnWriteIterator() {}
private:
    /**
     * Table being iterated over
     */
    Table *m_table;

    /**
     * List of blocks being iterated over
     */
    std::vector<char*> m_blocks;

    /**
     * Index of the current block being iterated over
     */
    uint32_t m_blockIndex;

    /**
     * Length of a tuple
     */
    const int m_tupleLength;

    /**
     * Address of the last tuple returned
     */
    char *   m_location;

    /**
     * Total number of tuples that are expected to be found
     */
    const uint32_t m_activeTupleCount;

    /**
     * Total number of tuples that have been found so far
     */
    uint32_t m_foundTuples;

    /**
     * Length of a block
     */
    const size_t m_blockLength;

    bool m_didFirstIteration;

    /**
     * Need to mark all tuples allocated in the last block as found
     * because they were inserted as dirty.
     */
    void cleanBlocksAfterLastFound();
};
}

#endif /* COPYONWRITEITERATOR_H_ */
