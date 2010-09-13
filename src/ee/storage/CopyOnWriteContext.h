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
#ifndef COPYONWRITECONTEXT_H_
#define COPYONWRITECONTEXT_H_

#include <vector>
#include <utility>
#include "common/TupleSerializer.h"
#include "storage/table.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "boost/scoped_ptr.hpp"

namespace voltdb {

#ifdef MEMCHECK
typedef struct {
    std::pair<char*, int> pair;
    int tupleLength;
} BlockPair;
#else
typedef std::pair<char*, int> BlockPair;
#endif
typedef std::vector<BlockPair> BlockPairVector;
typedef BlockPairVector::iterator BlockPairVectorI;

class TupleIterator;
class TempTable;
class ReferenceSerializeOut;

class CopyOnWriteContext {
public:
    /**
     * Construct a copy on write context for the specified table that will serialize tuples
     * using the provided serializer
     */
    CopyOnWriteContext(Table *m_table, TupleSerializer *m_serializer, int32_t partitionId);

    /**
     * Serialize tuples to the provided output until no more tuples can be serialized. Returns true
     * if there are more tuples to serialize and false otherwise.
     */
    bool serializeMore(ReferenceSerializeOutput *out);

    /**
     * Mark a tuple as dirty and make a copy if necessary. The new tuple param indicates
     * that this is a new tuple being introduced into the table (nextFreeTuple was called).
     * In that situation the tuple doesn't need to be copied, but and may need to be marked dirty
     * (if it will be scanned later by COWIterator), and it must be marked clean if it is not going to
     * be scanned by the COWIterator
     */
    void markTupleDirty(TableTuple tuple, bool newTuple);

    virtual ~CopyOnWriteContext();

private:
    /**
     * Table being copied
     */
    Table *m_table;

    /**
     * Temp table for copies of tuples that were dirtied.
     */
    boost::scoped_ptr<TempTable> m_backedUpTuples;

    /**
     * Serializer for tuples
     */
    TupleSerializer *m_serializer;

    /**
     * Memory pool for string allocations
     */
    Pool m_pool;

    /**
     * Copied and sorted tuple blocks that can be binary searched in order to find out. The pair
     * contains the block address as well as the original index of the block.
     */
    BlockPairVector m_blocks;

    /**
     * Iterator over the table via a CopyOnWriteIterator or an iterator over
     *  temp table used to stored backed up tuples
     */
    boost::scoped_ptr<TupleIterator> m_iterator;

    /**
     * Maximum serialized length of a tuple
     */
    const int m_maxTupleLength;

    TableTuple m_tuple;

    bool m_finishedTableScan;

    const int32_t m_partitionId;

    int32_t m_tuplesSerialized;
};

}

#endif /* COPYONWRITECONTEXT_H_ */
