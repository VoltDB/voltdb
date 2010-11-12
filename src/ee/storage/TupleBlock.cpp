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
#include "storage/TupleBlock.h"
#include "storage/table.h"

namespace voltdb {
TupleBlock::TupleBlock(Table *table, TBBucketPtr bucket) :
        m_references(0),
        m_storage(new char[table->m_tableAllocationSize]),
        m_tupleLength(table->m_tupleLength),
        m_tuplesPerBlock(table->m_tuplesPerBlock),
        m_activeTuples(0),
        m_nextFreeTuple(0),
        m_tuplesPerBlockDivNumBuckets(m_tuplesPerBlock / static_cast<double>(TUPLE_BLOCK_NUM_BUCKETS)),
        m_bucketIndex(0),
        m_bucket(bucket) {
}

std::pair<int, int> TupleBlock::merge(Table *table, TBPtr source) {
    uint32_t m_nextTupleInSourceOffset = 0;
    int sourceTuplesPendingDeleteOnUndoRelease = 0;
    while (hasFreeTuples() && !source->isEmpty()) {
        TableTuple sourceTuple(table->schema());
        TableTuple destinationTuple(table->schema());

        //Iterate further into the block looking for active tuples
        //Stop when running into the unused tuple boundry
        while (m_nextTupleInSourceOffset < source->unusedTupleBoundry()) {
            sourceTuple.move(&source->address()[m_tupleLength * m_nextTupleInSourceOffset]);
            m_nextTupleInSourceOffset++;
            if (sourceTuple.isActive()) {
                break;
            }
        }

        if (sourceTuple.address() == NULL) {
           //The block isn't empty, but there are no more active tuples.
           //Some of the tuples that make it register as not empty must have been
           //pending delete and those aren't mergable
            assert(sourceTuplesPendingDeleteOnUndoRelease);
            break;
        }

        //Can't move a tuple with a pending undo action, it would invalidate the pointer
        //Keep a count so that the block can be notified of the number
        //of tuples pending delete on undo release when calculating the correct bucket
        //index. If all the active tuples are pending delete on undo release the block
        //is effectively empty and shouldn't be considered for merge ops.
        //It will be completely discarded once the undo log releases the block.
        if (sourceTuple.isPendingDeleteOnUndoRelease()) {
            sourceTuplesPendingDeleteOnUndoRelease++;
            continue;
        }

        destinationTuple.move(nextFreeTuple().first);
        table->swapTuples( sourceTuple, destinationTuple);
        source->freeTuple(sourceTuple.address());
    }

    int newBucketIndex = calculateBucketIndex();
    if (newBucketIndex != m_bucketIndex) {
        m_bucketIndex = newBucketIndex;
        //std::cout << "Merged " << static_cast<void*> (this) << "(" << m_activeTuples << ") with " << static_cast<void*>(source.get()) << "(" << source->m_activeTuples << ")" << std::endl;
        return std::pair<int, int>(newBucketIndex, source->calculateBucketIndex(sourceTuplesPendingDeleteOnUndoRelease));
    } else {
        //std::cout << "Merged " << static_cast<void*> (this) << "(" << m_activeTuples << ") with " << static_cast<void*>(source.get()) << "(" << source->m_activeTuples << ")" << std::endl;
        return std::pair<int, int>( -1, source->calculateBucketIndex(sourceTuplesPendingDeleteOnUndoRelease));
    }
}
}

